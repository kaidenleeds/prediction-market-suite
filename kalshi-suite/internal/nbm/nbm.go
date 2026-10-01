// Package nbm reads NOAA's operational probabilistic National Blend of Models station bulletin.
// It streams the large all-station file and retains only explicitly requested settlement stations.
package nbm

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultBaseURL = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/blend/prod"
	maxBulletin    = 48 << 20
)

var headerRE = regexp.MustCompile(`^\s*([A-Z0-9]{3,8})\s+NBM\s+V[^ ]+\s+NBP\s+GUIDANCE\s+(\d{1,2}/\d{1,2}/\d{4})\s+(\d{4})\s+UTC\s*$`)

var retainedRows = map[string]bool{
	"UTC": true, "FHR": true, "TXNMN": true, "TXNSD": true,
	"TXNP1": true, "TXNP2": true, "TXNP5": true, "TXNP7": true, "TXNP9": true,
}

type StationForecast struct {
	Station     string            `json:"station"`
	Run         time.Time         `json:"run"`
	ArtifactURL string            `json:"artifact_url"`
	Values      map[string][]int  `json:"values"`
	RawRows     map[string]string `json:"raw_rows"`
}

type Receipt struct {
	ArtifactURL string            `json:"artifact_url"`
	Run         time.Time         `json:"run"`
	BytesRead   int64             `json:"bytes_read"`
	Attempts    int               `json:"attempts"`
	Requested   int               `json:"requested"`
	Forecasts   []StationForecast `json:"forecasts"`
	Missing     []string          `json:"missing"`
}

// FetchError keeps the bounded attempt count and last artifact visible to collector receipts.
// Unwrap preserves context deadline/transport classification without relabeling an outage.
type FetchError struct {
	Attempts     int
	LastArtifact string
	Err          error
}

func (e *FetchError) Error() string {
	if e == nil {
		return "NBM fetch failed"
	}
	return fmt.Sprintf("NBM fetch failed after %d attempt(s), last=%s: %v", e.Attempts, e.LastArtifact, e.Err)
}

func (e *FetchError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

type Client struct {
	BaseURL string
	HTTP    *http.Client
}

func NewClient(timeout time.Duration) *Client {
	return &Client{BaseURL: DefaultBaseURL, HTTP: &http.Client{Timeout: timeout}}
}

func candidateRuns(now time.Time) []time.Time {
	now = now.UTC().Add(-2 * time.Hour)
	cycles := []int{19, 13, 12, 7, 1, 0}
	var out []time.Time
	for day := 0; day < 2; day++ {
		d := now.AddDate(0, 0, -day)
		for _, hour := range cycles {
			run := time.Date(d.Year(), d.Month(), d.Day(), hour, 0, 0, 0, time.UTC)
			if !run.After(now) {
				out = append(out, run)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].After(out[j]) })
	return out
}

func bulletinURL(base string, run time.Time) string {
	return fmt.Sprintf("%s/blend.%s/%02d/text/blend_nbptx.t%02dz",
		strings.TrimRight(base, "/"), run.Format("20060102"), run.Hour(), run.Hour())
}

type countingReader struct {
	r io.Reader
	n int64
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	r.n += int64(n)
	return n, err
}

func parseValues(line string) (string, []int, bool) {
	fields := strings.Fields(strings.ReplaceAll(line, "|", " "))
	if len(fields) < 2 || !retainedRows[fields[0]] {
		return "", nil, false
	}
	values := make([]int, 0, len(fields)-1)
	for _, raw := range fields[1:] {
		v, err := strconv.Atoi(raw)
		if err != nil {
			return "", nil, false
		}
		values = append(values, v)
	}
	return fields[0], values, len(values) > 0
}

func parseRun(date, hhmm string) (time.Time, error) {
	return time.ParseInLocation("1/2/2006 1504", date+" "+hhmm, time.UTC)
}

func parseBulletin(r io.Reader, artifact string, requested map[string]bool) ([]StationForecast, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64<<10), 1<<20)
	byStation := map[string]StationForecast{}
	current := ""
	for scanner.Scan() {
		line := scanner.Text()
		if match := headerRE.FindStringSubmatch(line); match != nil {
			station := strings.ToUpper(match[1])
			current = ""
			if requested[station] {
				run, err := parseRun(match[2], match[3])
				if err != nil {
					continue
				}
				current = station
				byStation[station] = StationForecast{Station: station, Run: run, ArtifactURL: artifact,
					Values: map[string][]int{}, RawRows: map[string]string{}}
			}
			continue
		}
		if current == "" {
			continue
		}
		name, values, ok := parseValues(line)
		if !ok {
			continue
		}
		row := byStation[current]
		row.Values[name] = values
		row.RawRows[name] = strings.TrimSpace(line)
		byStation[current] = row
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	out := make([]StationForecast, 0, len(byStation))
	for _, row := range byStation {
		if len(row.Values["TXNP5"]) > 0 && len(row.Values["FHR"]) == len(row.Values["TXNP5"]) {
			out = append(out, row)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Station < out[j].Station })
	return out, nil
}

// FetchLatest tries the most recent operational cycles after NOAA's normal two-hour publication
// lag. Each attempt is bounded, and the response is streamed instead of retained wholesale.
func (c *Client) FetchLatest(ctx context.Context, stations []string, now time.Time) (Receipt, error) {
	if c == nil || c.HTTP == nil || strings.TrimSpace(c.BaseURL) == "" {
		return Receipt{}, errors.New("invalid NBM client")
	}
	want := map[string]bool{}
	for _, station := range stations {
		if station = strings.ToUpper(strings.TrimSpace(station)); station != "" {
			want[station] = true
		}
	}
	if len(want) == 0 || len(want) > 100 {
		return Receipt{}, fmt.Errorf("NBM station request count=%d outside 1..100", len(want))
	}
	var lastErr error
	attempts := 0
	lastArtifact := ""
	for _, run := range candidateRuns(now) {
		if err := ctx.Err(); err != nil {
			return Receipt{}, &FetchError{Attempts: attempts, LastArtifact: lastArtifact, Err: err}
		}
		artifact := bulletinURL(c.BaseURL, run)
		lastArtifact = artifact
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, artifact, nil)
		if err != nil {
			return Receipt{}, err
		}
		attempts++
		// Leave Accept-Encoding to net/http. When Transport adds gzip it also transparently
		// decompresses; setting the header manually disables that guarantee and could feed compressed
		// bytes to the line parser on a server that honors gzip.
		resp, err := c.HTTP.Do(req)
		if err != nil {
			lastErr = err
			if ctx.Err() != nil {
				return Receipt{}, &FetchError{Attempts: attempts, LastArtifact: lastArtifact, Err: ctx.Err()}
			}
			continue
		}
		if resp.StatusCode != http.StatusOK || resp.ContentLength > maxBulletin {
			lastErr = fmt.Errorf("NBM %s status=%d length=%d", run.Format(time.RFC3339), resp.StatusCode, resp.ContentLength)
			_ = resp.Body.Close()
			continue
		}
		counter := &countingReader{r: io.LimitReader(resp.Body, maxBulletin+1)}
		forecasts, parseErr := parseBulletin(counter, artifact, want)
		_ = resp.Body.Close()
		if parseErr != nil || counter.n > maxBulletin {
			if parseErr != nil {
				lastErr = parseErr
			} else {
				lastErr = fmt.Errorf("NBM bulletin exceeded %d bytes", maxBulletin)
			}
			continue
		}
		found := map[string]bool{}
		actualRun := run
		for _, row := range forecasts {
			found[row.Station] = true
			actualRun = row.Run
		}
		missing := make([]string, 0, len(want)-len(found))
		for station := range want {
			if !found[station] {
				missing = append(missing, station)
			}
		}
		sort.Strings(missing)
		return Receipt{ArtifactURL: artifact, Run: actualRun, BytesRead: counter.n, Attempts: attempts,
			Requested: len(want), Forecasts: forecasts, Missing: missing}, nil
	}
	if lastErr == nil {
		lastErr = errors.New("no NBM cycle candidates")
	}
	return Receipt{}, &FetchError{Attempts: attempts, LastArtifact: lastArtifact, Err: lastErr}
}
