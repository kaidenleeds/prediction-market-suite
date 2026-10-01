package server

import (
	"context"
	"errors"
	"strings"
	"time"
)

const (
	r164KalshiSemanticWarmCapacity = 128
	r164KalshiSemanticWarmWorkers  = 2
	r164KalshiSemanticWarmTimeout  = 4 * time.Second
)

type kalshiSemanticWarmJob struct {
	ticker string
	done   chan struct{}
	err    error
}

// scheduleKalshiSemanticWarm is memory-only and nonblocking. Public Event/Milestone reads run on
// the ordinary background tier; the urgent AUTO guard remains resident-only and fail-closed.
func (s *Server) scheduleKalshiSemanticWarm(ticker string) bool {
	if s == nil || s.kal == nil {
		return false
	}
	ticker = strings.ToUpper(strings.TrimSpace(ticker))
	if ticker == "" {
		return false
	}
	s.kalSemWarmOnce.Do(func() {
		s.kalSemWarmQueue = make(chan *kalshiSemanticWarmJob,
			r164KalshiSemanticWarmCapacity)
		for range r164KalshiSemanticWarmWorkers {
			go s.runKalshiSemanticWarmWorker()
		}
	})
	if _, loaded := s.kalSemWarmPending.LoadOrStore(ticker, struct{}{}); loaded {
		return true
	}
	job := &kalshiSemanticWarmJob{ticker: ticker, done: make(chan struct{})}
	select {
	case s.kalSemWarmQueue <- job:
		return true
	default:
		s.kalSemWarmPending.Delete(ticker)
		job.err = errors.New("Kalshi semantic warm queue full")
		close(job.done)
		return false
	}
}

func (s *Server) runKalshiSemanticWarmWorker() {
	for job := range s.kalSemWarmQueue {
		ctx, cancel := context.WithTimeout(context.Background(),
			r164KalshiSemanticWarmTimeout)
		job.err = s.warmKalshiSemanticTicker(ctx, job.ticker)
		cancel()
		s.kalSemWarmPending.Delete(job.ticker)
		close(job.done)
	}
}

func (s *Server) warmKalshiSemanticTicker(ctx context.Context, ticker string) error {
	market, ok := s.r159KalshiResidentMarket(ticker)
	if !ok {
		return errors.New("resident Kalshi market unavailable or stale")
	}
	eventTicker := strings.ToUpper(strings.TrimSpace(market.EventTicker))
	event, err := s.kalshiSemanticEventMode(ctx, eventTicker, false)
	if err != nil {
		return err
	}
	if !strings.EqualFold(strings.TrimSpace(event.Category), "sports") {
		return nil
	}
	_, err = s.kalshiSemanticMilestoneMode(ctx, eventTicker, false)
	return err
}

func (s *Server) scheduleKalshiSemanticSnapshotWarm(
	snapshot r154KalshiAdmissionSnapshot) {
	for _, position := range snapshot.Positions() {
		if position.PositionQty() != 0 {
			s.scheduleKalshiSemanticWarm(position.Ticker)
		}
	}
	for _, order := range snapshot.Orders() {
		s.scheduleKalshiSemanticWarm(order.Ticker)
	}
}
