package polymarket

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func r132ResetFeeScheduleCache() {
	feeScheduleMu.Lock()
	feeScheduleCache = map[string]feeScheduleEntry{}
	feeScheduleMu.Unlock()
}

func TestR132GammaFeeSchemaSeedsCompleteCurve(t *testing.T) {
	r132ResetFeeScheduleCache()
	t.Cleanup(r132ResetFeeScheduleCache)
	const condition = "0x1111111111111111111111111111111111111111111111111111111111111111"
	var m Market
	if err := json.Unmarshal([]byte(`{
		"conditionId":"`+condition+`",
		"feesEnabled":true,
		"feeSchedule":{"exponent":2,"rate":0.02,"takerOnly":true,"rebateRate":0.25},
		"orderPriceMinTickSize":0.001
	}`), &m); err != nil {
		t.Fatal(err)
	}
	if m.FeesEnabled == nil || !*m.FeesEnabled || m.FeeSchedule == nil || m.FeeSchedule.Rate != 0.02 ||
		m.FeeSchedule.Exponent != 2 || !m.FeeSchedule.TakerOnly || m.FeeSchedule.RebateRate != 0.25 || m.OrderPriceMinTickSize != 0.001 {
		t.Fatalf("Gamma fee/tick schema decode = %+v", m)
	}

	c := NewClient(time.Second)
	c.http.Transport = r132RoundTripFunc(func(*http.Request) (*http.Response, error) {
		t.Fatal("seeded Gamma schedule should not require a CLOB round trip")
		return nil, nil
	})
	c.registerMarketTokens(m) // fee seeding does not require clobTokenIds
	schedule, ok := c.ClobFeeSchedule(condition)
	if !ok || schedule.Rate != 0.02 || schedule.Exponent != 2 || !schedule.TakerOnly || schedule.RebateRate != 0.25 {
		t.Fatalf("seeded fee schedule = %+v ok=%v", schedule, ok)
	}
	if fee, ok := c.ClobFeeUSD(condition, 100, 0.5, true); !ok || fee != 0.125 {
		t.Fatalf("seeded exponent-2 fee = %.5f ok=%v, want .125 true", fee, ok)
	}
	if _, ok := c.ClobFeeRate(condition); ok {
		t.Fatal("legacy coefficient-only API must fail closed for exponent-2 schedules")
	}
}

func TestR132FeeFreeMustBeExplicitAndContradictionsStayUnknown(t *testing.T) {
	falseValue := false
	trueValue := true
	cases := []struct {
		name string
		m    Market
		ok   bool
	}{
		{name: "explicit fee-free", m: Market{FeesEnabled: &falseValue}, ok: true},
		{name: "absent enable flag", m: Market{FeeSchedule: func() *FeeSchedule { s := newFeeSchedule(0.05, 1, true); return &s }()}, ok: false},
		{name: "enabled but absent schedule", m: Market{FeesEnabled: &trueValue}, ok: false},
		{name: "contradictory disabled positive schedule", m: Market{FeesEnabled: &falseValue, FeeSchedule: func() *FeeSchedule { s := newFeeSchedule(0.05, 1, true); return &s }()}, ok: false},
		{name: "missing child fields", m: Market{FeesEnabled: &trueValue, FeeSchedule: &FeeSchedule{Exponent: 1, Rate: 0.05}}, ok: false},
		{name: "exponent two supported", m: Market{FeesEnabled: &trueValue, FeeSchedule: func() *FeeSchedule { s := newFeeSchedule(0.02, 2, true); return &s }()}, ok: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			schedule, ok := gammaFeeSchedule(tc.m)
			if ok != tc.ok {
				t.Fatalf("gammaFeeSchedule = %+v,%v; want ok=%v", schedule, ok, tc.ok)
			}
			if tc.name == "explicit fee-free" {
				if fee, exact := schedule.FeeUSD(100, 0.5, true); !exact || fee != 0 {
					t.Fatalf("explicit fee-free curve charged %.5f exact=%v", fee, exact)
				}
			}
		})
	}
}

func TestR132FeeCurveTakerMakerAndRebateSemantics(t *testing.T) {
	e1 := newFeeSchedule(0.05, 1, true)
	e1.RebateRate = 0.25
	if fee, ok := e1.FeeUSD(100, 0.5, true); !ok || fee != 1.25 {
		t.Fatalf("e=1 taker fee = %.5f ok=%v, want 1.25", fee, ok)
	}
	if fee, ok := e1.FeeUSD(100, 0.5, false); !ok || fee != 0 {
		t.Fatalf("taker-only maker fee = %.5f ok=%v, want zero (rebate is not instant)", fee, ok)
	}
	e2 := newFeeSchedule(0.02, 2, true)
	if fee, ok := e2.FeeUSD(100, 0.5, true); !ok || fee != 0.125 {
		t.Fatalf("e=2 taker fee = %.5f ok=%v, want .125", fee, ok)
	}
	makerCharged := newFeeSchedule(0.05, 1, false)
	if fee, ok := makerCharged.FeeUSD(100, 0.5, false); !ok || fee != 0 {
		t.Fatalf("legacy non-taker-only field reintroduced a V2 maker fee = %.5f ok=%v", fee, ok)
	}
}

func TestR132ClobFeeRateNeverTreatsTBFAsPlatformRate(t *testing.T) {
	tests := []struct {
		name string
		body string
		rate float64
		exp  float64
		ok   bool
	}{
		{name: "tbf only is unknown", body: `{"tbf":1000,"mbf":1000}`, ok: false},
		{name: "fd curve wins over tbf", body: `{"tbf":1000,"fd":{"r":0.02,"e":2,"to":true}}`, rate: 0.02, exp: 2, ok: true},
		{name: "explicit zero fd is known free", body: `{"tbf":1000,"fd":{"r":0,"e":1,"to":true}}`, rate: 0, exp: 1, ok: true},
		{name: "missing fields are unknown", body: `{}`, ok: false},
		{name: "partial fd is unknown", body: `{"fd":{"r":0.05}}`, ok: false},
		{name: "negative fd is unknown", body: `{"fd":{"r":-0.01,"e":1,"to":true}}`, ok: false},
	}
	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r132ResetFeeScheduleCache()
			t.Cleanup(r132ResetFeeScheduleCache)
			c := NewClient(time.Second)
			c.http.Transport = r132RoundTripFunc(func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     make(http.Header),
					Body:       io.NopCloser(strings.NewReader(tc.body)),
					Request:    req,
				}, nil
			})
			condition := "0x" + strings.Repeat(string(rune('a'+i)), 64)
			schedule, ok := c.ClobFeeSchedule(condition)
			if ok != tc.ok || (ok && (schedule.Rate != tc.rate || schedule.Exponent != tc.exp)) {
				t.Fatalf("ClobFeeSchedule = %+v,%v; want rate %.4f exp %.1f ok=%v", schedule, ok, tc.rate, tc.exp, tc.ok)
			}
		})
	}
}
