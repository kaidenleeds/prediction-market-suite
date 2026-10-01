package server

import "testing"

func TestR159KalshiIOCZeroFillIsTerminalUnfilled(t *testing.T) {
	state, authoritative := liveKalshiCreateReceiptState(
		liveProspectiveIOC, 4, 0, 4, true)
	if state != "unfilled" || !authoritative {
		t.Fatalf("IOC zero-fill = %q/%v, want authoritative unfilled", state, authoritative)
	}
	if !liveKalshiTerminalZeroFill(state, authoritative, 0, 4) {
		t.Fatal("IOC zero-fill with remaining=requested did not release risk and claim")
	}
	if liveKalshiReceiptNeedsCancelTrackingFor(liveProspectiveIOC, state, 4) {
		t.Fatal("terminal IOC zero-fill entered resting-order tracking")
	}
}

func TestR159KalshiFOKZeroFillIsTerminalUnfilled(t *testing.T) {
	state, authoritative := liveKalshiCreateReceiptState(
		liveProspectiveFOK, 4, 0, 4, true)
	if state != "unfilled" || !authoritative {
		t.Fatalf("FOK zero-fill = %q/%v, want authoritative unfilled", state, authoritative)
	}
	if !liveKalshiTerminalZeroFill(state, authoritative, 0, 4) {
		t.Fatal("FOK zero-fill with remaining=requested did not release risk and claim")
	}
	if why := liveProspectiveFOKPartialReason(liveProspectiveFOK, state, 0, 4); why != "" {
		t.Fatalf("valid FOK zero-fill triggered the partial-fill alarm: %q", why)
	}
}

func TestR159KalshiGTCZeroFillRemainsPending(t *testing.T) {
	state, authoritative := liveKalshiCreateReceiptState(
		"good_till_canceled", 4, 0, 4, true)
	if state != "pending" || !authoritative {
		t.Fatalf("GTC zero-fill = %q/%v, want authoritative pending", state, authoritative)
	}
	if liveKalshiTerminalZeroFill(state, authoritative, 0, 4) {
		t.Fatal("resting GTC order was treated as a terminal zero-fill")
	}
	if !liveKalshiReceiptNeedsCancelTrackingFor("good_till_canceled", state, 4) {
		t.Fatal("resting GTC order did not enter cancel tracking")
	}
}

func TestR159KalshiIOCPartialIsTerminalAndFOKPartialStillAlarms(t *testing.T) {
	state, authoritative := liveKalshiCreateReceiptState(
		liveProspectiveIOC, 4, 1, 3, true)
	if state != "partial" || !authoritative {
		t.Fatalf("IOC partial = %q/%v, want authoritative partial before fee reconciliation",
			state, authoritative)
	}
	if liveKalshiReceiptNeedsCancelTrackingFor(liveProspectiveIOC, state, 3) {
		t.Fatal("terminal IOC partial entered resting-order tracking")
	}

	state, authoritative = liveKalshiCreateReceiptState(
		liveProspectiveFOK, 4, 1, 3, true)
	if state != "partial" || !authoritative {
		t.Fatalf("FOK partial = %q/%v, want parsed partial before safety alarm",
			state, authoritative)
	}
	if why := liveProspectiveFOKPartialReason(liveProspectiveFOK, state, 1, 4); why == "" {
		t.Fatal("FOK partial did not trigger the atomicity safety alarm")
	}
}
