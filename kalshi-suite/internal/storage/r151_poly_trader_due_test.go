package storage

import (
	"context"
	"testing"
	"time"
)

func TestR151OpenPolyTraderConditionsDueForcedIndexQueryIsValid(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	rows, err := st.OpenPolyTraderConditionsDue(context.Background(), 10, time.Now().Unix())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("empty store returned %d due conditions", len(rows))
	}
}
