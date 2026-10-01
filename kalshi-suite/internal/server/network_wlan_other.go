//go:build !windows

package server

import (
	"context"
	"errors"
)

func defaultWLANCommand(context.Context) ([]byte, error) {
	return nil, errors.New("Windows WLAN telemetry unavailable")
}
