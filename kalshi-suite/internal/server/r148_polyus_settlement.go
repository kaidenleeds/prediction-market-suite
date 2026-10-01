package server

import (
	"strings"

	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

// polyUSFinalSettlementSource converts the strict public parser's authority into
// a versioned durable provenance. Callers invoke this only after SettledYes has
// accepted the terminal result; the storage boundary independently rejects any
// other PolyUS source label.
func polyUSFinalSettlementSource(book *polymarketus.BookData) string {
	if book != nil && strings.EqualFold(strings.TrimSpace(book.SettlementAuthority),
		polymarketus.FinalSettlementEndpointAuthority) {
		return storage.PolyUSFinalEndpointSettlementV2
	}
	return storage.PolyUSFinalBookSettlementV2
}
