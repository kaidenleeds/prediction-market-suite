package server

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

// auxiliaryPaperResetReceipt is the history-preserving reset result for Paper state that does not
// live in paper_fills, paper_parlays, or the ML JSON book. It is intentionally one call from the
// root Reset-P&L transaction so future omissions are visible in a single receipt.
type auxiliaryPaperResetReceipt struct {
	EpochAt            time.Time
	LivePolicyMirror   storage.LivePolicyMirrorResetReceipt
	StagedBundles      storage.StagedPaperBundleResetReceipt
	XvComboArchived    int
	CanceledMakerPosts int
	CanceledByBook     map[string]int
}

// resetAuxiliaryPaperPortfolios advances the isolated mirror and staged-package epochs, archives
// current XvCombo opens into its settleable history lane, then removes every pre-reset money-backed
// maker post from the in-memory fill engine. Immutable evidence remains in place.
func (s *Server) resetAuxiliaryPaperPortfolios(ctx context.Context, epoch time.Time) (auxiliaryPaperResetReceipt, error) {
	out := auxiliaryPaperResetReceipt{EpochAt: epoch.UTC(), CanceledByBook: map[string]int{}}
	if s == nil || s.store == nil {
		return out, errors.New("auxiliary Paper reset requires storage")
	}
	if epoch.IsZero() {
		epoch = time.Now().UTC()
		out.EpochAt = epoch
	}

	mirror, mirrorErr := s.store.ResetLivePolicyMirrorPortfolio(ctx, epoch)
	if mirrorErr == nil {
		out.LivePolicyMirror = mirror
		// Dedup and display state are current-epoch state, unlike the append-only evidence ledger.
		s.livePolicyMirrorMu.Lock()
		s.livePolicyMirrorSeen = make(map[string]time.Time)
		s.livePolicyMirrorMu.Unlock()
		s.livePolicyMirrorBrief.Store(nil)
	}

	staged, stagedErr := s.store.ResetStagedPaperBundlePortfolio(ctx, epoch)
	if stagedErr == nil {
		out.StagedBundles = staged
	}
	xvArchived, xvErr := s.resetXvComboPaperAt(epoch)
	if xvErr == nil {
		out.XvComboArchived = xvArchived
	}

	canceled, byBook, pendingErr := s.cancelPreResetPaperMakerPosts(ctx, epoch)
	out.CanceledMakerPosts, out.CanceledByBook = canceled, byBook

	if err := errors.Join(mirrorErr, stagedErr, xvErr, pendingErr); err != nil {
		return out, err
	}
	books := make([]string, 0, len(byBook))
	for book, n := range byBook {
		books = append(books, fmt.Sprintf("%s=%d", book, n))
	}
	sort.Strings(books)
	_ = s.store.Audit(ctx, "info", "paper", fmt.Sprintf(
		"auxiliary Paper reset: mirror excluded rows=%d/open=%d; staged excluded attempts=%d/open=%d; xvcombo archived open=%d; canceled maker posts=%d [%s]; history preserved",
		mirror.ExcludedRows, mirror.ExcludedOpen, staged.ExcludedAttempts, staged.ExcludedOpen,
		xvArchived, canceled, strings.Join(books, ", ")), "")
	return out, nil
}

func (s *Server) cancelPreResetPaperMakerPosts(ctx context.Context, epoch time.Time) (int, map[string]int, error) {
	byBook := map[string]int{}
	if s == nil || s.store == nil {
		return 0, byBook, errors.New("Paper maker reset requires storage")
	}
	epoch = epoch.UTC()
	s.pendMu.Lock()
	kept := make([]*pendingMaker, 0, len(s.pendingMakers))
	canceled := make([]*pendingMaker, 0, len(s.pendingMakers))
	for _, p := range s.pendingMakers {
		if p == nil {
			continue
		}
		// This lane is an explicitly unfunded counterfactual and never creates a Paper lot.
		if p.book == "subcent-golf" || (!p.postedAt.IsZero() && !p.postedAt.Before(epoch)) {
			kept = append(kept, p)
			continue
		}
		canceled = append(canceled, p)
	}
	s.pendingMakers = kept
	s.pendMu.Unlock()

	var errs []error
	for _, p := range canceled {
		book := strings.TrimSpace(p.book)
		if book == "" {
			book = "shared-paper"
		}
		byBook[book]++
		if err := s.store.MarkMakerExpiredRule(ctx, p.id, "paper-reset-epoch"); err != nil {
			errs = append(errs, fmt.Errorf("expire maker attempt %d: %w", p.id, err))
		}
		if p.relationReceiptID != "" {
			if _, err := s.store.RecordFundedRelationCancellation(context.WithoutCancel(ctx),
				p.relationReceiptID, "paper-reset-epoch"); err != nil {
				errs = append(errs, fmt.Errorf("cancel maker relation %s: %w", p.relationReceiptID, err))
			}
		}
	}
	return len(canceled), byBook, errors.Join(errs...)
}
