package server

import (
	"context"
	"strings"

	"github.com/kalshi-suite/kalshi-suite/internal/polyid"
)

type conditionTokenResolver interface {
	ConditionByToken(context.Context, string) (string, bool, error)
}

type conditionTokenCache interface {
	CachedConditionByToken(string) (conditionID string, found, known bool)
}

// recoverPTTConditionID admits a venue condition id as-is only when it has canonical width.
// Invalid activity values get one bounded official token lookup per unique asset; failed or
// budget-exhausted values stay raw so storage can preserve them under resolved=-3.
func recoverPTTConditionID(
	ctx context.Context,
	resolver conditionTokenResolver,
	conditionID, token string,
	recoveryLeft *int,
	byToken map[string]string,
	tried map[string]bool,
) (string, bool) {
	conditionID = strings.TrimSpace(conditionID)
	if canonical, ok := polyid.NormalizeConditionID(conditionID); ok {
		return canonical, false
	}
	token = strings.TrimSpace(token)
	if prior := byToken[token]; prior != "" {
		return prior, false
	}
	if cache, ok := resolver.(conditionTokenCache); ok {
		if recovered, found, known := cache.CachedConditionByToken(token); known {
			if found && polyid.ValidConditionID(recovered) {
				byToken[token] = recovered
				return recovered, false
			}
			return conditionID, false
		}
	}
	if token == "" || tried[token] || *recoveryLeft <= 0 {
		return conditionID, false
	}
	tried[token] = true
	*recoveryLeft = *recoveryLeft - 1
	recovered, found, err := resolver.ConditionByToken(ctx, token)
	if err != nil || !found || !polyid.ValidConditionID(recovered) {
		return conditionID, false
	}
	byToken[token] = recovered
	return recovered, true
}
