package quota

import (
	"context"
	"errors"
	"sync"

	"github.com/Space-DF/telemetry-service/internal/client"
)

const (
	FeatureAutomationMaxCount = "automation.max_count"

	ScopeOrganization = "organization"
	ScopeUser         = "user"
	ScopeSpace        = "space"
)

type Client interface {
	ReserveQuota(ctx context.Context, organization string, feature string, amount int, scopeType, scopeID string) (bool, error)
	ReleaseQuota(ctx context.Context, organization string, feature string, amount int, scopeType, scopeID string)
}

type Rule struct {
	Features  []string
	ScopeType string
	Amount    int
}

type Guard struct {
	client Client
}

type Reservation struct {
	guard        *Guard
	organization string
	rule         Rule
	scopeID      string
	once         sync.Once
}

func NewGuard(client Client) *Guard {
	return &Guard{
		client: client,
	}
}

func (g *Guard) Reserve(ctx context.Context, organization string, rule Rule, scopeID string) (*Reservation, error) {
	if g == nil || g.client == nil || len(rule.Features) == 0 || organization == "" {
		return nil, nil
	}

	reservedFeatures := make([]string, 0, len(rule.Features))
	for _, feature := range rule.Features {
		reserved, err := g.client.ReserveQuota(
			ctx,
			organization,
			feature,
			rule.amount(),
			rule.ScopeType,
			scopeID,
		)
		if err != nil {
			g.releaseFeatures(ctx, organization, reservedFeatures, rule, scopeID)
			return nil, err
		}
		if !reserved {
			g.releaseFeatures(ctx, organization, reservedFeatures, rule, scopeID)
			return nil, nil
		}
		reservedFeatures = append(reservedFeatures, feature)
	}

	return &Reservation{
		guard:        g,
		organization: organization,
		rule:         rule,
		scopeID:      scopeID,
	}, nil
}

func (g *Guard) Release(ctx context.Context, organization string, rule Rule, scopeID string) {
	if g == nil || g.client == nil || len(rule.Features) == 0 || organization == "" {
		return
	}

	g.releaseFeatures(ctx, organization, rule.Features, rule, scopeID)
}

func (g *Guard) releaseFeatures(ctx context.Context, organization string, features []string, rule Rule, scopeID string) {
	for _, feature := range features {
		g.client.ReleaseQuota(
			ctx,
			organization,
			feature,
			rule.amount(),
			rule.ScopeType,
			scopeID,
		)
	}
}

func (r *Reservation) Release(ctx context.Context) {
	if r == nil || r.guard == nil {
		return
	}

	r.once.Do(func() {
		r.guard.Release(ctx, r.organization, r.rule, r.scopeID)
	})
}

func IsExceeded(err error) bool {
	var quotaErr *client.QuotaExceededError
	return errors.As(err, &quotaErr)
}

func Message(err error) string {
	var quotaErr *client.QuotaExceededError
	if errors.As(err, &quotaErr) {
		return quotaErr.Error()
	}
	return "Quota exceeded."
}

func (r Rule) amount() int {
	if r.Amount > 0 {
		return r.Amount
	}
	return 1
}
