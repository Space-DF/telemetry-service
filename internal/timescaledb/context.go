package timescaledb

import (
	"context"
	"strings"
)

// context key type for organization
type ctxKeyOrg struct{}

// contextWithOrg returns a new context that carries the organization slug
func contextWithOrg(ctx context.Context, org string) context.Context {
	return context.WithValue(ctx, ctxKeyOrg{}, org)
}

// ContextWithOrg is exported helper to attach organization slug to context
func ContextWithOrg(ctx context.Context, org string) context.Context {
	return contextWithOrg(ctx, org)
}

// orgFromContext extracts the organization slug from context if present
func orgFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if v := ctx.Value(ctxKeyOrg{}); v != nil {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// OrgFromContext is the exported version of orgFromContext
func OrgFromContext(ctx context.Context) string {
	return orgFromContext(ctx)
}

// pqQuoteIdentifier quotes an identifier for Postgres (very small helper)
func pqQuoteIdentifier(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
