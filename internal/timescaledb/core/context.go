package core

import (
	"context"
	"strings"
)

// context key type for organization
type ctxKeyOrg struct{}

// ContextWithOrg is exported helper to attach organization slug to context
func ContextWithOrg(ctx context.Context, org string) context.Context {
	return context.WithValue(ctx, ctxKeyOrg{}, org)
}

// OrgFromContext extracts the organization slug from context if present
func OrgFromContext(ctx context.Context) string {
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

// pqQuoteIdentifier quotes an identifier for Postgres (very small helper)
func pqQuoteIdentifier(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
