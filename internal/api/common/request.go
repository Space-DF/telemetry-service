package common

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v4"
)

// ResolveOrgFromRequest extracts organization from hostname (like spacedf.localhost)
// or from X-Organization header with header taking priority.
// Format: {org_slug}.{domain} where domain is configured in settings
func ResolveOrgFromRequest(c echo.Context) string {
	// First try X-Organization header (for explicit control)
	if orgHeader := c.Request().Header.Get("X-Organization"); orgHeader != "" {
		return orgHeader
	}

	// Extract from hostname: org_slug.domain
	hostname := c.Request().Host
	if hostname == "" {
		return ""
	}

	parts := strings.Split(hostname, ".")
	if len(parts) > 0 && parts[0] != "" {
		return parts[0]
	}

	return ""
}

// ResolveSpaceSlugFromRequest extracts space_slug from X-Space header.
// Returns an error if the header is missing.
func ResolveSpaceSlugFromRequest(c echo.Context) (string, error) {
	spaceSlug := c.Request().Header.Get("X-Space")
	if spaceSlug == "" {
		return "", echo.NewHTTPError(http.StatusBadRequest, "X-Space header is required")
	}
	return spaceSlug, nil
}
