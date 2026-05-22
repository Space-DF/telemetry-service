package middleware

import (
	"github.com/labstack/echo/v4"
	"net/http"
)

// UserIDFromHeader extracts user_id from X-User-ID header (set by HAProxy)
// and stores it in the Echo context for use in handlers
func UserIDFromHeader() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			// Extract X-User-ID header set by HAProxy
			userID := c.Request().Header.Get("X-User-ID")
			if userID == "" {
				// Or return an error appropriate for your auth flow
				return c.JSON(http.StatusUnauthorized, map[string]string{"error": "Missing X-User-ID header"})
			}
			c.Set("user_id", userID)
			return next(c)
		}
	}
}
