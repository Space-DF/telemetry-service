package notifications

import (
	"crypto/ecdh"
	"encoding/base64"
	"net/http"

	"github.com/Space-DF/telemetry-service/internal/api/common"
	apimodels "github.com/Space-DF/telemetry-service/internal/api/notifications/models"
	"github.com/Space-DF/telemetry-service/internal/timescaledb"
	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
)

// isValidP256DHKey validates that the P256DH key is a valid base64url-encoded P-256 public key
func isValidP256DHKey(p256dh string) bool {
	keyBytes, err := base64.StdEncoding.DecodeString(p256dh)
	if err != nil {
		return false
	}

	// P-256 public key should be 65 bytes (1 byte 0x04 + 32 bytes x + 32 bytes y)
	if len(keyBytes) != 65 || keyBytes[0] != 0x04 {
		return false
	}

	_, err = ecdh.P256().NewPublicKey(keyBytes)
	return err == nil
}

// SubscribeHandler handles POST /v1/notifications/subscribe
// @Summary Subscribe to push notifications
// @Description Subscribe a user/device to receive web push notifications
// @Tags notifications
// @Accept json
// @Produce json
// @Param request body models.PushSubscriptionRequest true "Subscription details from browser"
// @Failure 400 {object} map[string]string "Invalid request"
// @Failure 401 {object} map[string]string "Unauthorized"
// @Failure 500 {object} map[string]string "Internal server error"
// @Router /telemetry/v1/notifications/subscribe [post]
// @Security Bearer
func SubscribeHandler(
	logger *zap.Logger,
	tsClient *timescaledb.Client,
) echo.HandlerFunc {
	return func(c echo.Context) error {
		org := common.ResolveOrgFromRequest(c)
		if org == "" {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "Could not determine organization from hostname or X-Organization header",
			})
		}

		// Get user ID from context (set by auth middleware)
		userID, _ := c.Get("user_id").(string)
		if userID == "" {
			return c.JSON(http.StatusUnauthorized, map[string]string{
				"error": "user_id not found in context",
			})
		}

		// Parse request
		var req apimodels.PushSubscriptionRequest
		if err := c.Bind(&req); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "invalid request body",
			})
		}

		// Validate required fields
		if req.Endpoint == "" || req.P256DH == "" || req.Auth == "" {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "endpoint, p256dh, and auth are required",
			})
		}

		// Validate P256DH key format (should be valid base64url)
		if !isValidP256DHKey(req.P256DH) {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "invalid p256dh key format",
			})
		}

		ctx := timescaledb.ContextWithOrg(c.Request().Context(), org)

		// Store subscription
		subscription, err := tsClient.StorePushSubscription(ctx, userID, &req)
		if err != nil {
			logger.Error("failed to store push subscription",
				zap.String("user_id", userID),
				zap.Error(err))
			return c.JSON(http.StatusInternalServerError, map[string]string{
				"error": "failed to store subscription",
			})
		}

		response := apimodels.PushSubscriptionResponse{
			ID:        subscription.ID,
			Endpoint:  subscription.Endpoint,
			CreatedAt: subscription.CreatedAt,
		}

		return c.JSON(http.StatusOK, response)
	}
}

// UnsubscribeHandler handles DELETE /v1/notifications/subscribe/:id
// @Summary Unsubscribe from push notifications
// @Description Remove a push notification subscription
// @Tags notifications
// @Param id path string true "Subscription ID"
// @Success 204
// @Failure 400 {object} map[string]string "Invalid request"
// @Failure 401 {object} map[string]string "Unauthorized"
// @Failure 404 {object} map[string]string "Subscription not found"
// @Failure 500 {object} map[string]string "Internal server error"
// @Router /telemetry/v1/notifications/subscribe/{id} [delete]
// @Security Bearer
func UnsubscribeHandler(
	logger *zap.Logger,
	tsClient *timescaledb.Client,
) echo.HandlerFunc {
	return func(c echo.Context) error {
		org := common.ResolveOrgFromRequest(c)
		if org == "" {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "Could not determine organization from hostname or X-Organization header",
			})
		}

		subscriptionID := c.Param("id")
		if subscriptionID == "" {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "The id is required",
			})
		}

		userID, _ := c.Get("user_id").(string)
		if userID == "" {
			return c.JSON(http.StatusUnauthorized, map[string]string{
				"error": "user_id not found in context",
			})
		}

		ctx := timescaledb.ContextWithOrg(c.Request().Context(), org)
		err := tsClient.DeletePushSubscription(ctx, userID, subscriptionID)
		if err != nil {
			logger.Error("failed to delete push subscription",
				zap.String("user_id", userID),
				zap.String("subscription id", subscriptionID),
				zap.Error(err))
			return c.JSON(http.StatusInternalServerError, map[string]string{
				"error": "failed to delete subscription",
			})
		}

		return c.NoContent(http.StatusNoContent)
	}
}
