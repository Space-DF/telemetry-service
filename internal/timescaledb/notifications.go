package timescaledb

import (
	"context"
	"fmt"

	"github.com/lib/pq"

	apimodels "github.com/Space-DF/telemetry-service/internal/api/notifications/models"
	"github.com/Space-DF/telemetry-service/internal/models"
	"github.com/google/uuid"
)

// StorePushSubscription stores or updates a push subscription in the organization schema.
func (c *Client) StorePushSubscription(
	ctx context.Context,
	userID string,
	request *apimodels.PushSubscriptionRequest,
) (*models.PushSubscription, error) {
	org := orgFromContext(ctx)
	if org == "" {
		return nil, fmt.Errorf("organization not found in context")
	}

	schema := pqQuoteIdentifier(org)
	query := fmt.Sprintf(`
		INSERT INTO %s.push_subscriptions (id, user_id, endpoint, p256dh, auth)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (endpoint) DO UPDATE
		SET p256dh = EXCLUDED.p256dh, auth = EXCLUDED.auth, user_id = EXCLUDED.user_id, updated_at = NOW()
		RETURNING id, user_id, endpoint, p256dh, auth, created_at, updated_at
	`, schema)

	subscription := &models.PushSubscription{}
	err := c.DB.QueryRowContext(ctx, query,
		uuid.New().String(), userID, request.Endpoint, request.P256DH, request.Auth,
	).Scan(
		&subscription.ID,
		&subscription.UserID,
		&subscription.Endpoint,
		&subscription.P256DH,
		&subscription.Auth,
		&subscription.CreatedAt,
		&subscription.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to store push subscription: %w", err)
	}

	return subscription, nil
}

// GetPushSubscriptionsBySpace gets active subscriptions in the target organization.
// GetPushSubscriptionsByUserIDs gets active subscriptions for multiple users in an organization.
func (c *Client) GetPushSubscriptionsByUserIDs(
	ctx context.Context,
	orgSlug string,
	userIDs []string,
) ([]*models.PushSubscription, error) {

	if len(userIDs) == 0 {
		return []*models.PushSubscription{}, nil
	}

	schema := pqQuoteIdentifier(orgSlug)

	query := fmt.Sprintf(`
		SELECT id, user_id, endpoint, p256dh, auth, created_at, updated_at
		FROM %s.push_subscriptions
		WHERE user_id = ANY($1)
		ORDER BY created_at DESC
	`, schema)

	rows, err := c.DB.QueryContext(ctx, query, pq.Array(userIDs))
	if err != nil {
		return nil, fmt.Errorf("failed to query push subscriptions: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()

	var subscriptions []*models.PushSubscription
	for rows.Next() {
		sub, err := scanPushSubscription(rows)
		if err != nil {
			return nil, err
		}
		subscriptions = append(subscriptions, sub)
	}

	return subscriptions, rows.Err()
}

// DeletePushSubscription deletes a push subscription for the given user in the current organization schema.
func (c *Client) DeletePushSubscription(ctx context.Context, userID, subscriptionID string) error {
	org := orgFromContext(ctx)
	if org == "" {
		return fmt.Errorf("organization not found in context")
	}

	return c.DeletePushSubscriptionById(ctx, org, userID, subscriptionID)
}

// DeletePushSubscriptionById deletes a push subscription for the given user in the target organization schema.
func (c *Client) DeletePushSubscriptionById(ctx context.Context, orgSlug, userID, subscriptionID string) error {
	schema := pqQuoteIdentifier(orgSlug)
	query := fmt.Sprintf(`
		DELETE FROM %s.push_subscriptions
		WHERE id = $1 AND user_id = $2
	`, schema)

	result, err := c.DB.ExecContext(ctx, query, subscriptionID, userID)
	if err != nil {
		return fmt.Errorf("failed to delete push subscription: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return fmt.Errorf("subscription not found")
	}

	return nil
}

type pushSubscriptionScanner interface {
	Scan(dest ...interface{}) error
}

func scanPushSubscription(scanner pushSubscriptionScanner) (*models.PushSubscription, error) {
	subscription := &models.PushSubscription{}

	err := scanner.Scan(
		&subscription.ID,
		&subscription.UserID,
		&subscription.Endpoint,
		&subscription.P256DH,
		&subscription.Auth,
		&subscription.CreatedAt,
		&subscription.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to scan push subscription: %w", err)
	}

	return subscription, nil
}
