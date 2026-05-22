package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"

	"go.uber.org/zap"
)

// GetSpaceUsersResponse represents the response from the auth-service API
type GetSpaceUsersResponse struct {
	SpaceID    string   `json:"space_id"`
	SpaceName  string   `json:"space_name"`
	UserIDs    []string `json:"user_ids"`
	TotalUsers int      `json:"total_users"`
}

// AuthServiceClient provides methods to interact with the auth-service
type AuthServiceClient struct {
	baseURL string
	logger  *zap.Logger
}

// NewAuthServiceClient creates a new auth service client
func NewAuthServiceClient(logger *zap.Logger) *AuthServiceClient {
	baseURL := os.Getenv("AUTH_SERVICE_URL")
	if baseURL == "" {
		baseURL = "http://auth/api"
	}
	return &AuthServiceClient{
		baseURL: baseURL,
		logger:  logger,
	}
}

// GetSpaceUserIDs fetches all user IDs in a space from the auth-service API
func (c *AuthServiceClient) GetSpaceUserIDs(ctx context.Context, orgSlug, spaceID string) ([]string, error) {
	url := fmt.Sprintf("%s/spaces/%s/users", c.baseURL, spaceID)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		c.logger.Error("failed to create request to auth-service", zap.Error(err))
		return nil, fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Organization", orgSlug)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		c.logger.Error("failed to call auth-service", zap.Error(err), zap.String("org_slug", orgSlug), zap.String("space_id", spaceID))
		return nil, fmt.Errorf("call auth-service: %w", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil && c.logger != nil {
			c.logger.Warn("failed to close auth-service response body",
				zap.String("org_slug", orgSlug),
				zap.String("space_id", spaceID),
				zap.Error(closeErr))
		}
	}()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		c.logger.Warn("auth-service returned non-200 status",
			zap.Int("status_code", resp.StatusCode),
			zap.String("org_slug", orgSlug),
			zap.String("space_id", spaceID),
			zap.String("response", string(body)))
		return nil, fmt.Errorf("auth-service returned status %d", resp.StatusCode)
	}

	var result GetSpaceUsersResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		c.logger.Error("failed to decode auth-service response", zap.Error(err))
		return nil, fmt.Errorf("decode response: %w", err)
	}

	if len(result.UserIDs) == 0 {
		return nil, nil
	}

	return result.UserIDs, nil
}
