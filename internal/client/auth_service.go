package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"go.uber.org/zap"
)

// GetSpaceUsersResponse represents the response from the auth-service API
type GetSpaceUsersResponse struct {
	UserIDs []string    `json:"user_ids"`
	Users   []SpaceUser `json:"users"`
}

type SpaceUser struct {
	ID       string `json:"id"`
	SlugName string `json:"slug_name"`
}

// AuthServiceClient provides methods to interact with the auth-service
type AuthServiceClient struct {
	baseURL    string
	httpClient *http.Client
	logger     *zap.Logger
}

// NewAuthServiceClient creates a new auth service client
func NewAuthServiceClient(logger *zap.Logger) *AuthServiceClient {
	baseURL := os.Getenv("AUTH_SERVICE_URL")
	if baseURL == "" {
		baseURL = "http://auth/api"
	}
	return &AuthServiceClient{
		baseURL:    strings.TrimRight(baseURL, "/"),
		httpClient: http.DefaultClient,
		logger:     logger,
	}
}

// GetUserIDs fetches user IDs in an organization, optionally filtered by X-Space.
func (c *AuthServiceClient) GetUsers(ctx context.Context, orgSlug, spaceSlug string) ([]SpaceUser, error) {
	url := fmt.Sprintf("%s/users", c.baseURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		c.logger.Error("failed to create request to auth-service",
			zap.Error(err),
			zap.String("org_slug", orgSlug),
			zap.String("space_slug", spaceSlug),
		)
		return nil, fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Organization", orgSlug)
	if spaceSlug != "" {
		req.Header.Set("X-Space", spaceSlug)
	}

	httpClient := c.httpClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		c.logger.Error("failed to call auth-service",
			zap.Error(err),
			zap.String("org_slug", orgSlug),
			zap.String("space_slug", spaceSlug),
		)
		return nil, fmt.Errorf("call auth-service: %w", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil && c.logger != nil {
			c.logger.Warn("failed to close auth-service response body",
				zap.Error(closeErr),
				zap.String("org_slug", orgSlug),
				zap.String("space_slug", spaceSlug),
			)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		c.logger.Warn("auth-service returned non-200 status",
			zap.Int("status_code", resp.StatusCode),
			zap.String("org_slug", orgSlug),
			zap.String("space_slug", spaceSlug),
			zap.String("response", string(body)),
		)
		return nil, fmt.Errorf("auth-service returned status %d", resp.StatusCode)
	}

	var result GetSpaceUsersResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		c.logger.Error("failed to decode auth-service response",
			zap.Error(err),
			zap.String("org_slug", orgSlug),
			zap.String("space_slug", spaceSlug),
		)
		return nil, fmt.Errorf("decode response: %w", err)
	}

	if len(result.Users) > 0 {
		return result.Users, nil
	}

	if len(result.UserIDs) == 0 {
		return nil, nil
	}

	users := make([]SpaceUser, 0, len(result.UserIDs))
	for _, userID := range result.UserIDs {
		users = append(users, SpaceUser{ID: userID})
	}
	return users, nil
}
