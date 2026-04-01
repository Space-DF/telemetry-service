package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"go.uber.org/zap"
)

// DeviceSpaceInfo represents device space information from device-service
type DeviceSpaceInfo struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	SpaceID   string `json:"space_id,omitempty"`
	SpaceSlug string `json:"space_slug,omitempty"`
}

// DeviceServiceClient handles communication with device-service
type DeviceServiceClient struct {
	baseURL    string
	httpClient *http.Client
	logger     *zap.Logger
}

// NewDeviceServiceClient creates a new device service client
func NewDeviceServiceClient(logger *zap.Logger) *DeviceServiceClient {
	if logger == nil {
		logger, _ = zap.NewProduction()
	}

	baseURL := os.Getenv("DEVICE_SERVICE_BASE_URL")
	if baseURL == "" {
		// Fallback for local development
		baseURL = "http://device/api"
	}

	return &DeviceServiceClient{
		baseURL: baseURL,
		logger:  logger,
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

// GetDeviceSpaceByDeviceID fetches device space information by device ID
// Uses the device-spaces endpoint with device_id query parameter
func (c *DeviceServiceClient) GetDeviceSpaceByDeviceID(ctx context.Context, deviceID, organization, spaceID string) (*DeviceSpaceInfo, error) {
	if deviceID == "" {
		c.logger.Info("device_id is empty")
		return nil, fmt.Errorf("device_id is required")
	}

	if organization == "" {
		c.logger.Info("organization is empty")
		return nil, fmt.Errorf("organization is required")
	}

	if spaceID == "" {
		c.logger.Info("space_id is empty")
		return nil, fmt.Errorf("space_id is required")
	}

	// Build the URL: GET {baseURL}/device-spaces/{device_id}/internal
	url := fmt.Sprintf("%s/device-spaces/%s/internal", c.baseURL, deviceID)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		c.logger.Error("failed to create request", zap.Error(err), zap.String("device_id", deviceID))
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Add required headers
	req.Header.Set("X-Organization", organization)
	req.Header.Set("X-Space", spaceID)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		c.logger.Error("failed to call device-service", zap.Error(err), zap.String("device_id", deviceID), zap.String("url", url))
		return nil, fmt.Errorf("failed to call device-service: %w", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			c.logger.Warn("failed to close response body", zap.Error(err))
		}
	}()

	if resp.StatusCode == http.StatusNotFound {
		c.logger.Info("device not found in device-service", zap.String("device_id", deviceID))
		return nil, nil // Device not found, return nil without error
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		c.logger.Warn("device-service returned error", zap.Int("status_code", resp.StatusCode), zap.String("device_id", deviceID), zap.String("response_body", string(body)))
		return nil, fmt.Errorf("device-service returned status %d: %s", resp.StatusCode, string(body))
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		c.logger.Error("failed to read response body", zap.Error(err), zap.String("device_id", deviceID))
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	// Try parsing as direct DeviceSpaceInfo object first
	var info DeviceSpaceInfo
	if err := json.Unmarshal(bodyBytes, &info); err == nil && info.ID != "" {
		return &info, nil
	}

	// Fallback: try parsing as paginated response with results array
	var result struct {
		Results []DeviceSpaceInfo `json:"results"`
		Count   int               `json:"count"`
	}

	if err := json.Unmarshal(bodyBytes, &result); err == nil && len(result.Results) > 0 {
		info := &result.Results[0]
		return info, nil
	}

	c.logger.Info("no device space found in response", zap.String("device_id", deviceID))
	return nil, nil // No results found
}
