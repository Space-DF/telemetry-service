package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"go.uber.org/zap"
)

type QuotaExceededError struct {
	Detail string
}

func (e *QuotaExceededError) Error() string {
	if e.Detail != "" {
		return e.Detail
	}
	return "Quota exceeded."
}

type quotaRequest struct {
	Feature   string `json:"feature"`
	Amount    int    `json:"amount"`
	ScopeType string `json:"scope_type"`
	ScopeID   string `json:"scope_id,omitempty"`
}

type quotaErrorResponse struct {
	Detail string `json:"detail"`
}

// OrganizationMonitoring mirrors the console-service monitoring configuration for an organization.
type OrganizationMonitoring struct {
	CellSize        float64            `json:"cell_size"`
	Thresholds      map[string]float64 `json:"thresholds"`
	Colors          map[string]string  `json:"colors"`
	DisplaySettings map[string]bool    `json:"display_settings"`
}

// ConsoleServiceClient handles internal billing quota calls to console-service.
type ConsoleServiceClient struct {
	baseURL    string
	httpClient *http.Client
	logger     *zap.Logger
}

func NewConsoleServiceClient(logger *zap.Logger) *ConsoleServiceClient {
	if logger == nil {
		logger, _ = zap.NewProduction()
	}

	baseURL := os.Getenv("CONSOLE_SERVICE_URL")
	if baseURL == "" {
		baseURL = "http://console/api"
	}

	return &ConsoleServiceClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		logger:  logger,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (c *ConsoleServiceClient) ReserveQuota(ctx context.Context, organization string, feature string, amount int, scopeType, scopeID string) (bool, error) {
	resp, err := c.sendQuotaRequest(ctx, "/billing/internal/quota/reserve", organization, quotaRequest{
		Feature:   feature,
		Amount:    amount,
		ScopeType: scopeType,
		ScopeID:   scopeID,
	})
	if err != nil {
		c.logger.Warn("reserve quota request failed; failing open", zap.String("organization", organization), zap.String("feature", feature), zap.Error(err))
		return false, nil
	}
	defer closeResponseBody(c.logger, resp)

	if resp.StatusCode == http.StatusOK {
		return true, nil
	}

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusForbidden {
		return false, &QuotaExceededError{Detail: quotaErrorDetail(body)}
	}

	c.logger.Warn("reserve quota returned unexpected status; failing open",
		zap.String("organization", organization),
		zap.String("feature", feature),
		zap.Int("status_code", resp.StatusCode),
		zap.String("response", string(body)),
	)
	return false, nil
}

func (c *ConsoleServiceClient) ReleaseQuota(ctx context.Context, organization string, feature string, amount int, scopeType, scopeID string) {
	resp, err := c.sendQuotaRequest(ctx, "/billing/internal/quota/release", organization, quotaRequest{
		Feature:   feature,
		Amount:    amount,
		ScopeType: scopeType,
		ScopeID:   scopeID,
	})
	if err != nil {
		c.logger.Warn("release quota request failed", zap.String("organization", organization), zap.String("feature", feature), zap.Error(err))
		return
	}
	defer closeResponseBody(c.logger, resp)

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		c.logger.Warn("release quota returned unexpected status",
			zap.String("organization", organization),
			zap.String("feature", feature),
			zap.Int("status_code", resp.StatusCode),
			zap.String("response", string(body)),
		)
	}
}

func (c *ConsoleServiceClient) GetOrganizationMonitoring(ctx context.Context, organization string) (*OrganizationMonitoring, error) {
	if c == nil || c.baseURL == "" {
		return nil, errors.New("console service URL is empty")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/organizations/monitoring", nil)
	if err != nil {
		return nil, fmt.Errorf("create monitoring request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Organization", organization)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("call console-service monitoring API: %w", err)
	}
	defer closeResponseBody(c.logger, resp)

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("console-service monitoring API returned %d: %s", resp.StatusCode, string(body))
	}

	var payload []OrganizationMonitoring
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, fmt.Errorf("decode monitoring response: %w", err)
	}
	if len(payload) == 0 {
		return nil, nil
	}
	return &payload[0], nil
}

func ResolveThresholds(defaultSafe, defaultCaution, defaultWarning float64, monitoring *OrganizationMonitoring) (float64, float64, float64) {
	safe := defaultSafe
	caution := defaultCaution
	warning := defaultWarning
	if monitoring == nil || len(monitoring.Thresholds) == 0 {
		return safe, caution, warning
	}

	if value, ok := monitoring.Thresholds["caution"]; ok && value > 0 {
		caution = convertMeterThresholdToCm(value)
	}
	if value, ok := monitoring.Thresholds["warning"]; ok && value > 0 {
		warning = convertMeterThresholdToCm(value)
	}
	if value, ok := monitoring.Thresholds["safe"]; ok && value > 0 {
		safe = convertMeterThresholdToCm(value)
	}
	return safe, caution, warning
}

func convertMeterThresholdToCm(value float64) float64 {
	if value > 0 && value <= 10 {
		return value * 100
	}
	return value
}

func (c *ConsoleServiceClient) sendQuotaRequest(ctx context.Context, path, organization string, payload quotaRequest) (*http.Response, error) {
	if c.baseURL == "" {
		return nil, errors.New("console service URL is empty")
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal quota request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create quota request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Organization", organization)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("call console-service: %w", err)
	}
	return resp, nil
}

func quotaErrorDetail(body []byte) string {
	var payload quotaErrorResponse
	if err := json.Unmarshal(body, &payload); err == nil && payload.Detail != "" {
		return payload.Detail
	}
	return "Quota exceeded."
}

func closeResponseBody(logger *zap.Logger, resp *http.Response) {
	if resp == nil || resp.Body == nil {
		return
	}
	if err := resp.Body.Close(); err != nil && logger != nil {
		logger.Warn("failed to close console-service response body", zap.Error(err))
	}
}
