package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"time"

	"go.uber.org/zap"
)

type DeviceEntityTemplate struct {
	Key          string   `json:"key"`
	UniqueID     string   `json:"unique_id"`
	ModelKey     string   `json:"model_key"`
	EntityType   string   `json:"entity_type"`
	Category     string   `json:"category"`
	Name         string   `json:"name"`
	Manufacturer string   `json:"manufacturer"`
	UnitOfMeas   string   `json:"unit_of_measurement"`
	Icon         string   `json:"icon"`
	DisplayType  []string `json:"display_type"`
}

type TransformerServiceClient struct {
	baseURL    string
	httpClient *http.Client
	logger     *zap.Logger
}

func NewTransformerServiceClient(logger *zap.Logger) *TransformerServiceClient {
	if logger == nil {
		logger, _ = zap.NewProduction()
	}

	baseURL := os.Getenv("TRANSFORMER_SERVICE_BASE_URL")
	if baseURL == "" {
		baseURL = "http://transformer:8080/api"
	}

	return &TransformerServiceClient{
		baseURL: baseURL,
		logger:  logger,
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

func (c *TransformerServiceClient) GetDeviceEntityTemplates(ctx context.Context, deviceModelID string) ([]DeviceEntityTemplate, error) {
	if deviceModelID == "" {
		return nil, fmt.Errorf("device_model_id is required")
	}

	endpoint := fmt.Sprintf("%s/device-models/%s/entities",
		c.baseURL,
		url.PathEscape(deviceModelID),
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to call transformer-service: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("transformer-service returned status %d: %s", resp.StatusCode, string(body))
	}

	var result struct {
		Results []DeviceEntityTemplate `json:"results"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode transformer-service response: %w", err)
	}

	return result.Results, nil
}
