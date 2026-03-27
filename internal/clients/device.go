package clients

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// DeviceClient is a thin HTTP client for the device-service API.
type DeviceClient struct {
	baseURL    string
	httpClient *http.Client
}

// NewDeviceClient creates a DeviceClient using the DEVICE_SERVICE_BASE_URL
// environment variable (defaults to "http://device/api").
func NewDeviceClient() *DeviceClient {
	baseURL := strings.TrimRight(os.Getenv("DEVICE_SERVICE_BASE_URL"), "/")
	if baseURL == "" {
		baseURL = "http://device/api"
	}
	return &DeviceClient{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

// GetDeviceInfo fetches a device by its UUID from the device-service.
// Returns nil (no error) when the device is not found or the service is unavailable,
// so a missing device never breaks the automation response.
func (c *DeviceClient) GetDeviceInfo(deviceID, org string) map[string]interface{} {
	url := fmt.Sprintf("%s/devices/%s/", c.baseURL, deviceID)

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil
	}
	if org != "" {
		req.Header.Set("X-Organization", org)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil
	}

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil
	}
	return result
}
