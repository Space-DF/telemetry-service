package models

import "time"

// HealthResponse represents the overall health status
type HealthResponse struct {
	Status    string                    `json:"status"`
	Timestamp string                    `json:"timestamp"`
	Checks    map[string]ComponentCheck `json:"checks"`
}

// ComponentCheck represents the health status of a single component
type ComponentCheck struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}

// ReadyResponse represents the readiness status
type ReadyResponse struct {
	Ready     bool   `json:"ready"`
	Message   string `json:"message"`
	Timestamp string `json:"timestamp"`
}

// LiveResponse represents the liveness status
type LiveResponse struct {
	Live      bool   `json:"live"`
	Timestamp string `json:"timestamp"`
}

// NewHealthResponse creates a new health response
func NewHealthResponse() *HealthResponse {
	return &HealthResponse{
		Status:    "healthy",
		Timestamp: time.Now().Format(time.RFC3339),
		Checks:    make(map[string]ComponentCheck),
	}
}

// NewReadyResponse creates a new ready response
func NewReadyResponse(ready bool, message string) *ReadyResponse {
	return &ReadyResponse{
		Ready:     ready,
		Message:   message,
		Timestamp: time.Now().Format(time.RFC3339),
	}
}

// NewLiveResponse creates a new live response
func NewLiveResponse() *LiveResponse {
	return &LiveResponse{
		Live:      true,
		Timestamp: time.Now().Format(time.RFC3339),
	}
}
