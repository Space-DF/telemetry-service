package models

import "time"

// OrgEventType represents the type of organization event
type OrgEventType string

const (
	// OrgCreated is emitted when a new organization is created
	OrgCreated OrgEventType = "org.created"
	// OrgUpdated is emitted when an organization is updated
	OrgUpdated OrgEventType = "org.updated"
	// OrgDeleted is emitted when an organization is deleted
	OrgDeleted OrgEventType = "org.deleted"
	// OrgDeactivated is emitted when an organization is deactivated
	OrgDeactivated OrgEventType = "org.deactivated"
	// OrgActivated is emitted when an organization is activated
	OrgActivated OrgEventType = "org.activated"
	// OrgDiscoveryReq is emitted when requesting active orgs
	OrgDiscoveryReq OrgEventType = "org.discovery.request"
	// OrgDiscoveryResp is the response with active orgs
	OrgDiscoveryResp OrgEventType = "org.discovery.response"
)

// OrgEvent represents an organization lifecycle event
type OrgEvent struct {
	EventType OrgEventType    `json:"event_type"`
	EventID   string          `json:"event_id"`
	Timestamp time.Time       `json:"timestamp"`
	Payload   OrgEventPayload `json:"payload"`
}

// OrgEventPayload contains organization details
type OrgEventPayload struct {
	ID               string    `json:"id"`
	Slug             string    `json:"slug"`
	Name             string    `json:"name"`
	Vhost            string    `json:"vhost,omitempty"`
	Exchange         string    `json:"exchange,omitempty"`
	TransformerQueue string    `json:"transformer_queue,omitempty"`
	TelemetryQueue   string    `json:"telemetry_queue,omitempty"`
	IsActive         bool      `json:"is_active"`
	CreatedAt        time.Time `json:"created_at,omitempty"`
	UpdatedAt        time.Time `json:"updated_at,omitempty"`
}

// OrgDiscoveryRequest is sent to discover active organizations
type OrgDiscoveryRequest struct {
	EventType   OrgEventType `json:"event_type"`
	EventID     string       `json:"event_id"`
	Timestamp   time.Time    `json:"timestamp"`
	ServiceName string       `json:"service_name"`
	ReplyTo     string       `json:"reply_to"`
}

// OrgDiscoveryResponse contains all active organizations
type OrgDiscoveryResponse struct {
	EventType  OrgEventType `json:"event_type"`
	EventID    string       `json:"event_id"`
	Timestamp  time.Time    `json:"timestamp"`
	Spaces     []OrgInfo    `json:"spaces"`
	TotalCount int          `json:"total_count"`
}

// OrgInfo contains basic organization information
type OrgInfo struct {
	Slug     string `json:"slug"`
	Name     string `json:"name"`
	Vhost    string `json:"vhost"`
	IsActive bool   `json:"is_active"`
}