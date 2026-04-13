package models

import (
	"github.com/Space-DF/telemetry-service/internal/api/events/models"
	"github.com/google/uuid"
)

// Request models for automations API.
// Pagination is handled by common.ParsePagination() in internal/api/common/pagination.go

type AutomationRequest struct {
	Name      *string                  `json:"name" validate:"required,min=1,max=100"`
	Title     *string                  `json:"title" validate:"omitempty,min=1,max=255"`
	DeviceID  string                   `json:"device_id" validate:"required,uuid"`
	SpaceID   *uuid.UUID               `json:"space_id,omitempty" validate:"omitempty"`
	ActionIDs []string                 `json:"action_ids,omitempty" validate:"omitempty,dive,uuid"`
	EventRule *models.EventRuleRequest `json:"event_rule,omitempty"`
}

// ActionRequest represents a request to create or update an action
type ActionRequest struct {
	Name string                 `json:"name" validate:"required,min=1,max=100"`
	Key  string                 `json:"key" validate:"required,min=1,max=100"`
	Data map[string]interface{} `json:"data,omitempty"`
}
