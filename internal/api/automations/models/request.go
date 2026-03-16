package models

import (
	"github.com/Space-DF/telemetry-service/internal/models"
)

// This file is intentionally left empty.
// Pagination is handled by common.ParsePagination() in internal/api/common/pagination.go

type AutomationRequest = models.AutomationRequest
type AutomationWithActions = models.AutomationWithActions
type Action = models.Action
type EventRuleFields = models.EventRuleFields

// ActionRequest represents a request to create or update an action
type ActionRequest struct {
	Name string                 `json:"name" validate:"required,min=1,max=100"`
	Key  string                 `json:"key" validate:"required,min=1,max=100"`
	Data map[string]interface{} `json:"data,omitempty"`
}
