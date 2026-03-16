package models

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Action represents an action that can be triggered by automations
type Action struct {
	ID        string    `json:"id" db:"id"`
	Name      string    `json:"name" db:"name"`
	Key       string    `json:"key" db:"key"`
	Data      *string   `json:"data,omitempty" db:"data"`
	CreatedAt time.Time `json:"created_at" db:"created_at"`
}

// Automation represents an automation database row
type Automation struct {
	ID          string
	Name        string
	DeviceID    string
	EventRuleID *string
	EventRule   *EventRule
	SpaceID     *uuid.UUID
	UpdatedAt   time.Time
	CreatedAt   time.Time
}

// AutomationWithActions represents an automation with its associated actions
type AutomationWithActions struct {
	Automation
	Actions []Action
}

// EventRuleFields represents event rule fields within an automation request
type EventRuleFields struct {
	RuleKey     *string         `json:"rule_key" validate:"required"`
	Definition  json.RawMessage `json:"definition"`
	IsActive    *bool           `json:"is_active"`
	RepeatAble  *bool           `json:"repeat_able"`
	CooldownSec *int            `json:"cooldown_sec"`
	Description *string         `json:"description"`
}

// AutomationRequest represents a request to create or update an automation
type AutomationRequest struct {
	Name      *string          `json:"name" validate:"required,min=1,max=100"`
	DeviceID  string           `json:"device_id" validate:"required,uuid"`
	SpaceID   *uuid.UUID       `json:"space_id,omitempty" validate:"omitempty"`
	ActionIDs []string         `json:"action_ids" validate:"required,dive,uuid"`
	EventRule *EventRuleFields `json:"event_rule,omitempty"`
}
