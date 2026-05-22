package models

import (
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
	Title       *string
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

// AutomationSummary holds counts of automations grouped by status.
type AutomationSummary struct {
	Total    int `json:"total"`
	Active   int `json:"active"`
	Disabled int `json:"disabled"`
}
