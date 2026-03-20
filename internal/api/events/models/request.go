package models

import (
	"encoding/json"
)

// No request models needed - pagination is handled by common.ParsePagination

// EventRuleRequest represents event rule fields within an automation request
type EventRuleRequest struct {
	RuleKey     *string         `json:"rule_key" validate:"required"`
	Definition  json.RawMessage `json:"definition"`
	IsActive    *bool           `json:"is_active"`
	RepeatAble  *bool           `json:"repeat_able"`
	CooldownSec *int            `json:"cooldown_sec"`
	Description *string         `json:"description"`
}
