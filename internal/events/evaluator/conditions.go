package evaluator

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Space-DF/telemetry-service/internal/models"
	"go.uber.org/zap"
)

type ConditionEvaluator struct {
	logger *zap.Logger
}

func NewConditionEvaluator(logger *zap.Logger) *ConditionEvaluator {
	return &ConditionEvaluator{
		logger: logger,
	}
}

// EvaluateDefinition evaluates database rule definition against an entity
func (c *ConditionEvaluator) EvaluateDefinition(definition string, entity models.TelemetryEntity, deviceID string) (bool, string, error) {
	var definitionMap map[string]interface{}
	if err := json.Unmarshal([]byte(definition), &definitionMap); err != nil {
		return false, "", fmt.Errorf("failed to parse rule definition: %w", err)
	}

	context := c.buildEvaluationContext(entity, deviceID)

	matched, matchDetails := c.evaluateConditions(definitionMap, context)
	return matched, matchDetails, nil
}

// EvaluateDefinitionWithContext evaluates database rule definition against a pre-built context.
func (c *ConditionEvaluator) EvaluateDefinitionWithContext(definition string, context map[string]interface{}) (bool, string, error) {
	var definitionMap map[string]interface{}
	if err := json.Unmarshal([]byte(definition), &definitionMap); err != nil {
		return false, "", fmt.Errorf("failed to parse rule definition: %w", err)
	}

	c.logger.Debug("Evaluating definition",
		zap.Int("definition_fields_count", len(definitionMap)),
		zap.Strings("context_keys", getMapKeys(context)))

	matched, details := c.evaluateConditions(definitionMap, context)

	c.logger.Debug("Definition evaluation result",
		zap.Bool("matched", matched),
		zap.String("details", details))

	return matched, details, nil
}

// getMapKeys returns the keys of a map for logging purposes
func getMapKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// BuildUnifiedContext creates a single context map from ALL entities.
func (c *ConditionEvaluator) BuildUnifiedContext(entities []models.TelemetryEntity, deviceID string, deviceInfo map[string]interface{}) map[string]interface{} {
	context := make(map[string]interface{})
	context["reported_at"] = time.Now()
	context["device_id"] = deviceID

	if deviceInfo != nil {
		context["device"] = deviceInfo
	}

	for _, entity := range entities {
		if entity.State == nil {
			continue
		}

		entityKey := strings.ToLower(entity.EntityType)

		switch s := entity.State.(type) {
		case map[string]interface{}:
			for k, v := range s {
				context[k] = v
			}
			context[entityKey] = s
		default:
			context[entityKey] = s
		}

		if entity.Attributes != nil {
			for k, v := range entity.Attributes {
				context[entityKey+"_"+k] = v
				if _, exists := context[k]; !exists {
					context[k] = v
				}
			}
		}
	}

	return context
}

// buildEvaluationContext creates context map for condition evaluation
func (c *ConditionEvaluator) buildEvaluationContext(entity models.TelemetryEntity, deviceID string) map[string]interface{} {
	context := make(map[string]interface{})
	context["reported_at"] = time.Now()
	context["device_id"] = deviceID
	context["entity_type"] = entity.EntityType

	if entity.State != nil {
		switch s := entity.State.(type) {
		case map[string]interface{}:
			for k, v := range s {
				context[k] = v
			}
		default:
			context[strings.ToLower(entity.EntityType)] = s
		}
	}

	return context
}

// evaluateConditions evaluates the conditions in the definition
func (c *ConditionEvaluator) evaluateConditions(definition map[string]interface{}, context map[string]interface{}) (bool, string) {
	conditions, ok := definition["conditions"]
	if !ok {
		return false, "no conditions"
	}

	conditionMap, ok := conditions.(map[string]interface{})
	if !ok {
		return false, "invalid conditions format"
	}

	return c.evaluateCondition(conditionMap, context)
}

// evaluateCondition evaluates a single condition node
func (c *ConditionEvaluator) evaluateCondition(condition map[string]interface{}, context map[string]interface{}) (bool, string) {
	if andConditions, ok := condition["and"]; ok {
		return c.evaluateAndCondition(andConditions, context)
	}

	if orConditions, ok := condition["or"]; ok {
		return c.evaluateOrCondition(orConditions, context)
	}

	if notCondition, ok := condition["not"]; ok {
		return c.evaluateNotCondition(notCondition, context)
	}

	if result, details, handled := c.evaluateSpecialCondition(condition, context); handled {
		return result, details
	}

	for field, rule := range condition {
		return c.evaluateFieldCondition(field, rule, context)
	}

	return false, "no valid condition found"
}

// evaluateAndCondition evaluates AND logic
func (c *ConditionEvaluator) evaluateAndCondition(andConditions interface{}, context map[string]interface{}) (bool, string) {
	conditions, ok := andConditions.([]interface{})
	if !ok {
		return false, "invalid and conditions format"
	}

	var details []string
	for _, cond := range conditions {
		condMap, ok := cond.(map[string]interface{})
		if !ok {
			continue
		}

		result, detail := c.evaluateCondition(condMap, context)
		if !result {
			return false, fmt.Sprintf("AND failed on: %s", detail)
		}
		details = append(details, detail)
	}

	return true, fmt.Sprintf("AND(%s)", strings.Join(details, ", "))
}

// evaluateOrCondition evaluates OR logic
func (c *ConditionEvaluator) evaluateOrCondition(orConditions interface{}, context map[string]interface{}) (bool, string) {
	conditions, ok := orConditions.([]interface{})
	if !ok {
		return false, "invalid or conditions format"
	}

	var details []string
	for _, cond := range conditions {
		condMap, ok := cond.(map[string]interface{})
		if !ok {
			continue
		}

		result, detail := c.evaluateCondition(condMap, context)
		if result {
			return true, fmt.Sprintf("OR matched: %s", detail)
		}
		details = append(details, detail)
	}

	return false, fmt.Sprintf("OR failed on all: %s", strings.Join(details, ", "))
}

// evaluateNotCondition evaluates NOT logic
func (c *ConditionEvaluator) evaluateNotCondition(notCondition interface{}, context map[string]interface{}) (bool, string) {
	conditions, ok := notCondition.([]interface{})
	if !ok {
		return false, "invalid not condition format"
	}

	var details []string
	for _, cond := range conditions {
		condMap, ok := cond.(map[string]interface{})
		if !ok {
			continue
		}

		result, detail := c.evaluateCondition(condMap, context)
		if result {
			return false, fmt.Sprintf("NOT failed on: %s", detail)
		}
		details = append(details, detail)
	}

	return true, fmt.Sprintf("NOT(%s)", strings.Join(details, ", "))
}

// evaluateSpecialCondition handles special condition types
func (c *ConditionEvaluator) evaluateSpecialCondition(condition map[string]interface{}, context map[string]interface{}) (bool, string, bool) {
	if weekdayRule, ok := condition["weekday_between"]; ok {
		return c.evaluateWeekdayBetween(weekdayRule, context), "weekday_between", true
	}

	if weekdayInRule, ok := condition["weekday_in"]; ok {
		return c.evaluateWeekdayIn(weekdayInRule, context), "weekday_in", true
	}

	if timeRule, ok := condition["time_between"]; ok {
		return c.evaluateTimeBetween(timeRule, context), "time_between", true
	}

	if deviceRule, ok := condition["device"]; ok {
		return c.evaluateDeviceCondition(deviceRule, context), "device", true
	}

	return false, "", false
}

// evaluateFieldCondition evaluates field comparison conditions
func (c *ConditionEvaluator) evaluateFieldCondition(field string, rule interface{}, context map[string]interface{}) (bool, string) {
	ruleMap, ok := rule.(map[string]interface{})
	if !ok {
		return false, fmt.Sprintf("invalid rule format for field %s", field)
	}

	value, exists := context[field]
	if !exists {
		return false, fmt.Sprintf("field %s not found", field)
	}

	floatValue, ok := c.convertToFloat64(value)
	if !ok {
		return false, fmt.Sprintf("field %s cannot be converted to number", field)
	}

	for op, expectedValue := range ruleMap {
		expectedFloat, ok := c.convertToFloat64(expectedValue)
		if !ok {
			continue
		}

		switch op {
		case "lte":
			if !(floatValue <= expectedFloat) {
				return false, fmt.Sprintf("%s %.2f not <= %.2f", field, floatValue, expectedFloat)
			}
		case "gte":
			if !(floatValue >= expectedFloat) {
				return false, fmt.Sprintf("%s %.2f not >= %.2f", field, floatValue, expectedFloat)
			}
		case "lt":
			if !(floatValue < expectedFloat) {
				return false, fmt.Sprintf("%s %.2f not < %.2f", field, floatValue, expectedFloat)
			}
		case "gt":
			if !(floatValue > expectedFloat) {
				return false, fmt.Sprintf("%s %.2f not > %.2f", field, floatValue, expectedFloat)
			}
		case "eq":
			if !(floatValue == expectedFloat) {
				return false, fmt.Sprintf("%s %.2f not == %.2f", field, floatValue, expectedFloat)
			}
		case "ne":
			if !(floatValue != expectedFloat) {
				return false, fmt.Sprintf("%s %.2f not != %.2f", field, floatValue, expectedFloat)
			}
		}
	}

	return true, fmt.Sprintf("%s conditions passed", field)
}

// evaluateWeekdayBetween evaluates weekday range conditions
func (c *ConditionEvaluator) evaluateWeekdayBetween(rule interface{}, context map[string]interface{}) bool {
	ruleMap, ok := rule.(map[string]interface{})
	if !ok {
		return false
	}

	reportedAt, ok := context["reported_at"].(time.Time)
	if !ok {
		return false
	}

	from, ok1 := c.convertToInt(ruleMap["from"])
	to, ok2 := c.convertToInt(ruleMap["to"])
	if !ok1 || !ok2 {
		return false
	}

	weekday := int(reportedAt.Weekday())
	return from <= weekday && weekday <= to
}

// evaluateWeekdayIn checks if the current weekday is in the given list.
func (c *ConditionEvaluator) evaluateWeekdayIn(rule interface{}, context map[string]interface{}) bool {
	days, ok := rule.([]interface{})
	if !ok {
		return false
	}

	reportedAt, ok := context["reported_at"].(time.Time)
	if !ok {
		return false
	}

	weekday := int(reportedAt.Weekday())
	c.logger.Info("Evaluating weekday_in condition", zap.Int("weekday", weekday), zap.Any("days", days))
	for _, d := range days {
		day, ok := c.convertToInt(d)
		if ok && day == weekday {
			return true
		}
	}

	return false
}

// evaluateTimeBetween evaluates time range conditions
func (c *ConditionEvaluator) evaluateTimeBetween(rule interface{}, context map[string]interface{}) bool {
	ruleMap, ok := rule.(map[string]interface{})
	if !ok {
		return false
	}

	reportedAt, ok := context["reported_at"].(time.Time)
	if !ok {
		return false
	}

	startStr, ok1 := ruleMap["start"].(string)
	endStr, ok2 := ruleMap["end"].(string)
	if !ok1 || !ok2 {
		return false
	}

	startTime, err1 := c.parseTimeString(startStr)
	endTime, err2 := c.parseTimeString(endStr)
	if err1 != nil || err2 != nil {
		return false
	}

	nowTime := reportedAt.Format("15:04")
	nowTimeParsed, err := c.parseTimeString(nowTime)
	if err != nil {
		return false
	}

	if startTime <= endTime {
		return startTime <= nowTimeParsed && nowTimeParsed <= endTime
	} else {
		return nowTimeParsed >= startTime || nowTimeParsed <= endTime
	}
}

// evaluateDeviceCondition evaluates device attribute conditions
func (c *ConditionEvaluator) evaluateDeviceCondition(rule interface{}, context map[string]interface{}) bool {
	ruleMap, ok := rule.(map[string]interface{})
	if !ok {
		return false
	}

	for attr, expectedValue := range ruleMap {
		contextValue, exists := context[attr]
		if !exists {
			deviceAttr := "device_" + attr
			contextValue, exists = context[deviceAttr]
			if !exists {
				return false
			}
		}

		if fmt.Sprintf("%v", contextValue) != fmt.Sprintf("%v", expectedValue) {
			return false
		}
	}

	return true
}

// Helper functions
func (c *ConditionEvaluator) convertToFloat64(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case string:
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f, true
		}
	}
	return 0, false
}

func (c *ConditionEvaluator) convertToInt(value interface{}) (int, bool) {
	switch v := value.(type) {
	case int:
		return v, true
	case float64:
		return int(v), true
	case string:
		if i, err := strconv.Atoi(v); err == nil {
			return i, true
		}
	}
	return 0, false
}

func (c *ConditionEvaluator) parseTimeString(timeStr string) (int, error) {
	parts := strings.Split(timeStr, ":")
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid time format")
	}

	hour, err1 := strconv.Atoi(parts[0])
	minute, err2 := strconv.Atoi(parts[1])
	if err1 != nil || err2 != nil {
		return 0, fmt.Errorf("invalid time components")
	}

	return hour*60 + minute, nil
}
