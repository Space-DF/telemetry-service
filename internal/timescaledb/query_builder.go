package timescaledb

import (
	"fmt"
	"strings"
)

// QueryBuilder helps build SQL queries dynamically with proper parameter handling
type QueryBuilder struct {
	whereParts []string
	args       []interface{}
	argIndex   int
}

// NewQueryBuilder creates a new QueryBuilder with the starting argument index
func NewQueryBuilder(startIndex int) *QueryBuilder {
	return &QueryBuilder{
		argIndex: startIndex,
		args:     make([]interface{}, 0),
	}
}

// AddCondition adds a WHERE clause condition with a parameter
func (qb *QueryBuilder) AddCondition(condition string, arg interface{}) {
	qb.whereParts = append(qb.whereParts, condition)
	qb.args = append(qb.args, arg)
	qb.argIndex++
}

// AddConditionMulti adds a WHERE clause condition with multiple parameters
func (qb *QueryBuilder) AddConditionMulti(condition string, args ...interface{}) {
	qb.whereParts = append(qb.whereParts, condition)
	qb.args = append(qb.args, args...)
	qb.argIndex += len(args)
}

// AddConditionIf adds a WHERE clause condition only if the value is non-empty
func (qb *QueryBuilder) AddConditionIf(condition string, arg interface{}) {
	switch v := arg.(type) {
	case string:
		if v != "" {
			qb.AddCondition(condition, arg)
		}
	case *string:
		if v != nil && *v != "" {
			qb.AddCondition(condition, arg)
		}
	case []bool:
		if len(v) > 0 {
			qb.AddCondition(condition, arg)
		}
	case []string:
		if len(v) > 0 {
			qb.AddCondition(condition, arg)
		}
	default:
		if arg != nil {
			qb.AddCondition(condition, arg)
		}
	}
}

// AddRawCondition adds a WHERE clause condition without a parameter
func (qb *QueryBuilder) AddRawCondition(condition string) {
	qb.whereParts = append(qb.whereParts, condition)
}

// AddILikeSearch adds an ILIKE search condition across multiple columns
func (qb *QueryBuilder) AddILikeSearch(searchPattern string, columns ...string) {
	if searchPattern == "" || len(columns) == 0 {
		return
	}

	var conditions []string
	for _, col := range columns {
		conditions = append(conditions, fmt.Sprintf("%s ILIKE $%d", col, qb.argIndex))
		qb.args = append(qb.args, searchPattern)
		qb.argIndex++
	}

	if len(conditions) > 0 {
		qb.whereParts = append(qb.whereParts, "("+strings.Join(conditions, " OR ")+")")
	}
}

// BuildWhere builds the WHERE clause string
func (qb *QueryBuilder) BuildWhere() string {
	if len(qb.whereParts) == 0 {
		return ""
	}
	return " WHERE " + strings.Join(qb.whereParts, " AND ")
}

// Args returns the accumulated arguments
func (qb *QueryBuilder) Args() []interface{} {
	return qb.args
}

// NextIndex returns the next argument index (for manual parameter building)
func (qb *QueryBuilder) NextIndex() int {
	idx := qb.argIndex
	qb.argIndex++
	return idx
}

// BuildLimitOffset builds LIMIT and OFFSET clauses
func (qb *QueryBuilder) BuildLimitOffset(limit, offset int) string {
	return fmt.Sprintf(" LIMIT $%d OFFSET $%d", qb.NextIndex(), qb.NextIndex())
}

// AddLimitOffset adds limit and offset to the query builder
func (qb *QueryBuilder) AddLimitOffset(limit, offset int) {
	qb.args = append(qb.args, limit, offset)
	qb.argIndex += 2
}
