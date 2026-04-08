package timescaledb

import (
	"errors"
)

var (
	ErrLocationDroppedTimeout = errors.New("location dropped due to timeout")
	ErrDateRequired           = errors.New("date is required")
	ErrInvalidDateFormat      = errors.New("invalid date format, expected YYYY-MM-DD")
)
