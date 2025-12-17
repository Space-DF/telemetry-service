package core

import (
	"errors"
	"fmt"
	"time"
)

const (
	BatchChannelBufferSize = 10
	DropTimeout            = 1 * time.Second
)

var (
	ErrLocationDroppedTimeout = fmt.Errorf("location dropped due to timeout")
	ErrDateRequired           = errors.New("date is required")
	ErrInvalidDateFormat      = errors.New("invalid date format, expected YYYY-MM-DD")
)

type ErrDroppedBatch struct {
	Size int
}

func (e *ErrDroppedBatch) Error() string {
	return fmt.Sprintf("batch dropped due to timeout, size: %d", e.Size)
}
