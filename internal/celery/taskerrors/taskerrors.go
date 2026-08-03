package taskerrors

import (
	"errors"
	"fmt"
)

type permanentError struct {
	err error
}

func (e *permanentError) Error() string {
	return e.err.Error()
}

func (e *permanentError) Unwrap() error {
	return e.err
}

func NewPermanentf(format string, args ...interface{}) error {
	return &permanentError{err: fmt.Errorf(format, args...)}
}

func IsPermanent(err error) bool {
	var target *permanentError
	return errors.As(err, &target)
}
