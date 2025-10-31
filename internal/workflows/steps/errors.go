package steps

import "time"

// ValidationError indicates invalid step parameters that should not be retried.
// TODO: Align with domain-specific error enrichment once shared error helpers exist.
type ValidationError struct {
	Msg string
}

// Error implements the error interface.
func (e ValidationError) Error() string {
	return e.Msg
}

// TransientError represents retryable failures.
type TransientError struct {
	Err        error
	RetryAfter time.Duration
	StatusCode int
}

// Error implements the error interface.
func (e TransientError) Error() string {
	if e.Err == nil {
		return "transient error"
	}
	return e.Err.Error()
}

// Unwrap exposes the underlying cause.
func (e TransientError) Unwrap() error {
	return e.Err
}

// PermanentError represents non-retryable failures.
type PermanentError struct {
	Msg        string
	Err        error
	StatusCode int
}

// Error implements the error interface.
func (e PermanentError) Error() string {
	switch {
	case e.Msg != "":
		return e.Msg
	case e.Err != nil:
		return e.Err.Error()
	default:
		return "permanent error"
	}
}

// Unwrap exposes the underlying cause.
func (e PermanentError) Unwrap() error {
	return e.Err
}
