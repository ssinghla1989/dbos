// ScheduledCallbackWorkflow orchestrates durable scheduled callbacks.
//
// The workflow loads the scheduled job metadata, performs a durable wait until the
// configured fire time, invokes the external callback through deterministic step
// executions, and persists status transitions after each attempt. Keep every step
// invocation deterministic—derive timestamps, identifiers, and retry parameters
// from workflow inputs or database records rather than sampling new randomness or
// wall-clock values at replay time. Wire the DBOS timer primitives (e.g.,
// dbos.Sleep/dbos.Timer) inside durableSleep once the SDK integration layer is
// ready.
package workflows

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/google/uuid"
	"github.com/ssinghl/dbos/internal/workflows/steps"
	"go.uber.org/zap"
)

const (
	scheduledCallbackWorkflowName = "ScheduledCallbackWorkflow"

	statusPending    = "PENDING"
	statusInProgress = "IN_PROGRESS"
	statusSucceeded  = "SUCCEEDED"
	statusFailed     = "FAILED"
	statusCancelled  = "CANCELLED"
)

const (
	defaultWorkflowMaxAttempts   = 5
	defaultInitialBackoffSeconds = 60
	defaultMaxBackoffSeconds     = 900
)

type scheduledJob struct {
	ID             string
	Status         string
	ScheduledFor   time.Time
	NextAttemptAt  time.Time
	Attempts       int
	MaxAttempts    int
	CallbackURL    string
	Method         string
	Headers        map[string]string
	Body           []byte
	IdempotencyKey string
	ExpectedStatus []int
	TimeoutSeconds int
	RetryPolicy    steps.RetryPolicy
}

type scheduledCallbackResult struct{}

func scheduledCallbackWorkflowEntry(ctx dbos.DBOSContext, jobID string) (scheduledCallbackResult, error) {
	if err := ScheduledCallbackWorkflow(ctx, jobID); err != nil {
		return scheduledCallbackResult{}, err
	}
	return scheduledCallbackResult{}, nil
}

// ScheduledCallbackWorkflow drives a durable wait-and-callback pipeline for a scheduled job.
func ScheduledCallbackWorkflow(ctx context.Context, jobID string) error {
	logger := zap.L().With(
		zap.String("workflow", scheduledCallbackWorkflowName),
		zap.String("job_id", jobID),
	)

	logger.Info("workflow.start",
		zap.String("status", "loading"),
	)

	job, err := queryScheduledCallback(ctx, jobID)
	if err != nil {
		return fmt.Errorf("load scheduled callback job: %w", err)
	}
	if job == nil {
		return fmt.Errorf("scheduled callback job %s not found", jobID)
	}

	logger = logger.With(zap.Time("scheduled_for", job.ScheduledFor))

	switch job.Status {
	case statusCancelled, statusSucceeded:
		logger.Info("workflow.finish",
			zap.String("status", job.Status),
			zap.String("reason", "job already terminal"),
		)
		return nil
	}

	if err := waitUntilScheduled(ctx, logger, job); err != nil {
		return err
	}

	attempts := job.Attempts
	maxAttempts := job.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = defaultWorkflowMaxAttempts
	}

	for attempts < maxAttempts {
		attempts++

		if err := markJobInProgress(ctx, jobID, attempts); err != nil {
			return fmt.Errorf("mark job in-progress: %w", err)
		}

		logger.Info("workflow.attempt start",
			zap.Int("attempt", attempts),
		)

		result, callErr := invokeCallback(ctx, job, attempts)
		if callErr == nil {
			logger.Info("workflow.attempt result",
				zap.Int("attempt", attempts),
				zap.Int("status_code", result.StatusCode),
				zap.Int64("duration_ms", result.DurationMs),
			)

			if err := markJobSucceeded(ctx, jobID, attempts, result); err != nil {
				return fmt.Errorf("mark job succeeded: %w", err)
			}

			logger.Info("workflow.finish",
				zap.String("status", statusSucceeded),
			)
			return nil
		}

		logger.Warn("workflow.attempt result",
			zap.Int("attempt", attempts),
			zap.Error(callErr),
		)

		if isPermanentFailure(callErr) {
			if err := markJobFailed(ctx, jobID, attempts, callErr.Error()); err != nil {
				return fmt.Errorf("mark job failed: %w", err)
			}

			logger.Info("workflow.finish",
				zap.String("status", statusFailed),
				zap.String("failure", "permanent"),
			)
			return callErr
		}

		if attempts >= maxAttempts {
			if err := markJobFailed(ctx, jobID, attempts, callErr.Error()); err != nil {
				return fmt.Errorf("mark job failed after retries: %w", err)
			}

			logger.Error("workflow.finish",
				zap.String("status", statusFailed),
				zap.String("failure", "max attempts"),
			)
			return fmt.Errorf("max attempts reached for job %s", jobID)
		}

		backoffDelay := computeRetryDelay(job, attempts)

		if err := scheduleRetry(ctx, jobID, attempts, backoffDelay, callErr.Error()); err != nil {
			return fmt.Errorf("schedule retry: %w", err)
		}

		logger.Info("workflow.waiting",
			zap.Int("attempt", attempts),
			zap.Duration("duration", backoffDelay),
		)

		if err := durableSleep(ctx, backoffDelay); err != nil {
			return fmt.Errorf("durable sleep: %w", err)
		}
	}

	logger.Error("workflow.finish",
		zap.String("status", statusFailed),
		zap.String("failure", "exhausted attempts"),
	)

	return fmt.Errorf("exhausted attempts for scheduled callback %s", jobID)
}

func waitUntilScheduled(ctx context.Context, logger *zap.Logger, job *scheduledJob) error {
	waitUntil := job.ScheduledFor
	if !job.NextAttemptAt.IsZero() && job.NextAttemptAt.After(waitUntil) {
		waitUntil = job.NextAttemptAt
	}

	// TODO: Replace time.Now with DBOS-provided deterministic workflow clock once available.
	delay := time.Until(waitUntil)
	if delay <= 0 {
		return nil
	}

	logger.Info("workflow.waiting",
		zap.Duration("duration", delay),
	)

	if err := durableSleep(ctx, delay); err != nil {
		return fmt.Errorf("wait until scheduled time: %w", err)
	}

	return nil
}

func invokeCallback(ctx context.Context, job *scheduledJob, attempt int) (steps.CallExternalAPIStepResult, error) {
	params := steps.CallExternalAPIStepParams{
		Method:         job.Method,
		URL:            job.CallbackURL,
		Headers:        job.Headers,
		Body:           job.Body,
		IdempotencyKey: job.IdempotencyKey,
		TimeoutSeconds: job.TimeoutSeconds,
		ExpectedStatus: job.ExpectedStatus,
		RetryPolicy:    job.RetryPolicy,
	}

	// TODO: Execute CallExternalAPIStep through DBOS step runner (e.g., dbos.RunStep) once wired.
	return steps.CallExternalAPIStep(ctx, params)
}

func isPermanentFailure(err error) bool {
	if err == nil {
		return false
	}

	var validationErr *steps.ValidationError
	if errors.As(err, &validationErr) {
		return true
	}

	var permanentErr *steps.PermanentError
	return errors.As(err, &permanentErr)
}

func computeRetryDelay(job *scheduledJob, attempt int) time.Duration {
	policy := job.RetryPolicy

	initial := policy.InitialBackoffMs
	if initial <= 0 {
		initial = defaultInitialBackoffSeconds * 1000
	}

	max := policy.MaxBackoffMs
	if max <= 0 {
		max = defaultMaxBackoffSeconds * 1000
	}

	if attempt < 1 {
		attempt = 1
	}

	base := time.Duration(1<<uint(attempt-1)) * time.Duration(initial) * time.Millisecond
	maxDuration := time.Duration(max) * time.Millisecond
	if base > maxDuration {
		base = maxDuration
	}

	jitter := deterministicRetryJitter(attempt)
	jittered := time.Duration(float64(base) * jitter)
	if jittered > maxDuration {
		jittered = maxDuration
	}

	if jittered <= 0 {
		return time.Duration(defaultInitialBackoffSeconds) * time.Second
	}

	return jittered
}

func deterministicRetryJitter(attempt int) float64 {
	sequence := []float64{0.8, 1.0, 1.2, 0.9, 1.1}
	idx := (attempt - 1) % len(sequence)
	if idx < 0 {
		idx = 0
	}
	return sequence[idx]
}

func durableSleep(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}

	seconds := int(delay / time.Second)
	if delay%time.Second != 0 {
		seconds++
	}

	_, err := steps.SleepStep(ctx, steps.SleepParams{DurationSeconds: seconds})
	return err
}

func queryScheduledCallback(ctx context.Context, jobID string) (*scheduledJob, error) {
	res, err := steps.QueryByIDStep(ctx, steps.QueryByIDParams{Table: "scheduled_callbacks", ID: jobID})
	if err != nil {
		return nil, fmt.Errorf("query scheduled callback: %w", err)
	}
	if res.Row == nil {
		return nil, nil
	}
	job, err := mapScheduledJob(res.Row)
	if err != nil {
		return nil, err
	}
	return job, nil
}

func markJobInProgress(ctx context.Context, jobID string, attempt int) error {
	_, err := steps.UpdateStatusStep(ctx, steps.UpdateStatusParams{
		Table:             "scheduled_callbacks",
		ID:                jobID,
		NewStatus:         statusInProgress,
		ExpectedOldStatus: statusPending,
		AdditionalColumns: map[string]steps.ColumnUpdate{
			"attempts":        {Value: attempt},
			"last_attempt_at": {UseCurrentTimestamp: true},
			"next_attempt_at": {Value: nil},
		},
	})
	if err != nil {
		return fmt.Errorf("update job status to in-progress: %w", err)
	}
	return nil
}

func markJobSucceeded(ctx context.Context, jobID string, attempt int, result steps.CallExternalAPIStepResult) error {
	_, err := steps.UpdateStatusStep(ctx, steps.UpdateStatusParams{
		Table:     "scheduled_callbacks",
		ID:        jobID,
		NewStatus: statusSucceeded,
		AdditionalColumns: map[string]steps.ColumnUpdate{
			"attempts":        {Value: attempt},
			"completed_at":    {UseCurrentTimestamp: true},
			"last_attempt_at": {UseCurrentTimestamp: true},
			"next_attempt_at": {Value: nil},
			"error_reason":    {Value: nil},
		},
	})
	if err != nil {
		return fmt.Errorf("update job status to succeeded: %w", err)
	}
	_ = result
	return nil
}

func markJobFailed(ctx context.Context, jobID string, attempt int, reason string) error {
	_, err := steps.UpdateStatusStep(ctx, steps.UpdateStatusParams{
		Table:     "scheduled_callbacks",
		ID:        jobID,
		NewStatus: statusFailed,
		AdditionalColumns: map[string]steps.ColumnUpdate{
			"attempts":        {Value: attempt},
			"last_attempt_at": {UseCurrentTimestamp: true},
			"next_attempt_at": {Value: nil},
			"error_reason":    {Value: reason},
		},
	})
	if err != nil {
		return fmt.Errorf("update job status to failed: %w", err)
	}
	return nil
}

func scheduleRetry(ctx context.Context, jobID string, attempt int, delay time.Duration, reason string) error {
	nextAttemptAt := time.Now().UTC().Add(delay)
	_, err := steps.UpdateStatusStep(ctx, steps.UpdateStatusParams{
		Table:     "scheduled_callbacks",
		ID:        jobID,
		NewStatus: statusPending,
		AdditionalColumns: map[string]steps.ColumnUpdate{
			"attempts":        {Value: attempt},
			"next_attempt_at": {Value: nextAttemptAt},
			"last_attempt_at": {UseCurrentTimestamp: true},
			"error_reason":    {Value: reason},
		},
	})
	if err != nil {
		return fmt.Errorf("update job status for retry: %w", err)
	}
	return nil
}

func mapScheduledJob(row map[string]any) (*scheduledJob, error) {
	job := &scheduledJob{}

	var err error
	if job.ID, err = parseUUID(row["id"]); err != nil {
		return nil, fmt.Errorf("parse id: %w", err)
	}
	if job.Status, err = parseString(row["status"]); err != nil {
		return nil, fmt.Errorf("parse status: %w", err)
	}
	if job.ScheduledFor, err = parseTime(row["scheduled_for"], false); err != nil {
		return nil, fmt.Errorf("parse scheduled_for: %w", err)
	}
	if job.NextAttemptAt, err = parseTime(row["next_attempt_at"], true); err != nil {
		return nil, fmt.Errorf("parse next_attempt_at: %w", err)
	}
	if job.Attempts, err = parseInt(row["attempts"]); err != nil {
		return nil, fmt.Errorf("parse attempts: %w", err)
	}
	if job.MaxAttempts, err = parseInt(row["max_attempts"]); err != nil {
		return nil, fmt.Errorf("parse max_attempts: %w", err)
	}
	if job.CallbackURL, err = parseString(row["callback_url"]); err != nil {
		return nil, fmt.Errorf("parse callback_url: %w", err)
	}
	if job.Method, err = parseString(row["method"]); err != nil {
		return nil, fmt.Errorf("parse method: %w", err)
	}
	if job.Headers, err = parseHeaders(row["headers"]); err != nil {
		return nil, fmt.Errorf("parse headers: %w", err)
	}
	if job.Body, err = parseBytes(row["body"]); err != nil {
		return nil, fmt.Errorf("parse body: %w", err)
	}
	if job.IdempotencyKey, err = parseOptionalString(row["idempotency_key"]); err != nil {
		return nil, fmt.Errorf("parse idempotency_key: %w", err)
	}
	if job.RetryPolicy, err = parseRetryPolicy(row["retry_policy"]); err != nil {
		return nil, fmt.Errorf("parse retry_policy: %w", err)
	}
	// ExpectedStatus and TimeoutSeconds currently not persisted; leave zero values.
	return job, nil
}

func parseUUID(value any) (string, error) {
	switch v := value.(type) {
	case nil:
		return "", errors.New("uuid is nil")
	case string:
		if _, err := uuid.Parse(v); err != nil {
			return "", err
		}
		return v, nil
	case []byte:
		if len(v) == 16 {
			u, err := uuid.FromBytes(v)
			if err != nil {
				return "", err
			}
			return u.String(), nil
		}
		return string(v), nil
	case [16]byte:
		u, err := uuid.FromBytes(v[:])
		if err != nil {
			return "", err
		}
		return u.String(), nil
	default:
		return "", fmt.Errorf("unsupported uuid type %T", value)
	}
}

func parseString(value any) (string, error) {
	result, err := parseOptionalString(value)
	if err != nil {
		return "", err
	}
	if result == "" {
		return "", errors.New("string value is empty")
	}
	return result, nil
}

func parseOptionalString(value any) (string, error) {
	switch v := value.(type) {
	case nil:
		return "", nil
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	default:
		return "", fmt.Errorf("unsupported string type %T", value)
	}
}

func parseTime(value any, allowZero bool) (time.Time, error) {
	switch v := value.(type) {
	case nil:
		if allowZero {
			return time.Time{}, nil
		}
		return time.Time{}, errors.New("time value is nil")
	case time.Time:
		return v, nil
	case string:
		parsed, err := time.Parse(time.RFC3339Nano, v)
		if err != nil {
			return time.Time{}, err
		}
		return parsed, nil
	default:
		return time.Time{}, fmt.Errorf("unsupported time type %T", value)
	}
}

func parseInt(value any) (int, error) {
	switch v := value.(type) {
	case nil:
		return 0, nil
	case int:
		return v, nil
	case int32:
		return int(v), nil
	case int64:
		return int(v), nil
	case uint32:
		return int(v), nil
	case uint64:
		return int(v), nil
	case float32:
		return int(v), nil
	case float64:
		return int(v), nil
	case string:
		parsed, err := strconv.Atoi(v)
		if err != nil {
			return 0, err
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("unsupported int type %T", value)
	}
}

func parseHeaders(value any) (map[string]string, error) {
	if value == nil {
		return nil, nil
	}
	var raw []byte
	switch v := value.(type) {
	case []byte:
		raw = v
	case string:
		raw = []byte(v)
	case map[string]any:
		result := make(map[string]string, len(v))
		for key, val := range v {
			str, err := parseOptionalString(val)
			if err != nil {
				return nil, fmt.Errorf("normalize header %s: %w", key, err)
			}
			result[key] = str
		}
		return result, nil
	default:
		return nil, fmt.Errorf("unsupported headers type %T", value)
	}
	if len(raw) == 0 {
		return nil, nil
	}
	var decoded map[string]string
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return nil, err
	}
	return decoded, nil
}

func parseBytes(value any) ([]byte, error) {
	switch v := value.(type) {
	case nil:
		return nil, nil
	case []byte:
		return v, nil
	case string:
		return []byte(v), nil
	default:
		return nil, fmt.Errorf("unsupported bytes type %T", value)
	}
}

func parseRetryPolicy(value any) (steps.RetryPolicy, error) {
	if value == nil {
		return steps.RetryPolicy{}, nil
	}
	var raw []byte
	switch v := value.(type) {
	case []byte:
		raw = v
	case string:
		raw = []byte(v)
	case map[string]any:
		encoded, err := json.Marshal(v)
		if err != nil {
			return steps.RetryPolicy{}, err
		}
		raw = encoded
	default:
		return steps.RetryPolicy{}, fmt.Errorf("unsupported retry_policy type %T", value)
	}
	if len(raw) == 0 {
		return steps.RetryPolicy{}, nil
	}
	var policy steps.RetryPolicy
	if err := json.Unmarshal(raw, &policy); err != nil {
		return steps.RetryPolicy{}, err
	}
	return policy, nil
}
