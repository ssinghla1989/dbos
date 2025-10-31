// Package steps provides deterministic workflow building blocks.
package steps

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"go.uber.org/zap"
)

// SleepParams configures SleepStep.
type SleepParams struct {
	DurationSeconds int
}

// SleepResult reports whether the step completed the sleep interval.
type SleepResult struct {
	Slept bool
}

// SleepStep pauses execution for a deterministic amount of time.
func SleepStep(ctx context.Context, params SleepParams) (SleepResult, error) {
	if params.DurationSeconds < 0 {
		return SleepResult{}, &ValidationError{Msg: "duration_seconds must be >= 0"}
	}

	logger := zap.L()
	logger.Info("sleep_step.start",
		zap.String("step", "SleepStep"),
		zap.Int("duration_seconds", params.DurationSeconds),
	)

	duration := time.Duration(params.DurationSeconds) * time.Second
	if duration <= 0 {
		logger.Info("sleep_step.finish",
			zap.String("step", "SleepStep"),
			zap.Int("duration_seconds", params.DurationSeconds),
			zap.Bool("slept", true),
		)
		return SleepResult{Slept: true}, nil
	}

	if dbosCtx, ok := ctx.(dbos.DBOSContext); ok {
		actual, err := dbos.Sleep(dbosCtx, duration)
		if err != nil {
			return SleepResult{}, classifySleepError(logger, params.DurationSeconds, err)
		}
		logger.Info("sleep_step.finish",
			zap.String("step", "SleepStep"),
			zap.Int("duration_seconds", params.DurationSeconds),
			zap.Bool("slept", true),
			zap.Duration("actual_duration", actual),
		)
		return SleepResult{Slept: true}, nil
	}

	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		err := fmt.Errorf("sleep interrupted: %w", ctx.Err())
		logger.Error("sleep_step.error",
			zap.String("step", "SleepStep"),
			zap.Int("duration_seconds", params.DurationSeconds),
			zap.Error(err),
		)
		return SleepResult{}, &TransientError{Err: err}
	case <-timer.C:
		logger.Info("sleep_step.finish",
			zap.String("step", "SleepStep"),
			zap.Int("duration_seconds", params.DurationSeconds),
			zap.Bool("slept", true),
		)
		return SleepResult{Slept: true}, nil
	}
}

func classifySleepError(logger *zap.Logger, seconds int, err error) error {
	logger.Error("sleep_step.error",
		zap.String("step", "SleepStep"),
		zap.Int("duration_seconds", seconds),
		zap.Error(err),
	)

	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return &TransientError{Err: err}
	default:
		return &PermanentError{Err: err}
	}
}
