// Package steps contains deterministic workflow steps. Avoid introducing non-deterministic
// behavior (randomness, wall-clock timestamps, etc.) so that DBOS workflows can replay
// executions reliably.
package steps

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
)

// CallExternalAPIStepParams defines the request parameters for CallExternalAPIStep.
type CallExternalAPIStepParams struct {
	Method         string            `json:"method"`
	URL            string            `json:"url"`
	Headers        map[string]string `json:"headers,omitempty"`
	Body           []byte            `json:"body,omitempty"`
	IdempotencyKey string            `json:"idempotency_key,omitempty"`
	TimeoutSeconds int               `json:"timeout_seconds,omitempty"`
	ExpectedStatus []int             `json:"expected_status,omitempty"`
	RetryPolicy    RetryPolicy       `json:"retry_policy,omitempty"`
}

// RetryPolicy configures retry behavior for CallExternalAPIStep.
type RetryPolicy struct {
	MaxAttempts         int    `json:"max_attempts,omitempty"`
	InitialBackoffMs    int    `json:"initial_backoff_ms,omitempty"`
	MaxBackoffMs        int    `json:"max_backoff_ms,omitempty"`
	BackoffType         string `json:"backoff_type,omitempty"`
	RetryOnStatusCodes  []int  `json:"retry_on_status_codes,omitempty"`
	RetryOnNetworkError bool   `json:"retry_on_network_error,omitempty"`
}

// CallExternalAPIStepResult captures the essential response data from CallExternalAPIStep.
type CallExternalAPIStepResult struct {
	StatusCode int    `json:"status_code"`
	Body       []byte `json:"body"`
	DurationMs int64  `json:"duration_ms"`
}

const (
	defaultTimeoutSeconds   = 10
	defaultMaxAttempts      = 3
	defaultInitialBackoffMs = 200
	defaultMaxBackoffMs     = 5000
)

// CallExternalAPIStep performs an HTTP request with retry logic suitable for DBOS workflows.
// TODO: add persistence of idempotency records to guarantee deduplication of mutating calls.
// TODO: add advanced retry policy tuning (circuit breakers, per-status windows, etc.).
// TODO: add observability hooks (metrics/tracing) when infrastructure is ready.
func CallExternalAPIStep(ctx context.Context, params CallExternalAPIStepParams) (CallExternalAPIStepResult, error) {
	logger := zap.L()

	normalizedMethod := normalizeMethod(params.Method)
	params.Method = normalizedMethod

	if err := validateParams(params); err != nil {
		return CallExternalAPIStepResult{}, err
	}

	retryPolicy := applyRetryPolicyDefaults(params.RetryPolicy)
	timeout := time.Duration(defaultTimeoutSeconds) * time.Second
	if params.TimeoutSeconds > 0 {
		timeout = time.Duration(params.TimeoutSeconds) * time.Second
	}

	client := &http.Client{Timeout: timeout}

	expectedStatuses := params.ExpectedStatus

	var lastErr error
	var lastStatus int
	var lastRetryAfter time.Duration

	for attempt := 1; attempt <= retryPolicy.MaxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return CallExternalAPIStepResult{}, &PermanentError{Msg: "context cancelled", Err: ctx.Err()}
		default:
		}

		reqBody := bytes.NewReader(params.Body)
		req, err := http.NewRequestWithContext(ctx, params.Method, params.URL, reqBody)
		if err != nil {
			return CallExternalAPIStepResult{}, &ValidationError{Msg: fmt.Sprintf("failed to build request: %v", err)}
		}

		for k, v := range params.Headers {
			req.Header.Set(k, v)
		}

		if params.IdempotencyKey != "" {
			req.Header.Set("Idempotency-Key", params.IdempotencyKey)
		}

		attemptStart := time.Now()
		logger.Info("external_api.start",
			zap.String("event", "external_api.start"),
			zap.String("method", params.Method),
			zap.String("url", params.URL),
			zap.String("idempotency_key", params.IdempotencyKey),
			zap.Int("attempt", attempt),
			zap.Int("body_bytes", len(params.Body)),
		)

		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
			lastStatus = 0
			lastRetryAfter = 0

			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				logger.Error("external_api.error",
					zap.String("event", "external_api.error"),
					zap.String("error_type", "context"),
					zap.Int("attempt", attempt),
					zap.Error(err),
				)
				return CallExternalAPIStepResult{}, &PermanentError{Msg: "context error", Err: err}
			}

			if shouldRetryNetworkError(err, retryPolicy) && attempt < retryPolicy.MaxAttempts {
				logger.Warn("external_api.error",
					zap.String("event", "external_api.error"),
					zap.String("error_type", "network"),
					zap.Int("attempt", attempt),
					zap.Error(err),
				)

				delay := computeBackoff(attempt, retryPolicy, 0)
				if delay > 0 {
					if err := sleepWithContext(ctx, delay); err != nil {
						return CallExternalAPIStepResult{}, &PermanentError{Msg: "context cancelled during backoff", Err: err}
					}
				}
				continue
			}

			logger.Error("external_api.error",
				zap.String("event", "external_api.error"),
				zap.String("error_type", "network"),
				zap.Int("attempt", attempt),
				zap.Error(err),
			)

			return CallExternalAPIStepResult{}, &TransientError{Err: err}
		}

		body, readErr := io.ReadAll(resp.Body)
		closeErr := resp.Body.Close()
		if readErr != nil {
			lastErr = readErr
			logger.Error("external_api.error",
				zap.String("event", "external_api.error"),
				zap.String("error_type", "read"),
				zap.Int("attempt", attempt),
				zap.Error(readErr),
			)
			return CallExternalAPIStepResult{}, &TransientError{Err: readErr}
		}
		if closeErr != nil {
			logger.Warn("external_api.error",
				zap.String("event", "external_api.error"),
				zap.String("error_type", "close"),
				zap.Int("attempt", attempt),
				zap.Error(closeErr),
			)
		}

		duration := time.Since(attemptStart)
		lastStatus = resp.StatusCode
		retryAfter, hasRetryAfter := extractRetryAfter(resp)
		if hasRetryAfter {
			lastRetryAfter = retryAfter
		} else {
			lastRetryAfter = 0
		}

		if isExpectedStatus(resp.StatusCode, expectedStatuses) {
			logger.Info("external_api.finish",
				zap.String("event", "external_api.finish"),
				zap.Int("status_code", resp.StatusCode),
				zap.Int64("duration_ms", duration.Milliseconds()),
				zap.Int("attempt", attempt),
				zap.Int("response_bytes", len(body)),
			)

			return CallExternalAPIStepResult{
				StatusCode: resp.StatusCode,
				Body:       body,
				DurationMs: duration.Milliseconds(),
			}, nil
		}

		if shouldRetryStatus(resp.StatusCode, retryPolicy) && attempt < retryPolicy.MaxAttempts {
			logger.Warn("external_api.error",
				zap.String("event", "external_api.error"),
				zap.String("error_type", "status"),
				zap.Int("status_code", resp.StatusCode),
				zap.Int("attempt", attempt),
			)

			delay := computeBackoff(attempt, retryPolicy, lastRetryAfter)
			if delay > 0 {
				if err := sleepWithContext(ctx, delay); err != nil {
					return CallExternalAPIStepResult{}, &PermanentError{Msg: "context cancelled during backoff", Err: err}
				}
			}
			continue
		}

		errorType := "status"
		if resp.StatusCode >= 400 && resp.StatusCode < 500 {
			if shouldRetryStatus(resp.StatusCode, retryPolicy) {
				logger.Warn("external_api.error",
					zap.String("event", "external_api.error"),
					zap.String("error_type", errorType),
					zap.Int("status_code", resp.StatusCode),
					zap.Int("attempt", attempt),
				)
				lastErr = fmt.Errorf("retryable status %d", resp.StatusCode)
				break
			}

			logger.Error("external_api.error",
				zap.String("event", "external_api.error"),
				zap.String("error_type", errorType),
				zap.Int("status_code", resp.StatusCode),
				zap.Int("attempt", attempt),
			)
			return CallExternalAPIStepResult{}, &PermanentError{StatusCode: resp.StatusCode, Msg: fmt.Sprintf("non-retryable status: %d", resp.StatusCode)}
		}

		logger.Error("external_api.error",
			zap.String("event", "external_api.error"),
			zap.String("error_type", errorType),
			zap.Int("status_code", resp.StatusCode),
			zap.Int("attempt", attempt),
		)

		lastErr = fmt.Errorf("unexpected status %d", resp.StatusCode)
		break
	}

	if lastErr == nil && lastStatus != 0 {
		lastErr = fmt.Errorf("request failed with status %d", lastStatus)
	}

	if shouldRetryStatus(lastStatus, retryPolicy) || shouldRetryNetworkError(lastErr, retryPolicy) {
		return CallExternalAPIStepResult{}, &TransientError{Err: lastErr, StatusCode: lastStatus, RetryAfter: lastRetryAfter}
	}

	return CallExternalAPIStepResult{}, &PermanentError{StatusCode: lastStatus, Msg: "request failed", Err: lastErr}
}

func normalizeMethod(method string) string {
	if method == "" {
		return http.MethodGet
	}
	return strings.ToUpper(strings.TrimSpace(method))
}

func validateParams(params CallExternalAPIStepParams) error {
	if params.URL == "" {
		return &ValidationError{Msg: "url is required"}
	}
	if _, err := url.ParseRequestURI(params.URL); err != nil {
		return &ValidationError{Msg: fmt.Sprintf("invalid url: %v", err)}
	}

	switch params.Method {
	case http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete:
	default:
		return &ValidationError{Msg: fmt.Sprintf("unsupported method: %s", params.Method)}
	}

	if requiresIdempotencyKey(params.Method) && params.IdempotencyKey == "" {
		return &ValidationError{Msg: "idempotency_key is required for mutating methods"}
	}

	return nil
}

func requiresIdempotencyKey(method string) bool {
	switch method {
	case http.MethodPost, http.MethodPut, http.MethodDelete:
		return true
	default:
		return false
	}
}

func applyRetryPolicyDefaults(policy RetryPolicy) RetryPolicy {
	assumeNetworkDefault := !policy.RetryOnNetworkError && policy.MaxAttempts == 0 && policy.InitialBackoffMs == 0 && policy.MaxBackoffMs == 0 && policy.BackoffType == "" && policy.RetryOnStatusCodes == nil

	if policy.MaxAttempts <= 0 {
		policy.MaxAttempts = defaultMaxAttempts
	}
	if policy.InitialBackoffMs <= 0 {
		policy.InitialBackoffMs = defaultInitialBackoffMs
	}
	if policy.MaxBackoffMs <= 0 {
		policy.MaxBackoffMs = defaultMaxBackoffMs
	}
	if policy.BackoffType == "" {
		policy.BackoffType = "exponential"
	}
	if policy.RetryOnStatusCodes == nil {
		policy.RetryOnStatusCodes = []int{http.StatusTooManyRequests}
	}
	if assumeNetworkDefault {
		// Callers can explicitly disable network retries by setting RetryOnNetworkError to false
		// while also providing at least one other policy field.
		policy.RetryOnNetworkError = true
	}
	return policy
}

func shouldRetryStatus(status int, policy RetryPolicy) bool {
	if status == 0 {
		return false
	}
	for _, code := range policy.RetryOnStatusCodes {
		if status == code {
			return true
		}
	}
	if status == http.StatusTooManyRequests {
		return true
	}
	if status >= 500 && status <= 599 {
		return true
	}
	return false
}

func shouldRetryNetworkError(err error, policy RetryPolicy) bool {
	if err == nil {
		return false
	}
	if !policy.RetryOnNetworkError {
		return false
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	var urlErr *url.Error
	return errors.As(err, &urlErr)
}

func computeBackoff(attempt int, policy RetryPolicy, retryAfter time.Duration) time.Duration {
	if retryAfter > 0 {
		return retryAfter
	}

	initial := time.Duration(policy.InitialBackoffMs) * time.Millisecond
	if initial <= 0 {
		initial = defaultInitialBackoffMs * time.Millisecond
	}

	max := time.Duration(policy.MaxBackoffMs) * time.Millisecond
	if max <= 0 {
		max = defaultMaxBackoffMs * time.Millisecond
	}

	if attempt < 1 {
		attempt = 1
	}

	var base time.Duration
	switch strings.ToLower(policy.BackoffType) {
	case "linear":
		base = initial * time.Duration(attempt)
	case "fixed":
		base = initial
	default:
		shift := attempt - 1
		if shift < 0 {
			shift = 0
		}
		if shift > 30 {
			shift = 30
		}
		multiplier := 1 << shift
		base = time.Duration(multiplier) * initial
	}

	if base > max {
		base = max
	}

	jitterFactor := deterministicJitter(attempt)
	jittered := time.Duration(float64(base) * jitterFactor)
	if jittered > max {
		jittered = max
	}
	if jittered < 0 {
		return 0
	}
	return jittered
}

func deterministicJitter(attempt int) float64 {
	factors := []float64{0.8, 1.15, 1.0, 1.2, 0.9}
	idx := (attempt - 1) % len(factors)
	if idx < 0 {
		idx = 0
	}
	return factors[idx]
}

func extractRetryAfter(resp *http.Response) (time.Duration, bool) {
	if resp == nil {
		return 0, false
	}
	value := strings.TrimSpace(resp.Header.Get("Retry-After"))
	if value == "" {
		return 0, false
	}

	if seconds, err := strconv.Atoi(value); err == nil {
		if seconds < 0 {
			return 0, false
		}
		return time.Duration(seconds) * time.Second, true
	}

	if date, err := http.ParseTime(value); err == nil {
		d := time.Until(date)
		if d <= 0 {
			return 0, false
		}
		return d, true
	}

	return 0, false
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func isExpectedStatus(status int, expected []int) bool {
	if len(expected) == 0 {
		return status >= 200 && status < 300
	}

	for _, code := range expected {
		if status == code {
			return true
		}
	}

	return false
}
