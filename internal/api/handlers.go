package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"github.com/ssinghl/dbos/internal/config"
	"github.com/ssinghl/dbos/internal/db"
	"github.com/ssinghl/dbos/internal/workflows"
)

// Handlers bundles HTTP handlers for the API surface.
// TODO: refactor to inject dedicated services once the scheduler is feature-complete.
type Handlers struct {
	logger  *zap.Logger
	cfg     *config.Config
	dbosCtx dbos.DBOSContext
	dbPool  *pgxpool.Pool
}

func NewHandlers(logger *zap.Logger, cfg *config.Config, dbosCtx dbos.DBOSContext, pool *pgxpool.Pool) *Handlers {
	return &Handlers{
		logger:  logger,
		cfg:     cfg,
		dbosCtx: dbosCtx,
		dbPool:  pool,
	}
}

func (h *Handlers) Health(c *gin.Context) {
	h.logger.Debug("health check invoked")
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

const (
	maxBodyBytes       = 64 * 1024
	maxDelayDuration   = 30 * 24 * time.Hour
	defaultHTTPMethod  = http.MethodPost
	defaultMaxAttempts = 5
)

var supportedHTTPMethods = map[string]struct{}{
	http.MethodGet:     {},
	http.MethodPost:    {},
	http.MethodPut:     {},
	http.MethodPatch:   {},
	http.MethodDelete:  {},
	http.MethodHead:    {},
	http.MethodOptions: {},
}

type scheduleCallbackRequest struct {
	CallbackURL    string            `json:"callback_url"`
	Method         string            `json:"method"`
	Headers        map[string]string `json:"headers"`
	Body           json.RawMessage   `json:"body"`
	DelaySeconds   *int              `json:"delay_seconds"`
	RunAt          string            `json:"run_at"`
	IdempotencyKey string            `json:"idempotency_key"`
	MaxAttempts    *int              `json:"max_attempts"`
}

type scheduleCallbackResponse struct {
	JobID     string `json:"job_id"`
	StatusURL string `json:"status_url"`
}

// ScheduleCallback handles scheduling of a callback workflow request.
func (h *Handlers) ScheduleCallback(c *gin.Context) {
	ctx := c.Request.Context()
	if h.dbPool == nil {
		h.logger.Error("database pool not configured for schedule callback handler")
		h.respondError(c, http.StatusInternalServerError, "database not configured")
		return
	}

	var req scheduleCallbackRequest
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		h.respondError(c, http.StatusBadRequest, fmt.Sprintf("invalid request payload: %v", err))
		return
	}

	if err := validateScheduleRequest(ctx, req); err != nil {
		h.respondError(c, http.StatusBadRequest, err.Error())
		return
	}

	method := normalizeHTTPMethod(req.Method)
	if _, ok := supportedHTTPMethods[method]; !ok {
		h.respondError(c, http.StatusBadRequest, fmt.Sprintf("unsupported HTTP method %q", method))
		return
	}

	scheduledFor, err := resolveScheduledFor(req)
	if err != nil {
		h.respondError(c, http.StatusBadRequest, err.Error())
		return
	}

	bodyBytes, err := normalizeBody(req.Body)
	if err != nil {
		h.respondError(c, http.StatusBadRequest, err.Error())
		return
	}
	if len(bodyBytes) > maxBodyBytes {
		h.respondError(c, http.StatusBadRequest, fmt.Sprintf("body size exceeds %d bytes", maxBodyBytes))
		return
	}

	headersJSON, err := marshalHeaders(req.Headers)
	if err != nil {
		h.respondError(c, http.StatusBadRequest, err.Error())
		return
	}

	idempotencyKey := strings.TrimSpace(req.IdempotencyKey)
	maxAttempts := defaultMaxAttempts
	if req.MaxAttempts != nil {
		if *req.MaxAttempts <= 0 {
			h.respondError(c, http.StatusBadRequest, "max_attempts must be greater than zero")
			return
		}
		maxAttempts = *req.MaxAttempts
	}

	tx, err := db.BeginTx(ctx, h.dbPool, pgx.TxOptions{})
	if err != nil {
		h.internalError(c, "begin transaction", err)
		return
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	if idempotencyKey != "" {
		var existingJobID uuid.UUID
		row, err := db.QueryRow(ctx, tx, `SELECT id FROM scheduled_callbacks WHERE idempotency_key = $1`, idempotencyKey)
		if err != nil {
			h.internalError(c, "lookup idempotency key", err)
			return
		}
		err = row.Scan(&existingJobID)
		switch {
		case err == nil:
			_ = tx.Rollback(ctx)
			h.logger.Info("schedule callback idempotent hit",
				zap.String("job_id", existingJobID.String()),
				zap.String("callback_url", req.CallbackURL),
				zap.String("idempotency_key", maskIdempotencyKey(idempotencyKey)),
			)
			c.JSON(http.StatusOK, scheduleCallbackResponse{
				JobID:     existingJobID.String(),
				StatusURL: buildStatusURL(existingJobID.String()),
			})
			return
		case errors.Is(err, pgx.ErrNoRows):
			// proceed with insert
		default:
			h.internalError(c, "lookup idempotency key", err)
			return
		}
	}

	jobID := uuid.New()
	var headersParam any
	if len(headersJSON) > 0 {
		headersParam = headersJSON
	}
	var bodyParam any
	if len(bodyBytes) > 0 {
		bodyParam = bodyBytes
	}
	var idempotencyParam any
	if idempotencyKey != "" {
		idempotencyParam = idempotencyKey
	}

	const insertSQL = `
	INSERT INTO scheduled_callbacks (
		id,
		idempotency_key,
		callback_url,
		method,
		headers,
		body,
		scheduled_for,
		max_attempts
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`
	if _, err := db.Exec(ctx, tx, insertSQL,
		jobID,
		idempotencyParam,
		req.CallbackURL,
		method,
		headersParam,
		bodyParam,
		scheduledFor,
		maxAttempts,
	); err != nil {
		h.internalError(c, "insert scheduled callback", err)
		return
	}

	if err := tx.Commit(ctx); err != nil {
		h.internalError(c, "commit scheduled callback", err)
		return
	}

	h.logger.Info("scheduled callback created",
		zap.String("job_id", jobID.String()),
		zap.String("callback_url", req.CallbackURL),
		zap.String("idempotency_key", maskIdempotencyKey(idempotencyKey)),
		zap.Time("scheduled_for", scheduledFor),
		zap.Int("max_attempts", maxAttempts),
	)

	go func(job string) {
		if err := workflows.StartScheduledCallbackWorkflow(context.Background(), h.dbosCtx, job); err != nil {
			h.logger.Error("failed to start scheduled callback workflow",
				zap.String("job_id", job),
				zap.Error(err),
			)
		}
	}(jobID.String())

	c.JSON(http.StatusAccepted, scheduleCallbackResponse{
		JobID:     jobID.String(),
		StatusURL: buildStatusURL(jobID.String()),
	})
}

func validateScheduleRequest(_ context.Context, req scheduleCallbackRequest) error {
	if strings.TrimSpace(req.CallbackURL) == "" {
		return errors.New("callback_url is required")
	}

	parsedURL, err := url.Parse(req.CallbackURL)
	if err != nil {
		return fmt.Errorf("callback_url is invalid: %w", err)
	}
	if !parsedURL.IsAbs() {
		return errors.New("callback_url must be absolute")
	}
	if parsedURL.Scheme != "https" {
		return errors.New("callback_url must use https")
	}

	if req.DelaySeconds != nil && req.RunAt != "" {
		return errors.New("only one of delay_seconds or run_at may be provided")
	}
	if req.DelaySeconds != nil {
		if *req.DelaySeconds < 0 {
			return errors.New("delay_seconds cannot be negative")
		}
		if time.Duration(*req.DelaySeconds)*time.Second > maxDelayDuration {
			return fmt.Errorf("delay_seconds exceeds maximum of %d seconds", int(maxDelayDuration/time.Second))
		}
	}

	if strings.TrimSpace(req.RunAt) != "" {
		if _, err := time.Parse(time.RFC3339, req.RunAt); err != nil {
			return fmt.Errorf("run_at must be RFC3339: %w", err)
		}
	}

	return nil
}

func resolveScheduledFor(req scheduleCallbackRequest) (time.Time, error) {
	now := time.Now().UTC()
	if req.DelaySeconds != nil {
		dl := time.Duration(*req.DelaySeconds) * time.Second
		return now.Add(dl), nil
	}
	if strings.TrimSpace(req.RunAt) != "" {
		runAt, err := time.Parse(time.RFC3339, req.RunAt)
		if err != nil {
			return time.Time{}, err
		}
		if runAt.Sub(now) > maxDelayDuration {
			return time.Time{}, fmt.Errorf("run_at exceeds maximum scheduling window of %s", maxDelayDuration)
		}
		return runAt, nil
	}
	return now, nil
}

func normalizeBody(raw json.RawMessage) ([]byte, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	// Attempt to treat body as a string, otherwise preserve raw JSON/object payload.
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		return []byte(text), nil
	}
	return raw, nil
}

func marshalHeaders(headers map[string]string) ([]byte, error) {
	if len(headers) == 0 {
		return nil, nil
	}
	encoded, err := json.Marshal(headers)
	if err != nil {
		return nil, fmt.Errorf("headers must be valid JSON object: %w", err)
	}
	return encoded, nil
}

func buildStatusURL(jobID string) string {
	return fmt.Sprintf("/v1/status/%s", jobID)
}

func (h *Handlers) internalError(c *gin.Context, message string, err error) {
	h.logger.Error(message, zap.Error(err))
	h.respondError(c, http.StatusInternalServerError, "internal server error")
}

func (h *Handlers) respondError(c *gin.Context, status int, msg string) {
	c.JSON(status, gin.H{"error": msg})
}

func normalizeHTTPMethod(method string) string {
	m := strings.TrimSpace(method)
	if m == "" {
		return defaultHTTPMethod
	}
	return strings.ToUpper(m)
}

func maskIdempotencyKey(value string) string {
	if value == "" {
		return ""
	}
	trimmed := strings.TrimSpace(value)
	if len(trimmed) <= 4 {
		return "***"
	}
	return trimmed[:4] + "***"
}
