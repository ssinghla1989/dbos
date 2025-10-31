package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/ssinghl/dbos/internal/config"
	"github.com/ssinghl/dbos/internal/db"
)

func init() {
	gin.SetMode(gin.TestMode)
}

func newTestHandlers() *Handlers {
	logger := zap.NewNop()
	cfg := &config.Config{}
	var pool pgxpool.Pool
	return NewHandlers(logger, cfg, nil, &pool)
}

func TestScheduleCallback_NoDBConfigured(t *testing.T) {
	h := NewHandlers(zap.NewNop(), &config.Config{}, nil, nil)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	req := httptest.NewRequest(http.MethodPost, "/v1/schedule", bytes.NewReader([]byte("{}")))
	req.Header.Set("Content-Type", "application/json")
	c.Request = req

	h.ScheduleCallback(c)

	require.Equal(t, http.StatusInternalServerError, w.Code)
	assert.JSONEq(t, `{"error":"database not configured"}`, w.Body.String())
}

func TestScheduleCallback_InvalidRequest(t *testing.T) {
	h := newTestHandlers()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	payload := map[string]any{
		"method": "post",
	}
	body, err := json.Marshal(payload)
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, "/v1/schedule", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	c.Request = req

	h.ScheduleCallback(c)

	require.Equal(t, http.StatusBadRequest, w.Code)
	assert.JSONEq(t, `{"error":"callback_url is required"}`, w.Body.String())
}

func TestScheduleCallback_SchedulesJobSuccessfully(t *testing.T) {
	h := newTestHandlers()
	fakeTx := &stubTx{}

	origBeginTx := beginTxFn
	beginTxFn = func(ctx context.Context, pool *pgxpool.Pool, opts pgx.TxOptions) (pgx.Tx, error) {
		require.NotNil(t, pool)
		return fakeTx, nil
	}
	t.Cleanup(func() { beginTxFn = origBeginTx })

	origQueryRow := queryRowFn
	queryRowFn = func(ctx context.Context, target db.QueryRowTarget, sql string, args ...any) (pgx.Row, error) {
		t.Fatalf("unexpected query row call: %s", sql)
		return nil, nil
	}
	t.Cleanup(func() { queryRowFn = origQueryRow })

	type execCall struct {
		sql  string
		args []any
	}
	var execCalls []execCall
	origExec := execFn
	execFn = func(ctx context.Context, target db.ExecTarget, sql string, args ...any) (pgconn.CommandTag, error) {
		require.Equal(t, fakeTx, target)
		copied := append([]any(nil), args...)
		execCalls = append(execCalls, execCall{sql: sql, args: copied})
		return pgconn.NewCommandTag("INSERT 0 1"), nil
	}
	t.Cleanup(func() { execFn = origExec })

	startCh := make(chan string, 1)
	origStart := startScheduledCallbackWorkflowFn
	startScheduledCallbackWorkflowFn = func(ctx context.Context, dbosCtx dbos.DBOSContext, job string) error {
		startCh <- job
		return nil
	}
	t.Cleanup(func() { startScheduledCallbackWorkflowFn = origStart })

	fixedNow := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	origNow := nowFn
	nowFn = func() time.Time { return fixedNow }
	t.Cleanup(func() { nowFn = origNow })

	payload := map[string]any{
		"callback_url":  "https://example.com/webhook",
		"method":        "patch",
		"headers":       map[string]string{"X-Test": "42"},
		"body":          map[string]any{"hello": "world"},
		"delay_seconds": 5,
		"max_attempts":  7,
	}
	body, err := json.Marshal(payload)
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, "/v1/schedule", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = req

	h.ScheduleCallback(c)

	require.Equal(t, http.StatusAccepted, w.Code)

	var resp scheduleCallbackResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	require.NotEmpty(t, resp.JobID)
	require.Equal(t, "/v1/status/"+resp.JobID, resp.StatusURL)

	select {
	case startedJob := <-startCh:
		require.Equal(t, resp.JobID, startedJob)
	case <-time.After(time.Second):
		t.Fatal("expected workflow start invocation")
	}

	require.Equal(t, 1, fakeTx.commitCount)
	require.Equal(t, 1, fakeTx.rollbackCount)
	require.Len(t, execCalls, 1)

	call := execCalls[0]
	require.Contains(t, call.sql, "INSERT INTO scheduled_callbacks")
	require.Len(t, call.args, 8)
	require.Equal(t, "https://example.com/webhook", call.args[2])
	require.Equal(t, http.MethodPatch, call.args[3])
	require.Equal(t, 7, call.args[7])

	headersBytes, ok := call.args[4].([]byte)
	require.True(t, ok)
	var recordedHeaders map[string]string
	require.NoError(t, json.Unmarshal(headersBytes, &recordedHeaders))
	require.Equal(t, "42", recordedHeaders["X-Test"])

	bodyBytes, ok := call.args[5].([]byte)
	require.True(t, ok)
	var recordedBody map[string]string
	require.NoError(t, json.Unmarshal(bodyBytes, &recordedBody))
	require.Equal(t, "world", recordedBody["hello"])

	scheduledFor, ok := call.args[6].(time.Time)
	require.True(t, ok)
	require.True(t, scheduledFor.Equal(fixedNow.Add(5*time.Second)))
}

func TestScheduleCallback_IdempotencyHitReturnsExistingJob(t *testing.T) {
	h := newTestHandlers()
	fakeTx := &stubTx{}

	origBeginTx := beginTxFn
	beginTxFn = func(ctx context.Context, pool *pgxpool.Pool, opts pgx.TxOptions) (pgx.Tx, error) {
		return fakeTx, nil
	}
	t.Cleanup(func() { beginTxFn = origBeginTx })

	origExec := execFn
	execFn = func(ctx context.Context, target db.ExecTarget, sql string, args ...any) (pgconn.CommandTag, error) {
		t.Fatalf("unexpected exec call")
		return pgconn.CommandTag{}, nil
	}
	t.Cleanup(func() { execFn = origExec })

	origStart := startScheduledCallbackWorkflowFn
	startScheduledCallbackWorkflowFn = func(ctx context.Context, dbosCtx dbos.DBOSContext, job string) error {
		t.Fatalf("workflow should not start for idempotent hit")
		return nil
	}
	t.Cleanup(func() { startScheduledCallbackWorkflowFn = origStart })

	existingID := uuid.New()
	origQueryRow := queryRowFn
	var queryCount int
	queryRowFn = func(ctx context.Context, target db.QueryRowTarget, sql string, args ...any) (pgx.Row, error) {
		queryCount++
		require.Equal(t, fakeTx, target)
		return &stubRow{scan: func(dest ...any) error {
			idPtr := dest[0].(*uuid.UUID)
			*idPtr = existingID
			return nil
		}}, nil
	}
	t.Cleanup(func() { queryRowFn = origQueryRow })

	payload := map[string]any{
		"callback_url":    "https://example.com/webhook",
		"idempotency_key": "duplicate-123",
	}
	body, err := json.Marshal(payload)
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, "/v1/schedule", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = req

	h.ScheduleCallback(c)

	require.Equal(t, http.StatusOK, w.Code)

	var resp scheduleCallbackResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	require.Equal(t, existingID.String(), resp.JobID)
	require.Equal(t, "/v1/status/"+existingID.String(), resp.StatusURL)
	require.Equal(t, 1, queryCount)
	require.Equal(t, 0, fakeTx.commitCount)
	require.GreaterOrEqual(t, fakeTx.rollbackCount, 1)
}

type stubTx struct {
	commitCount   int
	rollbackCount int
}

func (s *stubTx) Begin(ctx context.Context) (pgx.Tx, error) {
	panic("unexpected call to Begin")
}

func (s *stubTx) Commit(ctx context.Context) error {
	s.commitCount++
	return nil
}

func (s *stubTx) Rollback(ctx context.Context) error {
	s.rollbackCount++
	if s.commitCount > 0 {
		return pgx.ErrTxClosed
	}
	return nil
}

func (s *stubTx) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	panic("unexpected call to CopyFrom")
}

func (s *stubTx) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	panic("unexpected call to SendBatch")
}

func (s *stubTx) LargeObjects() pgx.LargeObjects {
	panic("unexpected call to LargeObjects")
}

func (s *stubTx) Prepare(ctx context.Context, name, sql string) (*pgconn.StatementDescription, error) {
	panic("unexpected call to Prepare")
}

func (s *stubTx) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	panic("unexpected call to Exec")
}

func (s *stubTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	panic("unexpected call to Query")
}

func (s *stubTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	panic("unexpected call to QueryRow")
}

func (s *stubTx) Conn() *pgx.Conn {
	return nil
}

type stubRow struct {
	scan func(dest ...any) error
}

func (r *stubRow) Scan(dest ...any) error {
	if r.scan == nil {
		panic("scan function not provided")
	}
	return r.scan(dest...)
}
