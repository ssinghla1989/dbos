// Package steps contains deterministic database-related workflow steps used by
// ScheduledCallbackWorkflow. These implementations favor determinism and simple
// sentinel errors so the workflow engine can classify retries easily.
package steps

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

var (
	defaultDBTimeoutSeconds = 10
	sharedPoolMu            sync.RWMutex
	sharedPool              *pgxpool.Pool
)

// ConfigureDBPool makes a pgx pool available to database-backed steps. Call this during
// application startup after establishing your primary database connection.
func ConfigureDBPool(pool *pgxpool.Pool) {
	sharedPoolMu.Lock()
	defer sharedPoolMu.Unlock()
	sharedPool = pool
}

// QueryByIDParams configures QueryByIDStep.
type QueryByIDParams struct {
	Table          string
	ID             string
	Columns        []string
	TimeoutSeconds int
}

// QueryByIDResult returns the selected row as map.
type QueryByIDResult struct {
	Row map[string]any
}

// QueryByIDStep looks up a single row by primary key. Missing rows are not
// treated as errors (Row will be nil).
func QueryByIDStep(ctx context.Context, params QueryByIDParams) (QueryByIDResult, error) {
	logger := zap.L()
	logger.Info("db_step.start",
		zap.String("step", "QueryByIDStep"),
		zap.String("table", params.Table),
		zap.String("id", params.ID),
	)

	tableName, err := normalizeIdentifier(params.Table)
	if err != nil {
		return QueryByIDResult{}, &ValidationError{Msg: fmt.Sprintf("invalid table: %v", err)}
	}
	if strings.TrimSpace(params.ID) == "" {
		return QueryByIDResult{}, &ValidationError{Msg: "id is required"}
	}

	columnList, err := buildColumnList(params.Columns)
	if err != nil {
		return QueryByIDResult{}, &ValidationError{Msg: err.Error()}
	}

	timeout := time.Duration(defaultDBTimeoutSeconds) * time.Second
	if params.TimeoutSeconds > 0 {
		timeout = time.Duration(params.TimeoutSeconds) * time.Second
	}

	stepCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	pool, err := getSharedPool(stepCtx)
	if err != nil {
		return QueryByIDResult{}, classifyPoolError(err)
	}

	query := fmt.Sprintf("SELECT %s FROM %s WHERE id=$1", columnList, tableName)
	rows, err := pool.Query(stepCtx, query, params.ID)
	if err != nil {
		return QueryByIDResult{}, classifyDBError("query row", err)
	}
	defer rows.Close()

	rowMap, err := pgx.CollectOneRow(rows, pgx.RowToMap)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			logger.Info("db_step.finish",
				zap.String("step", "QueryByIDStep"),
				zap.String("table", params.Table),
				zap.String("id", params.ID),
				zap.Bool("found", false),
			)
			return QueryByIDResult{Row: nil}, nil
		}
		return QueryByIDResult{}, classifyDBError("scan row", err)
	}

	logger.Info("db_step.finish",
		zap.String("step", "QueryByIDStep"),
		zap.String("table", params.Table),
		zap.String("id", params.ID),
		zap.Bool("found", true),
	)

	return QueryByIDResult{Row: rowMap}, nil
}

// UpdateStatusParams configures UpdateStatusStep.
type UpdateStatusParams struct {
	Table             string
	ID                string
	StatusColumn      string
	NewStatus         string
	ExpectedOldStatus string
	TimeoutSeconds    int
	AdditionalColumns map[string]ColumnUpdate
}

// UpdateStatusResult reports whether the row was updated.
type UpdateStatusResult struct {
	Updated   bool
	OldStatus string
}

// ColumnUpdate describes how to set an additional column during UpdateStatusStep.
type ColumnUpdate struct {
	Value               any
	UseCurrentTimestamp bool
}

// UpdateStatusStep performs an idempotent status transition. When ExpectOldStatus
// is provided, the update is conditional. Missing rows result in Updated=false.
func UpdateStatusStep(ctx context.Context, params UpdateStatusParams) (UpdateStatusResult, error) {
	logger := zap.L()
	logger.Info("db_step.start",
		zap.String("step", "UpdateStatusStep"),
		zap.String("table", params.Table),
		zap.String("id", params.ID),
		zap.String("new_status", params.NewStatus),
	)

	tableName, err := normalizeIdentifier(params.Table)
	if err != nil {
		return UpdateStatusResult{}, &ValidationError{Msg: fmt.Sprintf("invalid table: %v", err)}
	}
	if strings.TrimSpace(params.ID) == "" {
		return UpdateStatusResult{}, &ValidationError{Msg: "id is required"}
	}
	if strings.TrimSpace(params.NewStatus) == "" {
		return UpdateStatusResult{}, &ValidationError{Msg: "new_status is required"}
	}

	statusColumn := params.StatusColumn
	if strings.TrimSpace(statusColumn) == "" {
		statusColumn = "status"
	}
	statusName, err := normalizeIdentifier(statusColumn)
	if err != nil {
		return UpdateStatusResult{}, &ValidationError{Msg: fmt.Sprintf("invalid status column: %v", err)}
	}

	timeout := time.Duration(defaultDBTimeoutSeconds) * time.Second
	if params.TimeoutSeconds > 0 {
		timeout = time.Duration(params.TimeoutSeconds) * time.Second
	}

	stepCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	pool, err := getSharedPool(stepCtx)
	if err != nil {
		return UpdateStatusResult{}, classifyPoolError(err)
	}

	result := UpdateStatusResult{OldStatus: params.ExpectedOldStatus}
	setParts, args, err := buildUpdateAssignments(statusName, params.NewStatus, params.AdditionalColumns)
	if err != nil {
		return UpdateStatusResult{}, err
	}

	if strings.TrimSpace(params.ExpectedOldStatus) != "" {
		updateSQL, execArgs := buildConditionalUpdateSQL(tableName, statusName, setParts, args, params.ID, params.ExpectedOldStatus)
		commandTag, err := pool.Exec(stepCtx, updateSQL, execArgs...)
		if err != nil {
			return UpdateStatusResult{}, classifyDBError("conditional update", err)
		}

		if commandTag.RowsAffected() == 0 {
			current, err := loadCurrentStatus(stepCtx, pool, tableName, statusName, params.ID)
			if err != nil {
				return UpdateStatusResult{}, err
			}
			logger.Info("db_step.finish",
				zap.String("step", "UpdateStatusStep"),
				zap.String("table", params.Table),
				zap.String("id", params.ID),
				zap.String("new_status", params.NewStatus),
				zap.Bool("updated", false),
			)
			return UpdateStatusResult{Updated: false, OldStatus: current}, nil
		}

		logger.Info("db_step.finish",
			zap.String("step", "UpdateStatusStep"),
			zap.String("table", params.Table),
			zap.String("id", params.ID),
			zap.String("new_status", params.NewStatus),
			zap.Bool("updated", true),
		)
		return UpdateStatusResult{Updated: true, OldStatus: params.ExpectedOldStatus}, nil
	}

	// Unconditional update: fetch current status when available.
	current, err := loadCurrentStatus(stepCtx, pool, tableName, statusName, params.ID)
	if err != nil {
		return UpdateStatusResult{}, err
	}
	result.OldStatus = current

	updateSQL, execArgs := buildUnconditionalUpdateSQL(tableName, statusName, setParts, args, params.ID)
	commandTag, err := pool.Exec(stepCtx, updateSQL, execArgs...)
	if err != nil {
		return UpdateStatusResult{}, classifyDBError("update status", err)
	}

	updated := commandTag.RowsAffected() > 0
	logger.Info("db_step.finish",
		zap.String("step", "UpdateStatusStep"),
		zap.String("table", params.Table),
		zap.String("id", params.ID),
		zap.String("new_status", params.NewStatus),
		zap.Bool("updated", updated),
	)

	return UpdateStatusResult{Updated: updated, OldStatus: result.OldStatus}, nil
}

func loadCurrentStatus(ctx context.Context, pool *pgxpool.Pool, tableName, statusName, id string) (string, error) {
	query := fmt.Sprintf("SELECT %s FROM %s WHERE id=$1", statusName, tableName)
	var current string
	err := pool.QueryRow(ctx, query, id).Scan(&current)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", nil
		}
		return "", classifyDBError("load current status", err)
	}
	return current, nil
}

func buildColumnList(columns []string) (string, error) {
	if len(columns) == 0 {
		return "*", nil
	}

	sanitized := make([]string, len(columns))
	for i, col := range columns {
		name, err := normalizeIdentifier(col)
		if err != nil {
			return "", fmt.Errorf("invalid column[%d]: %w", i, err)
		}
		sanitized[i] = name
	}
	return strings.Join(sanitized, ", "), nil
}

func normalizeIdentifier(value string) (string, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "", fmt.Errorf("identifier is required")
	}
	for i, r := range trimmed {
		if !(r == '_' || (r >= '0' && r <= '9' && i > 0) || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')) {
			return "", fmt.Errorf("identifier %q contains invalid character %q", trimmed, r)
		}
	}
	return pgx.Identifier{trimmed}.Sanitize(), nil
}

func getSharedPool(ctx context.Context) (*pgxpool.Pool, error) {
	select {
	case <-ctx.Done():
		return nil, &TransientError{Err: ctx.Err()}
	default:
	}

	sharedPoolMu.RLock()
	pool := sharedPool
	sharedPoolMu.RUnlock()

	if pool == nil {
		return nil, &PermanentError{Msg: "database pool not configured"}
	}

	return pool, nil
}

func classifyPoolError(err error) error {
	if err == nil {
		return nil
	}
	var validationErr *ValidationError
	if errors.As(err, &validationErr) {
		return err
	}
	var permanentErr *PermanentError
	if errors.As(err, &permanentErr) {
		return err
	}
	return &TransientError{Err: err}
}

func classifyDBError(operation string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return &TransientError{Err: fmt.Errorf("%s: %w", operation, err)}
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if len(pgErr.Code) >= 2 {
			class := pgErr.Code[:2]
			switch class {
			case "22", "23", "24", "25", "42":
				return &PermanentError{Msg: fmt.Sprintf("%s: %s", operation, pgErr.Message), Err: err}
			}
		}
		return &TransientError{Err: fmt.Errorf("%s: %w", operation, err)}
	}

	return &TransientError{Err: fmt.Errorf("%s: %w", operation, err)}
}

func buildUpdateAssignments(statusName, newStatus string, extras map[string]ColumnUpdate) ([]string, []any, error) {
	setParts := make([]string, 0, 1+len(extras))
	args := make([]any, 0, 1+len(extras))
	nextParam := 1

	setParts = append(setParts, fmt.Sprintf("%s=$%d", statusName, nextParam))
	args = append(args, newStatus)
	nextParam++

	if len(extras) == 0 {
		return setParts, args, nil
	}

	columns := make([]string, 0, len(extras))
	for name := range extras {
		columns = append(columns, name)
	}
	sort.Strings(columns)

	for _, name := range columns {
		assignment := extras[name]
		sanitized, err := normalizeIdentifier(name)
		if err != nil {
			return nil, nil, &ValidationError{Msg: fmt.Sprintf("invalid column %s: %v", name, err)}
		}
		if assignment.UseCurrentTimestamp {
			setParts = append(setParts, fmt.Sprintf("%s=CURRENT_TIMESTAMP", sanitized))
			continue
		}
		setParts = append(setParts, fmt.Sprintf("%s=$%d", sanitized, nextParam))
		args = append(args, assignment.Value)
		nextParam++
	}

	return setParts, args, nil
}

func buildConditionalUpdateSQL(tableName, statusName string, setParts []string, args []any, id string, expected string) (string, []any) {
	updateParts := strings.Join(setParts, ", ")
	placeholderCount := len(args)
	updateSQL := fmt.Sprintf("UPDATE %s SET %s WHERE id=$%d AND %s=$%d", tableName, updateParts, placeholderCount+1, statusName, placeholderCount+2)
	execArgs := append(make([]any, 0, len(args)+2), args...)
	execArgs = append(execArgs, id, expected)
	return updateSQL, execArgs
}

func buildUnconditionalUpdateSQL(tableName, statusName string, setParts []string, args []any, id string) (string, []any) {
	updateParts := strings.Join(setParts, ", ")
	placeholderCount := len(args)
	updateSQL := fmt.Sprintf("UPDATE %s SET %s WHERE id=$%d", tableName, updateParts, placeholderCount+1)
	execArgs := append(make([]any, 0, len(args)+1), args...)
	execArgs = append(execArgs, id)
	return updateSQL, execArgs
}
