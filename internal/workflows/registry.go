package workflows

import (
	"context"
	"fmt"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ssinghl/dbos/internal/config"
)

func Initialize(ctx context.Context, cfg *config.Config, pool *pgxpool.Pool) (dbos.DBOSContext, error) {
	dbosConfig := dbos.Config{AppName: cfg.AppName}

	switch {
	case pool != nil:
		dbosConfig.SystemDBPool = pool
	case cfg.DatabaseURL != "":
		dbosConfig.DatabaseURL = cfg.DatabaseURL
	default:
		return nil, fmt.Errorf("missing database configuration for DBOS")
	}

	dbosCtx, err := dbos.NewDBOSContext(ctx, dbosConfig)
	if err != nil {
		return nil, fmt.Errorf("create DBOS context: %w", err)
	}

	if err := registerWorkflows(dbosCtx); err != nil {
		return nil, fmt.Errorf("register workflows: %w", err)
	}

	if err := dbosCtx.Launch(); err != nil {
		return nil, fmt.Errorf("launch DBOS: %w", err)
	}

	return dbosCtx, nil
}

func registerWorkflows(ctx dbos.DBOSContext) error {
	if ctx == nil {
		return fmt.Errorf("dbos context is not initialized")
	}

	dbos.RegisterWorkflow(ctx, scheduledCallbackWorkflowEntry, dbos.WithWorkflowName(scheduledCallbackWorkflowName))
	return nil
}
