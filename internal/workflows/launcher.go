package workflows

import (
	"context"
	"fmt"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"go.uber.org/zap"
)

// StartScheduledCallbackWorkflow triggers the DBOS workflow associated with a scheduled callback job.
func StartScheduledCallbackWorkflow(ctx context.Context, dbosCtx dbos.DBOSContext, jobID string) error {
	if dbosCtx == nil {
		return fmt.Errorf("dbos context is not initialized")
	}

	logger := zap.L().With(
		zap.String("workflow", scheduledCallbackWorkflowName),
		zap.String("job_id", jobID),
	)

	logger.Info("dispatching scheduled callback workflow")

	workflowOpts := []dbos.WorkflowOption{
		dbos.WithWorkflowID(jobID),
	}

	if _, err := dbos.RunWorkflow(dbosCtx, scheduledCallbackWorkflowEntry, jobID, workflowOpts...); err != nil {
		return fmt.Errorf("start scheduled callback workflow: %w", err)
	}

	logger.Info("scheduled callback workflow enqueued")
	return nil
}
