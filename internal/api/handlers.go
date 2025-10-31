package api

import (
	"net/http"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/ssinghl/dbos/internal/config"
)

type Handlers struct {
	logger  *zap.Logger
	cfg     *config.Config
	dbosCtx dbos.DBOSContext
}

func NewHandlers(logger *zap.Logger, cfg *config.Config, dbosCtx dbos.DBOSContext) *Handlers {
	return &Handlers{
		logger:  logger,
		cfg:     cfg,
		dbosCtx: dbosCtx,
	}
}

func (h *Handlers) Health(c *gin.Context) {
	h.logger.Debug("health check invoked")
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}
