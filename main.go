package main

import (
	"context"
	"log"
	"time"

	"go.uber.org/zap"

	"github.com/ssinghl/dbos/internal/api"
	"github.com/ssinghl/dbos/internal/config"
	"github.com/ssinghl/dbos/internal/db"
	"github.com/ssinghl/dbos/internal/logger"
	"github.com/ssinghl/dbos/internal/workflows"
)

func main() {
	ctx := context.Background()

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("failed to load configuration: %v", err)
	}

	zapLogger, err := logger.New(cfg)
	if err != nil {
		log.Fatalf("failed to initialize logger: %v", err)
	}
	defer func() {
		_ = zapLogger.Sync()
	}()

	pool, err := db.Init(ctx, cfg)
	if err != nil {
		zapLogger.Fatal("failed to initialize database", zap.Error(err))
	}
	if pool != nil {
		defer pool.Close()
	}

	dbosCtx, err := workflows.Initialize(ctx, cfg, pool)
	if err != nil {
		zapLogger.Fatal("failed to initialize DBOS", zap.Error(err))
	}
	defer dbosCtx.Shutdown(5 * time.Second)

	handlers := api.NewHandlers(zapLogger, cfg, dbosCtx)
	router := api.NewRouter(handlers)

	addr := cfg.ServerAddress()
	zapLogger.Info("starting HTTP server", zap.String("addr", addr))

	if err := router.Run(addr); err != nil {
		zapLogger.Fatal("server shutdown", zap.Error(err))
	}
}
