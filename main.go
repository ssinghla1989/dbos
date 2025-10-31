package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/ssinghl/dbos/internal/api"
	"github.com/ssinghl/dbos/internal/config"
	"github.com/ssinghl/dbos/internal/db"
	"github.com/ssinghl/dbos/internal/logger"
	"github.com/ssinghl/dbos/internal/workflows"
	"github.com/ssinghl/dbos/internal/workflows/steps"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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
		defer db.Close(pool)
	}
	steps.ConfigureDBPool(pool)

	dbosCtx, err := workflows.Initialize(ctx, cfg, pool)
	if err != nil {
		zapLogger.Fatal("failed to initialize DBOS", zap.Error(err))
	}
	defer dbosCtx.Shutdown(5 * time.Second)

	handlers := api.NewHandlers(zapLogger, cfg, dbosCtx, pool)
	router := api.NewRouter(handlers)

	server := &http.Server{
		Addr:    cfg.ServerAddress(),
		Handler: router,
	}

	zapLogger.Info("starting HTTP server", zap.String("addr", server.Addr))

	serverErr := make(chan error, 1)
	go func() {
		defer close(serverErr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	select {
	case <-ctx.Done():
		zapLogger.Info("shutdown signal received")
	case err := <-serverErr:
		if err != nil {
			zapLogger.Fatal("http server failed", zap.Error(err))
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil && !errors.Is(err, context.Canceled) {
		zapLogger.Error("server shutdown error", zap.Error(err))
	}

	zapLogger.Info("http server stopped")
}
