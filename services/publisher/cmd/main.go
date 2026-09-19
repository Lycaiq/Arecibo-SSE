package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/arecibo-sse/publisher/internal/broker"
	"github.com/arecibo-sse/publisher/internal/config"
	"github.com/arecibo-sse/publisher/internal/handler"
	"github.com/arecibo-sse/publisher/internal/middleware"
)

func main() {
	// Configuramos slog con formato texto para desarrollo.
	// En producción bastaría cambiar a slog.NewJSONHandler para ingestion en Loki/Datadog/etc.
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	cfg, err := config.Load()
	if err != nil {
		slog.Error("cargando config", "error", err)
		os.Exit(1)
	}

	// Conectamos a NATS antes de levantar el HTTP server.
	// No tiene sentido aceptar peticiones si no hay donde publicarlas.
	natsClient, err := broker.New(cfg.NatsURL)
	if err != nil {
		slog.Error("conectando a nats", "url", cfg.NatsURL, "error", err)
		os.Exit(1)
	}
	defer natsClient.Close()

	slog.Info("nats conectado", "url", cfg.NatsURL)

	// Registramos rutas en el mux de la librería estándar.
	mux := http.NewServeMux()
	mux.Handle("POST /publish", handler.NewPublishHandler(natsClient))
	mux.HandleFunc("GET /health", healthHandler)

	// Cadena de middleware: RequestID primero para que Logger ya tenga el ID disponible.
	// El orden importa: RequestID → Logger → Mux (manejadores reales).
	chain := middleware.RequestID(middleware.Logger(mux))

	server := &http.Server{
		Addr:    cfg.Addr(),
		Handler: chain,

		// Timeouts explícitos para evitar goroutines colgadas por conexiones lentas.
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Canal para capturar señales del SO — buffered para que el sender no bloquee.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		slog.Info("publisher escuchando", "addr", cfg.Addr())
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("servidor http caído", "error", err)
			os.Exit(1)
		}
	}()

	// Bloqueamos hasta recibir señal de parada.
	sig := <-quit
	slog.Info("señal recibida, apagando", "signal", sig.String())

	// 10 segundos para terminar las conexiones activas antes de forzar el cierre.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		slog.Error("shutdown forzado", "error", err)
		os.Exit(1)
	}

	slog.Info("publisher apagado limpiamente")
}

// healthHandler es un liveness probe simple para Docker y orquestadores.
func healthHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status":    "ok",
		"service":   "publisher",
		"timestamp": fmt.Sprintf("%d", time.Now().Unix()),
	})
}
