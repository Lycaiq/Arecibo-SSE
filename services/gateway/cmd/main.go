package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/arecibo-sse/gateway/internal/broker"
	"github.com/arecibo-sse/gateway/internal/config"
	"github.com/arecibo-sse/gateway/internal/middleware"
	"github.com/arecibo-sse/gateway/internal/sse"
)

func main() {
	// Mismo formato de logs que el publisher — facilita unificar ambos streams
	// en herramientas como Loki, Datadog o cualquier agregador que parsee texto.
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	cfg, err := config.Load()
	if err != nil {
		slog.Error("cargando config", "error", err)
		os.Exit(1)
	}

	// Conectamos a NATS antes de aceptar conexiones SSE.
	natsClient, err := broker.New(cfg.NatsURL)
	if err != nil {
		slog.Error("conectando a nats", "url", cfg.NatsURL, "error", err)
		os.Exit(1)
	}
	defer natsClient.Close()

	slog.Info("nats conectado", "url", cfg.NatsURL)

	// Construimos el handler SSE con el cliente NATS inyectado.
	sseHandler := sse.NewHandler(natsClient, cfg.AllowedOrigin)

	mux := http.NewServeMux()

	// El endpoint principal: cada cliente SSE hace GET /subscribe?topic=<topic>
	mux.Handle("GET /subscribe", sseHandler)

	// Preflight CORS para browsers que mandan OPTIONS antes del EventSource.
	mux.HandleFunc("OPTIONS /subscribe", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", cfg.AllowedOrigin)
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Cache-Control, X-Request-ID")
		w.WriteHeader(http.StatusNoContent)
	})

	mux.HandleFunc("GET /health", sse.HealthHandler)

	// Cadena de middleware: RequestID → Logger → Mux.
	// El Logger loguea "completado" cuando el handler SSE termina (cliente desconectado),
	// lo que nos da la duración real de cada sesión SSE en los logs del middleware.
	chain := middleware.RequestID(middleware.Logger(mux))

	server := &http.Server{
		Addr:    cfg.Addr(),
		Handler: chain,

		// ReadTimeout bajo porque una vez establecido el stream no hay más reads del cliente.
		ReadTimeout: 5 * time.Second,

		// WriteTimeout en 0 para SSE — las conexiones son de larga duración por diseño.
		// Un timeout aquí mataría conexiones SSE activas después de N segundos.
		WriteTimeout: 0,

		IdleTimeout: 60 * time.Second,
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		slog.Info("gateway escuchando", "addr", cfg.Addr())
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("servidor http caído", "error", err)
			os.Exit(1)
		}
	}()

	sig := <-quit
	slog.Info("señal recibida, apagando", "signal", sig.String())

	// 30s de gracia para que los clientes SSE activos reciban un cierre limpio.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		slog.Error("shutdown forzado", "error", err)
		os.Exit(1)
	}

	slog.Info("gateway apagado limpiamente")
}
