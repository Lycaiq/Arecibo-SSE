package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/arecibo-sse/gateway/internal/broker"
	"github.com/arecibo-sse/gateway/internal/config"
	"github.com/arecibo-sse/gateway/internal/sse"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	// Conectamos a NATS antes de aceptar conexiones SSE.
	natsClient, err := broker.New(cfg.NatsURL)
	if err != nil {
		log.Fatalf("nats: %v", err)
	}
	defer natsClient.Close()

	log.Printf("nats: conectado a %s", cfg.NatsURL)

	// Construimos el handler SSE con el cliente NATS inyectado.
	sseHandler := sse.NewHandler(natsClient, cfg.AllowedOrigin)

	mux := http.NewServeMux()

	// El endpoint principal: cada cliente SSE hace GET /subscribe?topic=<topic>
	mux.Handle("GET /subscribe", sseHandler)

	// Preflight CORS para browsers que mandan OPTIONS antes del EventSource.
	// En la práctica EventSource solo usa GET, pero algunos proxies lo transforman.
	mux.HandleFunc("OPTIONS /subscribe", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", cfg.AllowedOrigin)
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Cache-Control")
		w.WriteHeader(http.StatusNoContent)
	})

	mux.HandleFunc("GET /health", sse.HealthHandler)

	server := &http.Server{
		Addr:    cfg.Addr(),
		Handler: mux,

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
		log.Printf("gateway: escuchando en %s", cfg.Addr())
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http: %v", err)
		}
	}()

	<-quit
	log.Println("gateway: señal recibida, apagando...")

	// 30s de gracia para que los clientes SSE activos reciban un cierre limpio.
	// Es más que el publisher porque las conexiones SSE son de larga duración.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Fatalf("gateway: shutdown forzado: %v", err)
	}

	log.Println("gateway: apagado limpio")
}
