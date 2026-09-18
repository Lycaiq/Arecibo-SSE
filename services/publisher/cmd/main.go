package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/arecibo-sse/publisher/internal/broker"
	"github.com/arecibo-sse/publisher/internal/config"
	"github.com/arecibo-sse/publisher/internal/handler"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	// Conectamos a NATS antes de levantar el HTTP server.
	// No tiene sentido aceptar peticiones si no hay donde publicarlas.
	natsClient, err := broker.New(cfg.NatsURL)
	if err != nil {
		log.Fatalf("nats: %v", err)
	}
	defer natsClient.Close()

	log.Printf("nats: conectado a %s", cfg.NatsURL)

	// Registramos rutas en el mux de la librería estándar.
	// Para dos endpoints no justificamos traer un router externo.
	mux := http.NewServeMux()

	mux.Handle("POST /publish", handler.NewPublishHandler(natsClient))
	mux.HandleFunc("GET /health", healthHandler)

	server := &http.Server{
		Addr:    cfg.Addr(),
		Handler: mux,

		// Timeouts explícitos para evitar goroutines colgadas por conexiones lentas.
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Canal para capturar señales del SO — buffered para que el sender no bloquee.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		log.Printf("publisher: escuchando en %s", cfg.Addr())
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http: %v", err)
		}
	}()

	// Bloqueamos hasta recibir señal de parada.
	<-quit
	log.Println("publisher: señal recibida, apagando...")

	// 10 segundos para terminar las conexiones activas antes de forzar el cierre.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Fatalf("publisher: shutdown forzado: %v", err)
	}

	log.Println("publisher: apagado limpio")
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
