package sse

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/arecibo-sse/gateway/internal/middleware"
)

// keepAliveInterval define cada cuánto mandamos un comentario SSE de keepalive.
// 25s es seguro: la mayoría de proxies y balanceadores cortan conexiones idle en 30-60s.
const keepAliveInterval = 25 * time.Second

// Subscriber es la abstracción que necesita el handler del broker.
// Así podemos testear el handler sin levantar NATS real.
type Subscriber interface {
	Subscribe(topic string) (<-chan []byte, func(), error)
}

// Handler gestiona las conexiones SSE entrantes.
type Handler struct {
	subscriber    Subscriber
	allowedOrigin string
}

// NewHandler construye el handler con sus dependencias inyectadas.
func NewHandler(s Subscriber, allowedOrigin string) *Handler {
	return &Handler{
		subscriber:    s,
		allowedOrigin: allowedOrigin,
	}
}

// ServeHTTP maneja GET /subscribe?topic=<topic>.
//
// Cada conexión SSE tiene su propio X-Request-ID para poder seguir todo su ciclo
// de vida en los logs: apertura, mensajes enviados, duración y causa de cierre.
//
// Flujo de vida de una conexión:
//   1. Extraemos el request ID del contexto (inyectado por el middleware).
//   2. Validamos flusher y topic.
//   3. Suscribimos a NATS y registramos el defer de cancelación.
//   4. Logueamos "conectado" con el request ID.
//   5. Loop: mensajes, keepalive o ctx.Done().
//   6. El defer loguea "desconectado" con duración y total de mensajes enviados.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	reqID := middleware.FromContext(r.Context())

	// Solo GET tiene sentido para SSE.
	if r.Method != http.MethodGet {
		http.Error(w, `{"error":"método no permitido"}`, http.StatusMethodNotAllowed)
		return
	}

	// SSE requiere que el ResponseWriter pueda hacer flush inmediato.
	// El loggingWriter del middleware también implementa Flusher, así que este check no falla.
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, `{"error":"streaming no soportado por este servidor"}`, http.StatusInternalServerError)
		return
	}

	topic := strings.TrimSpace(r.URL.Query().Get("topic"))
	if topic == "" {
		http.Error(w, `{"error":"parámetro topic requerido"}`, http.StatusBadRequest)
		return
	}

	// Cabeceras SSE estándar.
	h.setSSEHeaders(w)

	// Suscribimos a NATS antes de escribir nada en el body.
	// Si falla aquí podemos devolver un error HTTP normal todavía.
	msgCh, cancel, err := h.subscriber.Subscribe(topic)
	if err != nil {
		slog.Error("sse: error suscribiendo",
			"request_id", reqID,
			"topic", topic,
			"error", err,
		)
		http.Error(w, `{"error":"error al suscribirse al topic"}`, http.StatusInternalServerError)
		return
	}

	connectedAt := time.Now()
	var msgsSent int64

	// CRÍTICO: cancel se llama siempre al salir, sin importar cómo termine la función.
	// El log de desconexión incluye duración y mensajes enviados para auditoría.
	defer func() {
		cancel()
		slog.Info("sse: cliente desconectado",
			"request_id", reqID,
			"topic", topic,
			"remote", r.RemoteAddr,
			"msgs_sent", msgsSent,
			"duration", time.Since(connectedAt).Round(time.Millisecond).String(),
		)
	}()

	slog.Info("sse: cliente conectado",
		"request_id", reqID,
		"topic", topic,
		"remote", r.RemoteAddr,
	)

	// Primer flush: confirma al browser que la conexión SSE está establecida.
	// Sin esto, algunos browsers esperan datos antes de disparar el evento `open` del EventSource.
	fmt.Fprintf(w, ": connected to %s\n\n", topic)
	flusher.Flush()

	keepAlive := time.NewTicker(keepAliveInterval)
	defer keepAlive.Stop()

	ctx := r.Context()

	for {
		select {

		case <-ctx.Done():
			// El cliente cerró la conexión (navegador cerrado, tab cerrada, red cortada).
			// El defer se encarga del cleanup — aquí solo salimos del loop.
			return

		case data, ok := <-msgCh:
			if !ok {
				// El canal fue cerrado desde el broker — situación anormal, salimos limpiamente.
				slog.Warn("sse: canal cerrado inesperadamente",
					"request_id", reqID,
					"topic", topic,
				)
				return
			}

			// Formato SSE estándar: "data: <payload>\n\n"
			// El doble salto de línea marca el fin del evento y dispara el listener en el browser.
			if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
				// Si no podemos escribir, el cliente ya se fue. Dejamos que el defer limpie.
				slog.Warn("sse: error escribiendo al cliente",
					"request_id", reqID,
					"topic", topic,
					"error", err,
				)
				return
			}
			flusher.Flush()
			msgsSent++

		case <-keepAlive.C:
			// Comentario SSE: el browser lo ignora pero mantiene la conexión TCP viva
			// y nos permite detectar clientes desconectados (el write fallará si el cliente se fue).
			if _, err := fmt.Fprintf(w, ": keepalive\n\n"); err != nil {
				slog.Warn("sse: cliente muerto detectado en keepalive",
					"request_id", reqID,
					"topic", topic,
					"error", err,
				)
				return
			}
			flusher.Flush()
		}
	}
}

// setSSEHeaders escribe todas las cabeceras necesarias para que SSE funcione correctamente
// a través de proxies, CDNs y browsers modernos.
func (h *Handler) setSSEHeaders(w http.ResponseWriter) {
	header := w.Header()
	header.Set("Content-Type", "text/event-stream")
	header.Set("Cache-Control", "no-cache, no-store, must-revalidate")
	header.Set("Connection", "keep-alive")
	header.Set("X-Accel-Buffering", "no")

	// CORS: necesario porque el frontend corre en un puerto diferente al gateway.
	header.Set("Access-Control-Allow-Origin", h.allowedOrigin)
	header.Set("Access-Control-Allow-Headers", "Cache-Control")
	header.Set("Access-Control-Allow-Credentials", "true")
}

// healthResponse es la respuesta del endpoint de salud.
type healthResponse struct {
	Status    string `json:"status"`
	Service   string `json:"service"`
	Timestamp int64  `json:"timestamp"`
}

// HealthHandler devuelve el estado del servicio.
func HealthHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(healthResponse{
		Status:    "ok",
		Service:   "gateway",
		Timestamp: time.Now().Unix(),
	})
}
