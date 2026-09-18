package sse

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"
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
// Flujo de vida de una conexión:
//   1. Validamos que el ResponseWriter soporte flush (necesario para SSE).
//   2. Validamos y sanitizamos el topic.
//   3. Escribimos las cabeceras SSE y hacemos el primer flush.
//   4. Creamos la suscripción NATS y registramos su cancelación con defer.
//   5. Loop: escuchamos mensajes, keepalive o desconexión del cliente.
//   6. Al salir del loop (por cualquier causa), el defer cancela la suscripción.
//
// El defer de cancelación en el paso 4 es la pieza crítica de este servicio.
// Sin él, cada cliente que se desconecta dejaría una goroutine y una suscripción NATS
// viva para siempre — un memory leak que escala linealmente con el tráfico.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Solo GET tiene sentido para SSE.
	if r.Method != http.MethodGet {
		http.Error(w, `{"error":"método no permitido"}`, http.StatusMethodNotAllowed)
		return
	}

	// SSE requiere que el ResponseWriter pueda hacer flush inmediato.
	// Si no lo soporta (ej. algunos middlewares de compresión), no podemos continuar.
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
	// X-Accel-Buffering: no → deshabilita el buffering de nginx para esta respuesta.
	// Sin esto, nginx acumula el stream y los clientes no ven nada hasta que el buffer se llene.
	h.setSSEHeaders(w)

	// Suscribimos a NATS antes de escribir nada en el body.
	// Si falla aquí podemos devolver un error HTTP normal todavía.
	msgCh, cancel, err := h.subscriber.Subscribe(topic)
	if err != nil {
		log.Printf("sse: error suscribiendo a %q: %v", topic, err)
		http.Error(w, `{"error":"error al suscribirse al topic"}`, http.StatusInternalServerError)
		return
	}

	// CRÍTICO: cancel se llama siempre al salir, sin importar cómo termine la función.
	// Cubre: desconexión del cliente, error de escritura, panic (con recover en el mux), timeout.
	defer func() {
		cancel()
		log.Printf("sse: cliente desconectado de %q (remote: %s)", topic, r.RemoteAddr)
	}()

	log.Printf("sse: cliente conectado a %q (remote: %s)", topic, r.RemoteAddr)

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
				log.Printf("sse: canal cerrado inesperadamente para topic %q", topic)
				return
			}

			// Formato SSE estándar: "data: <payload>\n\n"
			// El doble salto de línea marca el fin del evento y dispara el listener en el browser.
			if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
				// Si no podemos escribir, el cliente ya se fue. Dejamos que el defer limpie.
				log.Printf("sse: error escribiendo a cliente en %q: %v", topic, err)
				return
			}
			flusher.Flush()

		case <-keepAlive.C:
			// Comentario SSE: el browser lo ignora pero mantiene la conexión TCP viva
			// y nos permite detectar clientes desconectados (el write fallará si el cliente se fue).
			if _, err := fmt.Fprintf(w, ": keepalive\n\n"); err != nil {
				log.Printf("sse: cliente muerto detectado en keepalive para %q: %v", topic, err)
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
// Puede extenderse para verificar la conexión NATS en el futuro.
func HealthHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(healthResponse{
		Status:    "ok",
		Service:   "gateway",
		Timestamp: time.Now().Unix(),
	})
}
