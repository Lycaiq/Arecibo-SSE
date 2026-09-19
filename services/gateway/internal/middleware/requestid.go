package middleware

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// ctxKey es el tipo privado para las claves de contexto.
// Evita colisiones con otras librerías que usen string como clave.
type ctxKey string

const requestIDKey ctxKey = "request_id"

// RequestID es el middleware que gestiona el ciclo de vida del X-Request-ID.
//
// Si el cliente envía X-Request-ID lo usamos tal cual — permite correlacionar
// con sistemas externos (API gateway, load balancer, frontend).
// Si no viene, generamos uno internamente para el seguimiento interno.
// En ambos casos lo devolvemos en la respuesta para que el caller lo vea.
func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimSpace(r.Header.Get("X-Request-ID"))
		if id == "" {
			id = newID()
		}

		// Propagamos el ID al contexto para que los handlers downstream lo lean.
		ctx := context.WithValue(r.Context(), requestIDKey, id)
		r = r.WithContext(ctx)

		// Lo devolvemos en la respuesta — esencial para que el cliente pueda
		// buscar el ID en los logs cuando algo falla.
		w.Header().Set("X-Request-ID", id)

		next.ServeHTTP(w, r)
	})
}

// Logger registra cada petición con su request ID, método, ruta, status y duración.
// Para conexiones SSE de larga duración, la línea de "completado" aparece cuando el
// cliente se desconecta, lo que nos da la duración real de la sesión SSE.
func Logger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := FromContext(r.Context())
		start := time.Now()

		// Capturamos el status code real — el ResponseWriter estándar no lo expone.
		lrw := newLoggingWriter(w)

		slog.Info("request recibido",
			"request_id", id,
			"method", r.Method,
			"path", r.URL.Path,
			"query", r.URL.RawQuery,
			"remote", r.RemoteAddr,
			"user_agent", r.UserAgent(),
		)

		next.ServeHTTP(lrw, r)

		slog.Info("request completado",
			"request_id", id,
			"method", r.Method,
			"path", r.URL.Path,
			"status", lrw.status,
			"duration_ms", time.Since(start).Milliseconds(),
		)
	})
}

// FromContext extrae el request ID del contexto.
// Devuelve "unknown" si no existe — así los logs nunca tienen campos vacíos.
func FromContext(ctx context.Context) string {
	if id, ok := ctx.Value(requestIDKey).(string); ok && id != "" {
		return id
	}
	return "unknown"
}

// newID genera un ID aleatorio de 8 bytes en hexadecimal (16 chars).
// Es suficientemente único para correlacionar logs sin necesitar UUID completo.
func newID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// loggingWriter envuelve http.ResponseWriter capturando el status code.
// También implementa http.Flusher para que SSE siga funcionando a través del middleware.
type loggingWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func newLoggingWriter(w http.ResponseWriter) *loggingWriter {
	return &loggingWriter{ResponseWriter: w, status: http.StatusOK}
}

// WriteHeader captura el status y lo delega.
func (lw *loggingWriter) WriteHeader(code int) {
	if !lw.wroteHeader {
		lw.status = code
		lw.wroteHeader = true
		lw.ResponseWriter.WriteHeader(code)
	}
}

// Write captura el status implícito 200 cuando se escribe sin llamar a WriteHeader primero.
func (lw *loggingWriter) Write(b []byte) (int, error) {
	if !lw.wroteHeader {
		lw.WriteHeader(http.StatusOK)
	}
	return lw.ResponseWriter.Write(b)
}

// Flush delega al ResponseWriter subyacente si soporta flushing.
// CRÍTICO para SSE: sin esto, el check w.(http.Flusher) fallaría y el handler
// rechazaría todas las conexiones SSE con un error 500.
func (lw *loggingWriter) Flush() {
	if f, ok := lw.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}
