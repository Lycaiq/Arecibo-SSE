package handler

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"
)

// Publisher define la interfaz mínima que necesita el handler para operar.
// Dependemos de la abstracción para poder mockear NATS en los tests sin levantar nada.
type Publisher interface {
	Publish(topic string, payload any) error
}

// PublishRequest es el cuerpo JSON esperado en POST /publish.
type PublishRequest struct {
	Topic string         `json:"topic"`
	Data  map[string]any `json:"data"`
}

// Event es el envelope que transita por NATS hacia el gateway y luego al browser.
// Incluimos topic y timestamp aquí para que el frontend los tenga disponibles
// sin tener que parsear o inferir contexto desde afuera.
type Event struct {
	Topic     string         `json:"topic"`
	Data      map[string]any `json:"data"`
	Timestamp time.Time      `json:"timestamp"`
}

// PublishResponse es lo que devolvemos al caller cuando todo salió bien.
type PublishResponse struct {
	Status string `json:"status"`
	Topic  string `json:"topic"`
}

// PublishHandler encapsula la lógica de publicación HTTP → NATS.
type PublishHandler struct {
	publisher Publisher
}

// NewPublishHandler inyecta el publisher — el handler no sabe si es NATS real o un mock.
func NewPublishHandler(p Publisher) *PublishHandler {
	return &PublishHandler{publisher: p}
}

// ServeHTTP implementa http.Handler directamente para no necesitar un router externo.
func (h *PublishHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"método no permitido"}`, http.StatusMethodNotAllowed)
		return
	}

	// Limitamos el body a 1MB para evitar que alguien nos mande un payload gigante.
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	var req PublishRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&req); err != nil {
		http.Error(w, `{"error":"body inválido o malformado"}`, http.StatusBadRequest)
		return
	}

	// Un topic vacío o solo espacios rompería las suscripciones de NATS silenciosamente.
	req.Topic = strings.TrimSpace(req.Topic)
	if req.Topic == "" {
		http.Error(w, `{"error":"el campo topic es requerido"}`, http.StatusBadRequest)
		return
	}

	event := Event{
		Topic:     req.Topic,
		Data:      req.Data,
		Timestamp: time.Now().UTC(),
	}

	if err := h.publisher.Publish(req.Topic, event); err != nil {
		log.Printf("handler: error publicando en %q: %v", req.Topic, err)
		http.Error(w, `{"error":"error al publicar el evento"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)

	// Ignoramos el error del Encode — si falló al escribir la respuesta ya no hay mucho que hacer.
	_ = json.NewEncoder(w).Encode(PublishResponse{
		Status: "published",
		Topic:  req.Topic,
	})
}
