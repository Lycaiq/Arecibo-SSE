package handler_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/arecibo-sse/publisher/internal/handler"
)

// publisherMock implementa handler.Publisher sin tocar NATS.
type publisherMock struct {
	lastTopic   string
	lastPayload any
	shouldFail  bool
}

func (m *publisherMock) Publish(topic string, payload any) error {
	if m.shouldFail {
		return fmt.Errorf("error simulado de nats")
	}
	m.lastTopic = topic
	m.lastPayload = payload
	return nil
}

func TestPublishHandler_OK(t *testing.T) {
	mock := &publisherMock{}
	h := handler.NewPublishHandler(mock)

	body := `{"topic":"test.events","data":{"message":"hola"}}`
	req := httptest.NewRequest(http.MethodPost, "/publish", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("esperaba 202, obtuvo %d", rec.Code)
	}
	if mock.lastTopic != "test.events" {
		t.Fatalf("topic incorrecto: %q", mock.lastTopic)
	}
}

func TestPublishHandler_TopicVacio(t *testing.T) {
	mock := &publisherMock{}
	h := handler.NewPublishHandler(mock)

	body := `{"topic":"  ","data":{}}`
	req := httptest.NewRequest(http.MethodPost, "/publish", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("esperaba 400, obtuvo %d", rec.Code)
	}
}

func TestPublishHandler_MetodoNoPermitido(t *testing.T) {
	mock := &publisherMock{}
	h := handler.NewPublishHandler(mock)

	req := httptest.NewRequest(http.MethodGet, "/publish", nil)
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("esperaba 405, obtuvo %d", rec.Code)
	}
}

func TestPublishHandler_ErrorNATS(t *testing.T) {
	mock := &publisherMock{shouldFail: true}
	h := handler.NewPublishHandler(mock)

	body := `{"topic":"test.fallo","data":{}}`
	req := httptest.NewRequest(http.MethodPost, "/publish", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("esperaba 500, obtuvo %d", rec.Code)
	}
}

func TestPublishHandler_BodyMalformado(t *testing.T) {
	mock := &publisherMock{}
	h := handler.NewPublishHandler(mock)

	req := httptest.NewRequest(http.MethodPost, "/publish", bytes.NewBufferString("no-es-json"))
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("esperaba 400, obtuvo %d", rec.Code)
	}
}

func TestPublishHandler_RespuestaJSON(t *testing.T) {
	mock := &publisherMock{}
	h := handler.NewPublishHandler(mock)

	body := `{"topic":"alerts.critical","data":{"level":"high"}}`
	req := httptest.NewRequest(http.MethodPost, "/publish", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	var resp map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("respuesta no es JSON válido: %v", err)
	}

	if resp["status"] != "published" {
		t.Fatalf("status inesperado: %q", resp["status"])
	}
	if resp["topic"] != "alerts.critical" {
		t.Fatalf("topic inesperado: %q", resp["topic"])
	}
}
