package sse_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/arecibo-sse/gateway/internal/sse"
)

// flusherRecorder extiende httptest.ResponseRecorder implementando http.Flusher.
// El recorder nativo no lo implementa y el handler lo requiere para arrancar.
type flusherRecorder struct {
	*httptest.ResponseRecorder
	flushed int
}

func (fr *flusherRecorder) Flush() {
	fr.flushed++
}

// mockSubscriber simula el broker NATS sin levantar nada real.
type mockSubscriber struct {
	ch           chan []byte
	cancelCalled bool
	shouldFail   bool
}

func newMock() *mockSubscriber {
	return &mockSubscriber{ch: make(chan []byte, 8)}
}

func (m *mockSubscriber) Subscribe(_ string) (<-chan []byte, func(), error) {
	if m.shouldFail {
		return nil, nil, fmt.Errorf("error simulado de nats")
	}
	cancel := func() { m.cancelCalled = true }
	return m.ch, cancel, nil
}

// -- Tests --

func TestSSEHandler_CabecerasCorrectas(t *testing.T) {
	mock := newMock()
	h := sse.NewHandler(mock, "*")

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/subscribe?topic=test.events", nil).WithContext(ctx)
	rec := &flusherRecorder{ResponseRecorder: httptest.NewRecorder()}

	done := make(chan struct{})
	go func() {
		h.ServeHTTP(rec, req)
		close(done)
	}()

	// Damos tiempo al handler para escribir las cabeceras y el primer flush.
	time.Sleep(50 * time.Millisecond)
	cancel()
	<-done

	ct := rec.Header().Get("Content-Type")
	if ct != "text/event-stream" {
		t.Errorf("Content-Type incorrecto: %q", ct)
	}
	if rec.Header().Get("Cache-Control") == "" {
		t.Error("falta cabecera Cache-Control")
	}
	if rec.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Error("falta cabecera CORS")
	}
}

func TestSSEHandler_EntregaMensajeSSE(t *testing.T) {
	mock := newMock()
	h := sse.NewHandler(mock, "*")

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/subscribe?topic=alerts", nil).WithContext(ctx)
	rec := &flusherRecorder{ResponseRecorder: httptest.NewRecorder()}

	done := make(chan struct{})
	go func() {
		h.ServeHTTP(rec, req)
		close(done)
	}()

	// Esperamos a que el handler arranque y se suscriba.
	time.Sleep(30 * time.Millisecond)

	// Simulamos un mensaje llegando de NATS.
	mock.ch <- []byte(`{"topic":"alerts","data":{"msg":"incendio"}}`)

	time.Sleep(50 * time.Millisecond)
	cancel()
	<-done

	body := rec.Body.String()
	// El formato SSE exige "data: <payload>\n\n"
	if !strings.Contains(body, "data: {\"topic\":\"alerts\"") {
		t.Errorf("formato SSE incorrecto en la respuesta: %q", body)
	}
}

func TestSSEHandler_CancelacionLlamaCleanup(t *testing.T) {
	mock := newMock()
	h := sse.NewHandler(mock, "*")

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/subscribe?topic=test", nil).WithContext(ctx)
	rec := &flusherRecorder{ResponseRecorder: httptest.NewRecorder()}

	done := make(chan struct{})
	go func() {
		h.ServeHTTP(rec, req)
		close(done)
	}()

	time.Sleep(30 * time.Millisecond)

	// Simulamos que el cliente cierra la conexión.
	cancel()
	<-done

	// El defer del handler DEBE haber llamado a cancel() del mock.
	// Si no lo hizo, la suscripción NATS quedaría viva — el bug más grave que podría tener el gateway.
	if !mock.cancelCalled {
		t.Fatal("el cleanup de la suscripción no fue llamado al desconectarse el cliente")
	}
}

func TestSSEHandler_TopicVacio(t *testing.T) {
	mock := newMock()
	h := sse.NewHandler(mock, "*")

	req := httptest.NewRequest(http.MethodGet, "/subscribe", nil)
	rec := &flusherRecorder{ResponseRecorder: httptest.NewRecorder()}

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("esperaba 400, obtuvo %d", rec.Code)
	}
}

func TestSSEHandler_MetodoNoPermitido(t *testing.T) {
	mock := newMock()
	h := sse.NewHandler(mock, "*")

	req := httptest.NewRequest(http.MethodPost, "/subscribe?topic=test", nil)
	rec := &flusherRecorder{ResponseRecorder: httptest.NewRecorder()}

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("esperaba 405, obtuvo %d", rec.Code)
	}
}

func TestSSEHandler_ErrorBroker(t *testing.T) {
	mock := &mockSubscriber{shouldFail: true}
	h := sse.NewHandler(mock, "*")

	req := httptest.NewRequest(http.MethodGet, "/subscribe?topic=test", nil)
	rec := &flusherRecorder{ResponseRecorder: httptest.NewRecorder()}

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("esperaba 500, obtuvo %d", rec.Code)
	}
}

func TestSSEHandler_PrimerFlushContieneConnected(t *testing.T) {
	mock := newMock()
	h := sse.NewHandler(mock, "*")

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/subscribe?topic=pings", nil).WithContext(ctx)
	rec := &flusherRecorder{ResponseRecorder: httptest.NewRecorder()}

	done := make(chan struct{})
	go func() {
		h.ServeHTTP(rec, req)
		close(done)
	}()

	time.Sleep(40 * time.Millisecond)
	cancel()
	<-done

	// El comentario inicial confirma al EventSource que la conexión está activa.
	if !strings.Contains(rec.Body.String(), ": connected to pings") {
		t.Errorf("falta el comentario de confirmación de conexión en: %q", rec.Body.String())
	}
}

func TestSSEHandler_MultiplesFlushes(t *testing.T) {
	mock := newMock()
	h := sse.NewHandler(mock, "*")

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/subscribe?topic=metrics", nil).WithContext(ctx)
	rec := &flusherRecorder{ResponseRecorder: httptest.NewRecorder()}

	done := make(chan struct{})
	go func() {
		h.ServeHTTP(rec, req)
		close(done)
	}()

	time.Sleep(20 * time.Millisecond)

	// Enviamos 3 mensajes consecutivos.
	mock.ch <- []byte(`{"n":1}`)
	mock.ch <- []byte(`{"n":2}`)
	mock.ch <- []byte(`{"n":3}`)

	time.Sleep(60 * time.Millisecond)
	cancel()
	<-done

	body := rec.Body.String()
	for _, n := range []string{`{"n":1}`, `{"n":2}`, `{"n":3}`} {
		if !strings.Contains(body, n) {
			t.Errorf("mensaje %q no encontrado en la respuesta SSE", n)
		}
	}

	// Cada mensaje más el flush inicial deberían haber generado al menos 4 flushes.
	if rec.flushed < 4 {
		t.Errorf("se esperaban al menos 4 flushes, se hicieron %d", rec.flushed)
	}
}
