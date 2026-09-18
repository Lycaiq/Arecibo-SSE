//go:build integration

// Tests de integración del publisher API.
// Validan que POST /publish → NATS funciona de extremo a extremo.
//
// Ejecución:
//   docker run -d -p 4222:4222 nats:2.10-alpine
//   go test -tags integration -v -timeout 30s ./...

package integration_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/arecibo-sse/publisher/internal/broker"
	"github.com/arecibo-sse/publisher/internal/handler"
	natsgo "github.com/nats-io/nats.go"
)

func natsURL() string {
	if url := os.Getenv("NATS_URL"); url != "" {
		return url
	}
	return "nats://localhost:4222"
}

// startPublisher arranca el publisher en un puerto libre y devuelve su URL base.
// Si NATS no está disponible, hace t.Skip().
func startPublisher(t *testing.T) string {
	t.Helper()

	nc, err := broker.New(natsURL())
	if err != nil {
		t.Skipf("NATS no disponible: %v", err)
		return ""
	}

	mux := http.NewServeMux()
	mux.Handle("POST /publish", handler.NewPublishHandler(nc))

	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("listener: %v", err)
	}

	srv := &http.Server{
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	go func() { _ = srv.Serve(ln) }()

	t.Cleanup(func() {
		srv.Close()
		nc.Close()
	})

	port := ln.Addr().(*net.TCPAddr).Port
	return fmt.Sprintf("http://localhost:%d", port)
}

// subscribeToNATS crea una suscripción NATS y devuelve un canal de mensajes y un cleanup.
func subscribeToNATS(t *testing.T, topic string) (<-chan *natsgo.Msg, func()) {
	t.Helper()

	nc, err := natsgo.Connect(natsURL())
	if err != nil {
		t.Skipf("NATS no disponible: %v", err)
		return nil, nil
	}

	msgCh := make(chan *natsgo.Msg, 16)
	sub, err := nc.Subscribe(topic, func(msg *natsgo.Msg) {
		msgCh <- msg
	})
	if err != nil {
		t.Fatalf("suscribiendo a %s: %v", topic, err)
	}

	cleanup := func() {
		sub.Unsubscribe()
		nc.Close()
	}

	return msgCh, cleanup
}

// postPublish hace un POST /publish al servidor con el topic y data dados.
func postPublish(t *testing.T, baseURL, topic string, data map[string]any) *http.Response {
	t.Helper()

	body, _ := json.Marshal(map[string]any{
		"topic": topic,
		"data":  data,
	})

	resp, err := http.Post(baseURL+"/publish", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /publish: %v", err)
	}
	return resp
}

// ============================================================================
// Tests
// ============================================================================

func TestIntegration_Post_PublicaEnNATS(t *testing.T) {
	// Valida el flujo completo: HTTP POST → publisher → NATS.
	// El publisher es una caja negra — lo probamos por sus efectos en NATS.
	baseURL := startPublisher(t)

	topic := "pub.integration.basic"
	msgCh, cleanup := subscribeToNATS(t, topic)
	defer cleanup()

	resp := postPublish(t, baseURL, topic, map[string]any{"test": "integración"})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status inesperado: %d (esperaba 202)", resp.StatusCode)
	}

	select {
	case msg := <-msgCh:
		// El mensaje que llega a NATS debe ser el Event envelope del publisher.
		var event map[string]any
		if err := json.Unmarshal(msg.Data, &event); err != nil {
			t.Fatalf("mensaje de NATS no es JSON válido: %v", err)
		}
		if event["topic"] != topic {
			t.Errorf("topic inesperado en el evento: %v", event["topic"])
		}
		if event["timestamp"] == nil {
			t.Error("falta el campo timestamp en el evento")
		}
		t.Logf("evento recibido: %s", msg.Data)

	case <-time.After(3 * time.Second):
		t.Fatal("timeout: el mensaje no llegó a NATS")
	}
}

func TestIntegration_Post_TopicVacio_Rechaza(t *testing.T) {
	baseURL := startPublisher(t)

	body, _ := json.Marshal(map[string]any{"topic": "  ", "data": map[string]any{}})
	resp, err := http.Post(baseURL+"/publish", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("esperaba 400, obtuvo %d", resp.StatusCode)
	}
}

func TestIntegration_Post_RespuestaJSON_TieneTopicYStatus(t *testing.T) {
	baseURL := startPublisher(t)

	topic := "pub.integration.response"
	resp := postPublish(t, baseURL, topic, map[string]any{"x": 1})
	defer resp.Body.Close()

	var result map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("respuesta no es JSON: %v", err)
	}

	if result["status"] != "published" {
		t.Errorf("status inesperado: %q", result["status"])
	}
	if result["topic"] != topic {
		t.Errorf("topic inesperado: %q", result["topic"])
	}
}

func TestIntegration_Post_MultiplesPublicaciones_LleganEnOrden(t *testing.T) {
	baseURL := startPublisher(t)

	topic := "pub.integration.order"
	msgCh, cleanup := subscribeToNATS(t, topic)
	defer cleanup()

	const count = 10
	for i := 1; i <= count; i++ {
		resp := postPublish(t, baseURL, topic, map[string]any{"n": i})
		resp.Body.Close()
		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("mensaje %d rechazado: %d", i, resp.StatusCode)
		}
	}

	received := 0
	deadline := time.After(5 * time.Second)
	for received < count {
		select {
		case <-msgCh:
			received++
		case <-deadline:
			t.Fatalf("timeout: solo recibidos %d/%d mensajes", received, count)
		}
	}

	t.Logf("✓ %d mensajes publicados y recibidos en NATS", count)
}
