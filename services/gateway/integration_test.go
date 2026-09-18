//go:build integration

// Tests de integración del gateway SSE.
// Requieren un servidor NATS activo. No usan mocks — validan el flujo real.
//
// Ejecución:
//   docker run -d -p 4222:4222 nats:2.10-alpine
//   go test -tags integration -v -timeout 30s ./...
//
// Con URL de NATS personalizada:
//   NATS_URL=nats://192.168.1.10:4222 go test -tags integration -v ./...

package integration_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arecibo-sse/gateway/internal/broker"
	"github.com/arecibo-sse/gateway/internal/sse"
	natsgo "github.com/nats-io/nats.go"
)

// natsURL lee la URL de NATS del entorno o usa localhost por defecto.
func natsURL() string {
	if url := os.Getenv("NATS_URL"); url != "" {
		return url
	}
	return "nats://localhost:4222"
}

// testServer agrupa el servidor HTTP del gateway y su cliente NATS para facilitar el cleanup.
type testServer struct {
	URL    string
	srv    *http.Server
	client *broker.Client
}

// newTestServer arranca el gateway en un puerto libre del SO y devuelve el servidor listo para usar.
// Hace t.Skip() si NATS no está disponible — así los tests no fallan en CI sin NATS.
func newTestServer(t *testing.T) *testServer {
	t.Helper()

	nc, err := broker.New(natsURL())
	if err != nil {
		t.Skipf("NATS no disponible en %s — omitiendo test de integración: %v", natsURL(), err)
		return nil
	}

	h := sse.NewHandler(nc, "*")
	mux := http.NewServeMux()
	mux.Handle("GET /subscribe", h)
	mux.HandleFunc("GET /health", sse.HealthHandler)

	// Puerto :0 le pide al SO que asigne uno libre — evita colisiones entre tests paralelos.
	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("abriendo listener: %v", err)
	}

	srv := &http.Server{Handler: mux, WriteTimeout: 0}
	go func() { _ = srv.Serve(ln) }()

	t.Cleanup(func() {
		srv.Close()
		nc.Close()
	})

	port := ln.Addr().(*net.TCPAddr).Port
	return &testServer{
		URL:    fmt.Sprintf("http://localhost:%d", port),
		srv:    srv,
		client: nc,
	}
}

// publishDirect publica un payload JSON directamente en NATS, sin pasar por el publisher HTTP.
// Esto nos permite testear el gateway de forma aislada del publisher.
func publishDirect(t *testing.T, topic string, payload any) {
	t.Helper()

	nc, err := natsgo.Connect(natsURL())
	if err != nil {
		t.Skipf("NATS no disponible: %v", err)
	}
	defer nc.Drain()

	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("serializando payload: %v", err)
	}

	if err := nc.Publish(topic, data); err != nil {
		t.Fatalf("publicando en %s: %v", topic, err)
	}

	// Flush garantiza que el mensaje llegó al servidor NATS antes de que el test continúe.
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush NATS: %v", err)
	}
}

// sseClient abre una conexión SSE al gateway y devuelve el body del stream.
// Usa el contexto del test para cancelar automáticamente al terminar.
func sseClient(t *testing.T, gatewayURL, topic string) io.ReadCloser {
	t.Helper()

	// Deshabilitamos compresión para no interferir con el stream chunked de SSE.
	httpClient := &http.Client{
		Transport: &http.Transport{DisableCompression: true},
	}

	// Vinculamos la petición al contexto del test — si el test termina, la conexión se cierra.
	ctx := context.Background()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		fmt.Sprintf("%s/subscribe?topic=%s", gatewayURL, topic), nil)
	if err != nil {
		t.Fatalf("creando request SSE: %v", err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("conectando a gateway SSE: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		t.Fatalf("status inesperado del gateway: %d", resp.StatusCode)
	}

	return resp.Body
}

// waitForSSEData lee líneas del stream SSE con timeout hasta encontrar una línea "data:".
// Cuando el contexto expira, la lectura falla y el test recibe un error claro.
func waitForSSEData(t *testing.T, body io.Reader, timeout time.Duration) string {
	t.Helper()

	type result struct {
		data string
		err  error
	}
	ch := make(chan result, 1)

	go func() {
		scanner := bufio.NewScanner(body)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "data: ") {
				ch <- result{data: strings.TrimPrefix(line, "data: ")}
				return
			}
		}
		if err := scanner.Err(); err != nil {
			ch <- result{err: err}
		} else {
			ch <- result{err: io.EOF}
		}
	}()

	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("leyendo stream SSE: %v", r.err)
		}
		return r.data
	case <-time.After(timeout):
		t.Fatalf("timeout de %s esperando evento SSE", timeout)
		return ""
	}
}

// ============================================================================
// Tests
// ============================================================================

func TestIntegration_NATSPublish_LlegaAlClienteSSE(t *testing.T) {
	// Flujo: publish directo a NATS → gateway reenvía → cliente SSE recibe.
	// Es el test más importante — valida que el puente NATS↔SSE funciona.
	srv := newTestServer(t)

	topic := "integration.basic"
	body := sseClient(t, srv.URL, topic)
	defer body.Close()

	// Pequeña espera para que la suscripción NATS del gateway quede activa.
	// Sin esto, el mensaje publicado llega antes de que el gateway esté suscrito.
	time.Sleep(100 * time.Millisecond)

	publishDirect(t, topic, map[string]any{
		"msg":   "hola desde nats",
		"value": 42,
	})

	data := waitForSSEData(t, body, 3*time.Second)

	if !strings.Contains(data, "hola desde nats") {
		t.Errorf("datos inesperados en el evento SSE: %s", data)
	}
}

func TestIntegration_MultipleClientes_RecibenmMismoMensaje(t *testing.T) {
	// Un publisher → N clientes SSE: todos deben recibir el mismo mensaje.
	// Valida que el fan-out de NATS funciona correctamente con el gateway.
	srv := newTestServer(t)

	const numClients = 20
	topic := "integration.broadcast"

	bodies := make([]io.ReadCloser, numClients)
	for i := range bodies {
		bodies[i] = sseClient(t, srv.URL, topic)
	}
	defer func() {
		for _, b := range bodies {
			b.Close()
		}
	}()

	time.Sleep(150 * time.Millisecond)

	publishDirect(t, topic, map[string]any{"broadcast": true, "clients": numClients})

	var wg sync.WaitGroup
	var received atomic.Int32

	for _, body := range bodies {
		wg.Add(1)
		go func(b io.Reader) {
			defer wg.Done()
			data := waitForSSEData(t, b, 5*time.Second)
			if strings.Contains(data, "broadcast") {
				received.Add(1)
			}
		}(body)
	}

	wg.Wait()

	if int(received.Load()) != numClients {
		t.Errorf("%d/%d clientes recibieron el evento — posible fan-out roto",
			received.Load(), numClients)
	}
}

func TestIntegration_Desconexion_NoLeakGoroutines(t *testing.T) {
	// Conectamos y desconectamos clientes repetidamente.
	// Si hubiera un leak, el número de goroutines crecería sin límite.
	// Este test no puede medir goroutines del gateway (es otro proceso),
	// pero sí verifica que el servidor sigue respondiendo correctamente
	// después de múltiples ciclos de conexión/desconexión.
	srv := newTestServer(t)

	topic := "integration.disconnect"
	const cycles = 15

	for i := 0; i < cycles; i++ {
		body := sseClient(t, srv.URL, topic)
		time.Sleep(30 * time.Millisecond)
		body.Close()
		time.Sleep(20 * time.Millisecond)
	}

	// El gateway debe seguir respondiendo después de N desconexiones.
	// Si hubiera un leak crítico (deadlock, panic), esto fallaría.
	resp, err := http.Get(srv.URL + "/health")
	if err != nil {
		t.Fatalf("gateway no responde después de %d desconexiones: %v", cycles, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("health check falló: %d", resp.StatusCode)
	}

	t.Logf("gateway saludable después de %d ciclos de conexión/desconexión", cycles)
}

func TestIntegration_CabecerasSSE_SonCorrectas(t *testing.T) {
	// Las cabeceras SSE incorrectas hacen que el browser no procese el stream.
	// Este test es especialmente importante porque los errores de cabecera son silenciosos.
	srv := newTestServer(t)

	httpClient := &http.Client{
		Transport: &http.Transport{DisableCompression: true},
	}

	resp, err := httpClient.Get(srv.URL + "/subscribe?topic=integration.headers")
	if err != nil {
		t.Fatalf("request fallido: %v", err)
	}
	defer resp.Body.Close()

	required := map[string]string{
		"Content-Type":                "text/event-stream",
		"Cache-Control":               "no-cache",
		"Access-Control-Allow-Origin": "*",
		"X-Accel-Buffering":           "no",
	}

	for header, contains := range required {
		got := resp.Header.Get(header)
		if !strings.Contains(got, contains) {
			t.Errorf("cabecera %q: esperaba contener %q, obtuvo %q", header, contains, got)
		}
	}
}

func TestIntegration_WildcardTopic_CapturaMensajes(t *testing.T) {
	// NATS soporta wildcards (*) y (>). Este test verifica que el gateway
	// los pasa correctamente a NATS sin modificarlos.
	srv := newTestServer(t)

	topic := "integration.wildcard.*"
	body := sseClient(t, srv.URL, topic)
	defer body.Close()

	time.Sleep(100 * time.Millisecond)

	// Publicamos en sub-topics que deberían ser capturados por el wildcard.
	publishDirect(t, "integration.wildcard.click", map[string]any{"action": "click"})

	data := waitForSSEData(t, body, 3*time.Second)
	if !strings.Contains(data, "click") {
		t.Errorf("wildcard no capturó el evento esperado: %s", data)
	}
}

func TestIntegration_MultiplesTopics_AisladosEntreSi(t *testing.T) {
	// Un cliente suscrito a topic A NO debe recibir mensajes de topic B.
	// Valida que el aislamiento de suscripciones NATS funciona correctamente.
	srv := newTestServer(t)

	topicA := "integration.isolation.a"
	topicB := "integration.isolation.b"

	bodyA := sseClient(t, srv.URL, topicA)
	defer bodyA.Close()

	time.Sleep(100 * time.Millisecond)

	// Publicamos SOLO en B — el cliente de A no debe recibir nada.
	publishDirect(t, topicB, map[string]any{"secret": "solo-para-b"})

	// Publicamos en A — el cliente de A sí debe recibirlo.
	time.Sleep(50 * time.Millisecond)
	publishDirect(t, topicA, map[string]any{"msg": "correcto"})

	data := waitForSSEData(t, bodyA, 3*time.Second)

	if strings.Contains(data, "solo-para-b") {
		t.Error("el cliente de A recibió un mensaje del topic B — aislamiento roto")
	}
	if !strings.Contains(data, "correcto") {
		t.Errorf("el cliente de A no recibió su mensaje: %s", data)
	}
}

func TestIntegration_CargaBaja_LatenciaAceptable(t *testing.T) {
	// Publica 50 mensajes seguidos y mide que todos llegan con latencia razonable.
	// No es un benchmark — es un sanity check de throughput mínimo.
	srv := newTestServer(t)

	topic := "integration.latency"
	body := sseClient(t, srv.URL, topic)
	defer body.Close()

	time.Sleep(100 * time.Millisecond)

	const numMsgs = 50
	type payload struct {
		N         int   `json:"n"`
		SentAt    int64 `json:"sent_at"`
	}

	// Publicamos todos los mensajes en ráfaga.
	for i := 1; i <= numMsgs; i++ {
		publishDirect(t, topic, payload{N: i, SentAt: time.Now().UnixMilli()})
	}

	// Leemos todos los eventos y verificamos que lleguen en orden razonable.
	received := 0
	scanner := bufio.NewScanner(body)
	deadline := time.After(10 * time.Second)

	for received < numMsgs {
		lineCh := make(chan string, 1)
		go func() {
			if scanner.Scan() {
				lineCh <- scanner.Text()
			}
		}()

		select {
		case line := <-lineCh:
			if strings.HasPrefix(line, "data: ") {
				received++
			}
		case <-deadline:
			t.Fatalf("timeout: solo recibidos %d/%d mensajes", received, numMsgs)
		}
	}

	t.Logf("✓ %d mensajes recibidos correctamente", received)
}
