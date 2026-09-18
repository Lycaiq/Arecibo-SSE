// Herramienta de prueba de estrés para el SSE Gateway.
//
// Abre N conexiones SSE concurrentes y publica M eventos a velocidad controlada,
// midiendo throughput, latencia y uso de memoria del proceso local.
//
// Uso:
//   go run ./scripts/stress/main.go [flags]
//
// Flags:
//   -gateway    URL del gateway SSE        (default: http://localhost:8081)
//   -publisher  URL del publisher API      (default: http://localhost:8080)
//   -clients    Número de clientes SSE     (default: 100)
//   -events     Eventos a publicar         (default: 200)
//   -rate       Eventos por segundo        (default: 20)
//   -topic      Topic NATS a usar          (default: stress.test)
//
// Ejemplo rápido:
//   go run ./scripts/stress/main.go -clients 500 -events 1000 -rate 50

package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// config agrupa todos los parámetros de la prueba.
type config struct {
	gatewayURL  string
	publisherURL string
	clients     int
	events      int
	rate        int
	topic       string
}

// stats acumula métricas de la prueba de forma thread-safe.
type stats struct {
	connected     atomic.Int64
	connectErrors atomic.Int64
	eventsRecv    atomic.Int64
	latencySum    atomic.Int64 // suma de latencias en ms
	latencyCount  atomic.Int64
}

func main() {
	cfg := parseFlags()

	fmt.Printf("\n🚀 Arecibo-SSE Stress Test\n")
	fmt.Printf("   Gateway:    %s\n", cfg.gatewayURL)
	fmt.Printf("   Publisher:  %s\n", cfg.publisherURL)
	fmt.Printf("   Clientes:   %d SSE connections\n", cfg.clients)
	fmt.Printf("   Eventos:    %d @ %d eventos/seg\n", cfg.events, cfg.rate)
	fmt.Printf("   Topic:      %s\n\n", cfg.topic)

	st := &stats{}
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)
	goroutinesBefore := runtime.NumGoroutine()

	start := time.Now()

	// Lanzamos todos los clientes SSE en paralelo.
	var clientsReady sync.WaitGroup
	var allDone sync.WaitGroup

	for i := 0; i < cfg.clients; i++ {
		allDone.Add(1)
		go func(id int) {
			defer allDone.Done()
			runSSEClient(cfg, st, id, cfg.events, &clientsReady)
		}(i)
	}

	// Esperamos a que todos los clientes estén conectados antes de publicar.
	waitWithTimeout(&clientsReady, 15*time.Second, "esperando conexiones SSE")

	connected := int(st.connected.Load())
	fmt.Printf("✅ %d/%d clientes conectados\n\n", connected, cfg.clients)

	if connected == 0 {
		fmt.Println("❌ No se pudo conectar ningún cliente. ¿Están corriendo los servicios?")
		fmt.Println("   Levántalos con: docker compose up")
		os.Exit(1)
	}

	// Publicamos eventos a la velocidad configurada.
	interval := time.Second / time.Duration(cfg.rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	fmt.Printf("📤 Publicando %d eventos...\n", cfg.events)

	for i := 0; i < cfg.events; i++ {
		<-ticker.C
		go publishEvent(cfg, i)
	}

	// Esperamos a que todos los clientes terminen de recibir (con timeout).
	done := make(chan struct{})
	go func() {
		allDone.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		fmt.Println("\n⚠️  Timeout: algunos clientes no terminaron a tiempo.")
	}

	elapsed := time.Since(start)

	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)
	goroutinesAfter := runtime.NumGoroutine()

	printReport(cfg, st, elapsed, memBefore, memAfter, goroutinesBefore, goroutinesAfter)
}

// runSSEClient abre una conexión SSE, espera los eventos esperados y cierra.
func runSSEClient(cfg config, st *stats, id, expectedEvents int, ready *sync.WaitGroup) {
	url := fmt.Sprintf("%s/subscribe?topic=%s", cfg.gatewayURL, cfg.topic)

	httpClient := &http.Client{
		Transport: &http.Transport{DisableCompression: true},
		Timeout:   0, // sin timeout — conexiones de larga duración
	}

	resp, err := httpClient.Get(url)
	if err != nil {
		st.connectErrors.Add(1)
		ready.Done()
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		st.connectErrors.Add(1)
		ready.Done()
		return
	}

	st.connected.Add(1)
	ready.Done() // señal: este cliente está conectado

	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}

		st.eventsRecv.Add(1)

		// Calculamos latencia si el payload tiene sent_at.
		data := strings.TrimPrefix(line, "data: ")
		var payload struct {
			Data struct {
				SentAt int64 `json:"sent_at"`
			} `json:"data"`
		}
		if err := json.Unmarshal([]byte(data), &payload); err == nil && payload.Data.SentAt > 0 {
			latency := time.Now().UnixMilli() - payload.Data.SentAt
			st.latencySum.Add(latency)
			st.latencyCount.Add(1)
		}

		// Si ya recibimos suficientes eventos, cerramos.
		// Esto permite que el test termine sin esperar al timeout.
		if st.eventsRecv.Load() >= int64(cfg.clients*expectedEvents) {
			return
		}
	}
}

// publishEvent publica un evento de estrés con timestamp para medir latencia.
func publishEvent(cfg config, n int) {
	payload, _ := json.Marshal(map[string]any{
		"topic": cfg.topic,
		"data": map[string]any{
			"n":       n,
			"sent_at": time.Now().UnixMilli(),
			"source":  "stress-test",
		},
	})

	resp, err := http.Post(cfg.publisherURL+"/publish", "application/json",
		bytes.NewReader(payload))
	if err != nil {
		return
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
}

// printReport imprime el resumen de la prueba de forma legible.
func printReport(
	cfg config,
	st *stats,
	elapsed time.Duration,
	memBefore, memAfter runtime.MemStats,
	goroutinesBefore, goroutinesAfter int,
) {
	eventsRecv := st.eventsRecv.Load()
	expectedTotal := int64(cfg.clients) * int64(cfg.events)
	deliveryRate := float64(eventsRecv) / float64(expectedTotal) * 100

	throughput := float64(eventsRecv) / elapsed.Seconds()

	var avgLatency float64
	if count := st.latencyCount.Load(); count > 0 {
		avgLatency = float64(st.latencySum.Load()) / float64(count)
	}

	allocDelta := int64(memAfter.TotalAlloc) - int64(memBefore.TotalAlloc)

	fmt.Printf("\n%s\n", strings.Repeat("─", 50))
	fmt.Printf("📊 RESULTADOS\n")
	fmt.Printf("%s\n", strings.Repeat("─", 50))
	fmt.Printf("  Duración total:     %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("  Clientes conectados: %d / %d (errores: %d)\n",
		st.connected.Load(), cfg.clients, st.connectErrors.Load())
	fmt.Printf("  Eventos publicados:  %d\n", cfg.events)
	fmt.Printf("  Eventos recibidos:   %d / %d (%.1f%%)\n",
		eventsRecv, expectedTotal, deliveryRate)
	fmt.Printf("  Throughput:          %.0f eventos/seg\n", throughput)

	if avgLatency > 0 {
		fmt.Printf("  Latencia promedio:   %.1f ms\n", avgLatency)
	}

	fmt.Printf("  Goroutines (Δ):      %+d (%d → %d)\n",
		goroutinesAfter-goroutinesBefore, goroutinesBefore, goroutinesAfter)
	fmt.Printf("  Allocations:         %.2f MB\n", float64(allocDelta)/1024/1024)

	fmt.Printf("%s\n\n", strings.Repeat("─", 50))

	if deliveryRate < 95 {
		fmt.Printf("⚠️  Tasa de entrega baja (%.1f%%). Posible saturación del gateway.\n\n", deliveryRate)
	} else {
		fmt.Printf("✅ Prueba completada correctamente.\n\n")
	}
}

// waitWithTimeout espera a que el WaitGroup llegue a cero con un timeout máximo.
func waitWithTimeout(wg *sync.WaitGroup, timeout time.Duration, msg string) {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-done:
	case <-timer.C:
		fmt.Printf("⚠️  Timeout %s esperando: %s\n", timeout, msg)
	}
}

func parseFlags() config {
	cfg := config{}
	flag.StringVar(&cfg.gatewayURL,   "gateway",   "http://localhost:8081", "URL del gateway SSE")
	flag.StringVar(&cfg.publisherURL, "publisher", "http://localhost:8080", "URL del publisher API")
	flag.IntVar(&cfg.clients,         "clients",   100,                     "Número de clientes SSE")
	flag.IntVar(&cfg.events,          "events",    200,                     "Número de eventos a publicar")
	flag.IntVar(&cfg.rate,            "rate",      20,                      "Eventos por segundo")
	flag.StringVar(&cfg.topic,        "topic",     "stress.test",           "Topic NATS")
	flag.Parse()
	return cfg
}
