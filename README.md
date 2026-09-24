# 📡 Arecibo-SSE

Sistema de notificaciones en tiempo real de alto rendimiento que utiliza **Server-Sent Events (SSE)** y **NATS** como alternativa ligera a WebSockets para flujos de datos unidireccionales (Backend → Frontend).

Inspirado en las transmisiones históricas del radiotelescopio de Arecibo hacia el cosmos: un emisor potente, muchos receptores, señal unidireccional.

---

## ¿Qué hace este proyecto?

Arecibo-SSE resuelve el problema de enviar notificaciones del servidor al navegador a escala, sin la complejidad de WebSockets ni la latencia del polling. El flujo es simple:

```
[Cualquier servicio]
        │
        │  POST /publish  {"topic": "alerts.critical", "data": {...}}
        ▼
┌─────────────────┐      publica      ┌──────────┐     suscribe     ┌─────────────────┐
│  Publisher API  │ ────────────────▶ │   NATS   │ ◀────────────── │   SSE Gateway   │
│   (Go :8080)    │                   │  :4222   │                  │   (Go :8081)    │
└─────────────────┘                   └──────────┘                  └────────┬────────┘
                                                                             │
                                                          text/event-stream  │
                                                                             ▼
                                                                  ┌─────────────────┐
                                                                  │  React Frontend │
                                                                  │  (Vite :5173)   │
                                                                  └─────────────────┘
```

---

## ¿Qué es SSE?

**Server-Sent Events** es una tecnología web estándar (parte de HTML5) que permite al servidor enviar datos al navegador a través de una conexión HTTP normal que se mantiene abierta.

La idea es simple: el browser hace un `GET` y en lugar de recibir una respuesta completa y cerrar la conexión, la deja abierta indefinidamente. El servidor va escribiendo datos en esa conexión a medida que ocurren eventos, y el browser los procesa en tiempo real.

### Cómo funciona en el navegador

El browser tiene una API nativa llamada `EventSource` que abstrae todo esto:

```javascript
// El browser abre una conexión HTTP y la mantiene viva
const es = new EventSource('/subscribe?topic=alerts');

// Cada vez que el servidor envía un evento, esta función se ejecuta
es.onmessage = (event) => {
  const data = JSON.parse(event.data);
  console.log('Nuevo evento:', data);
};

// Si la conexión se corta (red inestable, servidor reiniciado),
// el browser reconecta automáticamente. Sin código extra.
es.onerror = (err) => {
  console.log('Reconectando...');
};
```

El formato que viaja por la red es texto plano muy simple:

```
data: {"topic":"alerts","data":{"msg":"servidor caído"},"timestamp":"2026-09-22T03:00:00Z"}

data: {"topic":"alerts","data":{"msg":"servidor recuperado"},"timestamp":"2026-09-22T03:01:00Z"}

: keepalive

```

Cada evento es una línea que empieza con `data:`, seguida de una línea en blanco que marca el fin del evento. Los comentarios (líneas con `:`) son ignorados por el browser pero mantienen la conexión TCP activa.

### SSE vs WebSockets — ¿Por qué SSE?

| Característica | SSE | WebSockets |
|---|---|---|
| **Dirección** | Servidor → Cliente | Bidireccional |
| **Protocolo** | HTTP/1.1 y HTTP/2 nativo | Upgrade a protocolo WS |
| **Reconexión** | Automática (built-in) | Manual |
| **Infraestructura** | Funciona con nginx, CDN, proxies estándar | Requiere soporte WS explícito |
| **Complejidad** | Baja — es HTTP normal | Alta — nuevo protocolo |
| **HTTP/2** | Multiplexado (miles de streams por conexión TCP) | No aplica |
| **Caso de uso ideal** | Feeds, notificaciones, dashboards | Chat, juegos, colaboración en tiempo real |

**SSE es la elección correcta cuando el servidor habla y el cliente escucha.** Para Arecibo-SSE, donde los eventos solo fluyen del backend al browser, SSE elimina complejidad sin sacrificar capacidad.

### SSE como fallback cuando WebSocket falla

Hay entornos donde WebSocket simplemente no funciona:

- **Proxies corporativos** — muchos proxies HTTP de empresas bloquean o transforman el `Upgrade: websocket` y rompen el handshake silenciosamente.
- **Infraestructura vieja** — algunos CDNs, balanceadores o firewalls no soportan WS y cierran la conexión sin avisar.
- **Redes restrictivas** — en hoteles, aeropuertos o VPNs, los puertos 80/443 funcionan pero WS falla.

En todos esos casos el frontend puede degradar a SSE para seguir recibiendo notificaciones, aunque pierda la capacidad bidireccional. Para un sistema de notificaciones (servidor → cliente) eso es suficiente — el usuario sigue recibiendo alertas sin notar nada.

**Flujo de decisión en el cliente:**

```
Frontend arranca
       │
       ▼
Intenta conectar WebSocket (ws://...)
       │
   ¿Conectó? ──── Sí ──▶ Usa WebSocket normalmente
       │
      No (error / timeout)
       │
       ▼
Fallback: abre EventSource (GET /subscribe?topic=...)
       │
   ¿Conectó? ──── Sí ──▶ Recibe notificaciones vía SSE
       │                  (solo lectura, sin envío desde el cliente)
      No
       │
       ▼
Muestra error de conectividad al usuario
```

**Ejemplo de implementación del patrón fallback:**

```typescript
// hooks/useRealtimeNotifications.ts
//
// Intenta WebSocket primero. Si falla en los primeros 3 segundos,
// cae automáticamente a SSE para seguir recibiendo eventos del servidor.

import { useEffect, useState } from 'react';

type Transport = 'websocket' | 'sse' | 'error' | 'connecting';

export function useRealtimeNotifications(topic: string) {
  const [messages, setMessages]   = useState<unknown[]>([]);
  const [transport, setTransport] = useState<Transport>('connecting');

  useEffect(() => {
    let ws: WebSocket | null = null;
    let es: EventSource | null = null;
    // Si WebSocket no levanta en 3s, asumimos que el entorno lo bloquea.
    const wsTimeout = setTimeout(() => fallbackToSSE(), 3000);

    function handleMessage(data: string) {
      try {
        setMessages(prev => [JSON.parse(data), ...prev].slice(0, 100));
      } catch { /* mensaje no-JSON, ignorar */ }
    }

    function fallbackToSSE() {
      ws?.close();
      es = new EventSource(`/subscribe?topic=${topic}`);
      es.onopen    = () => setTransport('sse');
      es.onmessage = (e) => handleMessage(e.data);
      es.onerror   = () => setTransport('error');
    }

    // --- Intento 1: WebSocket ---
    try {
      ws = new WebSocket(`wss://${location.host}/ws?topic=${topic}`);

      ws.onopen = () => {
        clearTimeout(wsTimeout); // WS funcionó, cancelamos el fallback
        setTransport('websocket');
      };

      ws.onmessage = (e) => handleMessage(e.data);

      ws.onerror = () => {
        clearTimeout(wsTimeout);
        fallbackToSSE(); // fallo inmediato → SSE ahora
      };
    } catch {
      // WebSocket no disponible (ej. HTTP sin TLS en producción)
      clearTimeout(wsTimeout);
      fallbackToSSE();
    }

    return () => {
      clearTimeout(wsTimeout);
      ws?.close();
      es?.close();
    };
  }, [topic]);

  return { messages, transport };
}
```

En el componente, el `transport` permite mostrarle al usuario qué conexión está activa — útil para depuración o para informar en entornos donde WS no funciona:

```tsx
const { messages, transport } = useRealtimeNotifications('alerts.critical');

// transport === 'websocket' → conexión completa
// transport === 'sse'       → modo lectura, notificaciones funcionando
// transport === 'error'     → sin conexión
```

> **Nota de diseño**: este patrón es relevante cuando tu backend tiene WebSocket para envío bidireccional (ej. el usuario puede responder o interactuar desde el frontend) pero quieres garantizar que al menos las notificaciones lleguen en cualquier red. Si tu caso de uso es solo recibir datos del servidor — como en Arecibo-SSE — SSE directamente es la solución más simple y no necesitas el fallback.

---

## Arquitectura detallada

```
┌─────────────────────────────────────────────────────────────────────┐
│                         MONOREPOSITORIO                             │
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────┐  │
│  │              services/publisher  (Go)                        │  │
│  │                                                              │  │
│  │   POST /publish                                              │  │
│  │   ────────────▶  handler.PublishHandler                     │  │
│  │                        │                                     │  │
│  │                        ▼                                     │  │
│  │                  broker.Client.Publish(topic, event)        │  │
│  │                        │                                     │  │
│  └────────────────────────┼─────────────────────────────────────┘  │
│                           │                                         │
│                    NATS publish                                     │
│                           │                                         │
│                           ▼                                         │
│                   ┌──────────────┐                                  │
│                   │  NATS Server │  nats://nats:4222               │
│                   │  (Docker)    │                                  │
│                   └──────┬───────┘                                  │
│                          │                                          │
│                   NATS subscribe                                    │
│                          │                                          │
│  ┌───────────────────────┼─────────────────────────────────────┐  │
│  │           services/gateway  (Go)                             │  │
│  │                       │                                      │  │
│  │              broker.Client.Subscribe(topic)                  │  │
│  │              ──────────────────────────────▶ chan []byte     │  │
│  │                                                    │         │  │
│  │              sse.Handler  (por cada conexión SSE)  │         │  │
│  │              ┌─────────────────────────────────┐   │         │  │
│  │              │  for { select {                  │   │         │  │
│  │              │    case data := ←msgCh:         │◀──┘         │  │
│  │              │      fmt.Fprintf(w,"data:...\n")│             │  │
│  │              │      flusher.Flush()             │             │  │
│  │              │    case ←ctx.Done():             │             │  │
│  │              │      defer cancel() // ← crítico │             │  │
│  │              │      return                      │             │  │
│  │              │    case ←keepAlive.C:            │             │  │
│  │              │      fmt.Fprintf(w,":keepalive") │             │  │
│  │              │  } }                             │             │  │
│  │              └─────────────────────────────────┘             │  │
│  └─────────────────────────────────────────────────────────────┘  │
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────┐  │
│  │              web/  (React + Vite)                            │  │
│  │                                                              │  │
│  │   useSSE(topic)                                             │  │
│  │   ┌─────────────────────────────────────────────────┐      │  │
│  │   │  const es = new EventSource('/subscribe?topic') │      │  │
│  │   │  es.onmessage = (e) => setEvents(prev =>        │      │  │
│  │   │    [JSON.parse(e.data), ...prev].slice(0, 200)) │      │  │
│  │   │  return () => es.close()  // cleanup en unmount │      │  │
│  │   └─────────────────────────────────────────────────┘      │  │
│  └─────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

### Decisiones de diseño críticas para rendimiento

**Gateway SSE — el cuello de botella más importante:**

1. **Un `go` por conexión SSE, no uno por mensaje.** Cada cliente HTTP tiene su propio goroutine que vive mientras dura la conexión. Go puede manejar cientos de miles de goroutines sin problema.

2. **`defer cancel()` es obligatorio.** Cada suscripción NATS consume recursos en el servidor NATS. Si el cliente se desconecta sin cancelar, la suscripción queda viva para siempre. El `defer` garantiza que el cleanup ocurre sin importar cómo termine la función.

3. **Non-blocking send en el callback de NATS.** El dispatcher de NATS es un goroutine compartido entre todas las suscripciones de la misma conexión. Si el callback bloquea (porque el cliente SSE es lento), paraliza a todos los demás clientes. El `select/default` descarta el mensaje en vez de bloquear.

4. **`WriteTimeout: 0` en el servidor HTTP del gateway.** Un timeout aquí mataría conexiones SSE activas. Las conexiones SSE son de larga duración por diseño.

5. **Keepalive cada 25 segundos.** La mayoría de proxies y load balancers cortan conexiones idle después de 30-60 segundos. El comentario SSE (`: keepalive`) mantiene la conexión viva y detecta clientes muertos cuando el write falla.

---

## Estructura del repositorio

```
Arecibo-SSE/
├── docker-compose.yml              # Stack completo: NATS + Publisher + Gateway
├── .env.example                    # Plantilla de variables de entorno
├── scripts/
│   ├── dev.sh                      # Arranque local sin Docker
│   ├── e2e_test.sh                 # Smoke test E2E con curl
│   └── stress/
│       └── main.go                 # Herramienta de prueba de carga
├── services/
│   ├── publisher/                  # Microservicio: recibe HTTP, publica en NATS
│   │   ├── Dockerfile              # Multi-stage, imagen final ~10MB (scratch)
│   │   ├── go.mod
│   │   ├── cmd/main.go             # Punto de entrada, shutdown graceful
│   │   ├── integration_test.go     # Tests con NATS real (build tag: integration)
│   │   └── internal/
│   │       ├── config/config.go    # Variables de entorno
│   │       ├── broker/nats.go      # Cliente NATS con reconexión infinita
│   │       └── handler/
│   │           ├── publish.go      # POST /publish → NATS
│   │           └── publish_test.go # 6 tests unitarios (sin NATS)
│   └── gateway/                    # Microservicio: suscribe NATS, sirve SSE
│       ├── Dockerfile              # Multi-stage, imagen final ~10MB (scratch)
│       ├── go.mod
│       ├── cmd/main.go             # Punto de entrada, WriteTimeout=0
│       ├── integration_test.go     # 7 tests con NATS real (build tag: integration)
│       └── internal/
│           ├── config/config.go    # Variables de entorno
│           ├── broker/nats.go      # Subscribe() → chan + cancel()
│           └── sse/
│               ├── handler.go      # El corazón del sistema
│               └── handler_test.go # 7 tests unitarios con -race
└── web/                            # Frontend React + TypeScript + Vite
    ├── vite.config.ts              # Proxy: /subscribe → :8081, /publish → :8080
    └── src/
        ├── types.ts                # SSEEventData, DisplayEvent, ConnectionStatus
        ├── hooks/
        │   ├── useSSE.ts           # EventSource lifecycle + cleanup
        │   └── usePublish.ts       # POST /publish con feedback
        └── components/
            ├── StatusBadge.tsx     # Badge animado de estado de conexión
            ├── EventCard.tsx       # Tarjeta de evento con color por topic
            └── Controls.tsx        # Formulario de topic + botones
```

---

## Requisitos

- **Docker** y **Docker Compose** (para el stack completo)
- **Go 1.22+** (para desarrollo local)
- **Node.js 18+** (para el frontend)

---

## Inicio rápido con Docker

```bash
# 1. Clonar el repositorio
git clone https://github.com/Lycaiq/Arecibo-SSE.git
cd Arecibo-SSE

# 2. Levantar el stack completo
docker compose up --build

# 3. Abrir el frontend en el navegador
open http://localhost:5173   # solo si usas npm run dev
```

Los servicios quedan disponibles en:
- **NATS**: `nats://localhost:4222` · Monitoreo: `http://localhost:8222`
- **Publisher API**: `http://localhost:8080`
- **SSE Gateway**: `http://localhost:8081`

---

## Desarrollo local (sin Docker)

```bash
# Terminal 1 — NATS (único componente que sí necesita Docker)
docker run -d --rm -p 4222:4222 -p 8222:8222 nats:2.10-alpine --http_port 8222

# Terminal 2 — Publisher + Gateway en paralelo
./scripts/dev.sh

# Terminal 3 — Frontend con hot reload
cd web && npm run dev
# → http://localhost:5173
```

---

## API Reference

### Publisher — `POST /publish`

Publica un evento en un topic NATS. Cualquier cliente SSE suscrito al topic lo recibirá en tiempo real.

**Request:**
```json
{
  "topic": "alerts.critical",
  "data": {
    "message": "Servidor caído",
    "severity": "high",
    "service": "api-gateway"
  }
}
```

**Response `202 Accepted`:**
```json
{
  "status": "published",
  "topic": "alerts.critical"
}
```

**Errores:**
| Código | Causa |
|---|---|
| `400` | Body malformado o topic vacío |
| `405` | Método distinto de POST |
| `500` | Error al conectar con NATS |

---

### Gateway — `GET /subscribe`

Abre una conexión SSE. El cliente recibe eventos en tiempo real mientras la conexión esté activa.

**Query param:** `topic` (requerido) — topic NATS a escuchar. Soporta wildcards de NATS (`*`, `>`).

```
GET /subscribe?topic=alerts.critical
GET /subscribe?topic=alerts.*
GET /subscribe?topic=events.>
```

**Stream de respuesta** (`200 OK`, `Content-Type: text/event-stream`):
```
: connected to alerts.critical

data: {"topic":"alerts.critical","data":{"message":"Servidor caído"},"timestamp":"2026-09-17T20:00:00Z"}

: keepalive

data: {"topic":"alerts.critical","data":{"message":"Recuperado"},"timestamp":"2026-09-17T20:01:00Z"}
```

**Cabeceras de respuesta:**
```
Content-Type: text/event-stream
Cache-Control: no-cache, no-store, must-revalidate
Connection: keep-alive
X-Accel-Buffering: no
Access-Control-Allow-Origin: *
```

---

### Health Checks

```bash
# Publisher
curl http://localhost:8080/health
# {"status":"ok","service":"publisher","timestamp":1726610400}

# Gateway
curl http://localhost:8081/health
# {"status":"ok","service":"gateway","timestamp":1726610400}
```

---

## Guía de testing

### Tests unitarios (sin dependencias externas)

```bash
# Gateway — 7 tests con race detector
cd services/gateway
go test ./internal/... -race -v

# Publisher — 6 tests con race detector
cd services/publisher
go test ./internal/... -race -v
```

### Tests de integración (requieren NATS)

```bash
# Levantar NATS
docker run -d --rm -p 4222:4222 nats:2.10-alpine

# Correr tests de integración
cd services/gateway
go test -tags integration -v -timeout 30s ./...

cd services/publisher
go test -tags integration -v -timeout 30s ./...
```

Los tests de integración validan:
- ✅ Mensaje publicado en NATS → llega al cliente SSE
- ✅ 20 clientes simultáneos reciben el mismo mensaje (fan-out)
- ✅ Al desconectar, el gateway sigue respondiendo (no leak)
- ✅ Cabeceras SSE correctas
- ✅ Wildcards NATS (`alerts.*`) funcionan
- ✅ Topics aislados entre sí
- ✅ 50 mensajes en ráfaga llegan todos

### Smoke test E2E (requiere stack completo)

```bash
docker compose up -d
./scripts/e2e_test.sh
```

Salida esperada:
```
🔬 Arecibo-SSE E2E Smoke Test
   Gateway:   http://localhost:8081
   Publisher: http://localhost:8080
   Topic:     e2e.smoke.test

── Test 1: Gateway health check
  ✅ Gateway responde con 200
── Test 2: Publisher health check
  ✅ Publisher responde con 200
── Test 3: Cabeceras SSE
  ✅ Content-Type: text/event-stream presente
  ✅ X-Accel-Buffering: no presente
  ✅ Cabecera CORS presente
── Test 4: Flujo completo SSE (timeout: 10s)
  ✅ Evento publicado (202 Accepted)
  ✅ Evento recibido por el cliente SSE con test_id=e2e-1726610400
── Test 5: Validación de topic vacío (publisher)
  ✅ Publisher rechaza topic vacío con 400

──────────────────────────────────
  ✅ Pasaron: 7
  ❌ Fallaron: 0
──────────────────────────────────

🎉 Todos los tests pasaron.
```

### Prueba de estrés

```bash
docker compose up -d

# Escenario básico: 100 clientes, 200 eventos
go run ./scripts/stress/main.go

# Escenario medio: 500 clientes, 1000 eventos a 50 eventos/seg
go run ./scripts/stress/main.go -clients 500 -events 1000 -rate 50

# Escenario exigente: 2000 clientes, 500 eventos
go run ./scripts/stress/main.go -clients 2000 -events 500 -rate 100 -topic alerts.critical
```

Salida de ejemplo:
```
🚀 Arecibo-SSE Stress Test
   Gateway:    http://localhost:8081
   Publisher:  http://localhost:8080
   Clientes:   500 SSE connections
   Eventos:    1000 @ 50 eventos/seg

✅ 500/500 clientes conectados

📤 Publicando 1000 eventos...

──────────────────────────────────────────────────
📊 RESULTADOS
──────────────────────────────────────────────────
  Duración total:      21.4s
  Clientes conectados: 500 / 500 (errores: 0)
  Eventos publicados:  1000
  Eventos recibidos:   499850 / 500000 (99.9%)
  Throughput:          23357 eventos/seg
  Latencia promedio:   4.2 ms
  Goroutines (Δ):      +3 (12 → 15)
  Allocations:         48.21 MB
──────────────────────────────────────────────────

✅ Prueba completada correctamente.
```

---

## Configuración

Copia `.env.example` a `.env` y ajusta los valores:

```bash
cp .env.example .env
```

| Variable | Servicio | Default | Descripción |
|---|---|---|---|
| `NATS_URL` | Publisher, Gateway | `nats://localhost:4222` | URL del servidor NATS |
| `PORT` | Publisher | `8080` | Puerto de escucha |
| `PORT` | Gateway | `8081` | Puerto de escucha |
| `ALLOWED_ORIGIN` | Gateway | `*` | CORS origin (usar dominio en producción) |

---

## Uso desde el frontend

El `useSSE` hook encapsula todo el ciclo de vida de `EventSource`:

```tsx
import { useSSE } from './hooks/useSSE';

function Dashboard() {
  const { events, status, clear } = useSSE('alerts.critical');

  return (
    <div>
      <span>Estado: {status}</span>
      {events.map(event => (
        <div key={event.id}>
          <strong>{event.topic}</strong>
          <pre>{JSON.stringify(event.data, null, 2)}</pre>
        </div>
      ))}
    </div>
  );
}
```

Para publicar desde el frontend:

```tsx
import { usePublish } from './hooks/usePublish';

function AlertButton() {
  const { publish, status } = usePublish();

  return (
    <button onClick={() => publish('alerts.critical', { msg: 'Alerta manual' })}>
      {status === 'loading' ? 'Enviando...' : 'Disparar alerta'}
    </button>
  );
}
```

---

## Publicar desde cualquier cliente HTTP

```bash
# curl
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '{"topic":"alerts.critical","data":{"msg":"Servidor caído","severity":"high"}}'

# Escuchar en terminal (sin browser)
curl -N http://localhost:8081/subscribe?topic=alerts.critical
```

```python
# Python
import requests
requests.post('http://localhost:8080/publish', json={
    'topic': 'metrics.cpu',
    'data': {'host': 'web-01', 'cpu': 87.3}
})
```

```go
// Go
http.Post("http://localhost:8080/publish", "application/json",
    strings.NewReader(`{"topic":"deploys.prod","data":{"version":"v2.1.0"}}`))
```

---

## Producción

Para producción, considera:

1. **CORS**: cambia `ALLOWED_ORIGIN=*` al dominio real.
2. **nginx / ALB**: requieren configuración específica para no cortar conexiones SSE. Ver guía completa abajo.
3. **Load balancing**: gracias a NATS, **no se necesitan sticky sessions**. Cualquier instancia del gateway atiende a cualquier cliente.
4. **Escala horizontal del gateway**: añadir instancias solo requiere que se conecten a NATS. El fan-out lo hace NATS.
5. **TLS**: termina TLS en nginx, ALB o CloudFront; los servicios Go no necesitan cambios.

📖 **[Guía completa de escalado en AWS con ALB →](docs/aws-scaling.md)**

Cubre: idle timeout, deregistration delay, sticky sessions, NATS en AWS, CloudFront, security groups y checklist de producción.

---

Ejemplo de configuración nginx para SSE:

```nginx
location /subscribe {
    proxy_pass http://gateway:8081;
    proxy_http_version 1.1;
    proxy_set_header Connection '';
    proxy_buffering off;
    proxy_cache off;
    proxy_read_timeout 3600s;
    proxy_send_timeout 3600s;
    chunked_transfer_encoding on;
}
```

---

## Licencia

MIT — ver [LICENSE](LICENSE).
