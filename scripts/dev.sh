#!/usr/bin/env bash
# =============================================================================
# Arranca todo el stack en modo desarrollo local (sin Docker).
# Útil para desarrollo rápido: NATS debe estar corriendo por separado.
# Uso: ./scripts/dev.sh
# =============================================================================
set -euo pipefail

# Verificamos que NATS esté disponible antes de arrancar los servicios
if ! nc -z localhost 4222 2>/dev/null; then
  echo "❌ NATS no está corriendo en localhost:4222"
  echo "   Levántalo con: docker run -p 4222:4222 -p 8222:8222 nats:2.10-alpine --http_port 8222"
  exit 1
fi

echo "✅ NATS detectado"
echo "🚀 Arrancando Publisher en :8080..."
(cd services/publisher && PORT=8080 NATS_URL=nats://localhost:4222 go run ./cmd/main.go) &
PID_PUBLISHER=$!

echo "🚀 Arrancando Gateway en :8081..."
(cd services/gateway && PORT=8081 NATS_URL=nats://localhost:4222 go run ./cmd/main.go) &
PID_GATEWAY=$!

# Cleanup limpio al hacer Ctrl+C
trap "kill $PID_PUBLISHER $PID_GATEWAY 2>/dev/null; echo '🛑 Servicios detenidos'" EXIT INT TERM

echo "🟢 Stack corriendo. Ctrl+C para detener."
wait
