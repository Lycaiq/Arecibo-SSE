#!/usr/bin/env bash
# =============================================================================
# Smoke test E2E: valida el flujo completo usando solo curl.
# Útil para verificar que el stack está funcionando en cualquier entorno.
#
# Uso:
#   ./scripts/e2e_test.sh
#   GATEWAY_URL=http://mygw:8081 PUBLISHER_URL=http://mypub:8080 ./scripts/e2e_test.sh
# =============================================================================
set -euo pipefail

GATEWAY_URL="${GATEWAY_URL:-http://localhost:8081}"
PUBLISHER_URL="${PUBLISHER_URL:-http://localhost:8080}"
TOPIC="${TOPIC:-e2e.smoke.test}"
TIMEOUT="${TIMEOUT:-10}"  # segundos máximos esperando el evento SSE

PASS=0
FAIL=0

log_ok()   { echo "  ✅ $*"; ((PASS++)) || true; }
log_fail() { echo "  ❌ $*"; ((FAIL++)) || true; }
log_info() { echo "  ℹ️  $*"; }

echo ""
echo "🔬 Arecibo-SSE E2E Smoke Test"
echo "   Gateway:   $GATEWAY_URL"
echo "   Publisher: $PUBLISHER_URL"
echo "   Topic:     $TOPIC"
echo ""

# ----------------------------------------------------------------------------
# 1. Health check del gateway
# ----------------------------------------------------------------------------
echo "── Test 1: Gateway health check"
STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$GATEWAY_URL/health" --max-time 5 || echo "000")
if [[ "$STATUS" == "200" ]]; then
  log_ok "Gateway responde con 200"
else
  log_fail "Gateway no responde (HTTP $STATUS). ¿Está corriendo?"
fi

# ----------------------------------------------------------------------------
# 2. Health check del publisher
# ----------------------------------------------------------------------------
echo "── Test 2: Publisher health check"
STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$PUBLISHER_URL/health" --max-time 5 || echo "000")
if [[ "$STATUS" == "200" ]]; then
  log_ok "Publisher responde con 200"
else
  log_fail "Publisher no responde (HTTP $STATUS). ¿Está corriendo?"
fi

# ----------------------------------------------------------------------------
# 3. Cabeceras SSE correctas
# ----------------------------------------------------------------------------
echo "── Test 3: Cabeceras SSE"
HEADERS=$(curl -s -I --max-time 5 \
  "$GATEWAY_URL/subscribe?topic=$TOPIC" \
  -H "Accept: text/event-stream" 2>/dev/null || echo "")

if echo "$HEADERS" | grep -qi "content-type: text/event-stream"; then
  log_ok "Content-Type: text/event-stream presente"
else
  log_fail "Content-Type incorrecto o ausente"
fi

if echo "$HEADERS" | grep -qi "x-accel-buffering: no"; then
  log_ok "X-Accel-Buffering: no presente"
else
  log_fail "Falta X-Accel-Buffering: no (podría fallar detrás de nginx)"
fi

if echo "$HEADERS" | grep -qi "access-control-allow-origin"; then
  log_ok "Cabecera CORS presente"
else
  log_fail "Falta Access-Control-Allow-Origin"
fi

# ----------------------------------------------------------------------------
# 4. Flujo completo: SSE recibe evento publicado por el publisher
# ----------------------------------------------------------------------------
echo "── Test 4: Flujo completo SSE (timeout: ${TIMEOUT}s)"

# Abrimos conexión SSE en background y redirigimos a un archivo temporal.
SSE_OUT=$(mktemp)
curl -s -N --max-time "$TIMEOUT" \
  "$GATEWAY_URL/subscribe?topic=$TOPIC" \
  -H "Accept: text/event-stream" > "$SSE_OUT" &
CURL_PID=$!

log_info "Conexión SSE abierta (PID: $CURL_PID)"
sleep 1  # damos tiempo al gateway para registrar la suscripción NATS

# Publicamos un evento de prueba con un ID único.
TEST_ID="e2e-$(date +%s)"
PUB_STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
  -X POST "$PUBLISHER_URL/publish" \
  -H "Content-Type: application/json" \
  -d "{\"topic\":\"$TOPIC\",\"data\":{\"test_id\":\"$TEST_ID\",\"msg\":\"smoke test\"}}" \
  --max-time 5 || echo "000")

if [[ "$PUB_STATUS" == "202" ]]; then
  log_ok "Evento publicado (202 Accepted)"
else
  log_fail "Error al publicar (HTTP $PUB_STATUS)"
fi

# Esperamos a que el evento aparezca en el archivo SSE.
RECEIVED=false
for _ in $(seq 1 "$TIMEOUT"); do
  if grep -q "$TEST_ID" "$SSE_OUT" 2>/dev/null; then
    RECEIVED=true
    break
  fi
  sleep 1
done

# Terminamos el curl SSE.
kill "$CURL_PID" 2>/dev/null || true
wait "$CURL_PID" 2>/dev/null || true

if $RECEIVED; then
  log_ok "Evento recibido por el cliente SSE con test_id=$TEST_ID"
else
  log_fail "El evento NO llegó al cliente SSE en ${TIMEOUT}s"
  log_info "Contenido recibido:"
  cat "$SSE_OUT" | head -20 | sed 's/^/    /'
fi

rm -f "$SSE_OUT"

# ----------------------------------------------------------------------------
# 5. Validación de topic inválido
# ----------------------------------------------------------------------------
echo "── Test 5: Validación de topic vacío (publisher)"
STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
  -X POST "$PUBLISHER_URL/publish" \
  -H "Content-Type: application/json" \
  -d '{"topic":"","data":{}}' \
  --max-time 5 || echo "000")
if [[ "$STATUS" == "400" ]]; then
  log_ok "Publisher rechaza topic vacío con 400"
else
  log_fail "Publisher debería rechazar topic vacío (obtuvo $STATUS)"
fi

# ----------------------------------------------------------------------------
# Resumen
# ----------------------------------------------------------------------------
echo ""
echo "────────────────────────────────"
echo "  ✅ Pasaron: $PASS"
echo "  ❌ Fallaron: $FAIL"
echo "────────────────────────────────"
echo ""

if [[ "$FAIL" -gt 0 ]]; then
  echo "💡 Levanta el stack con: docker compose up"
  exit 1
fi

echo "🎉 Todos los tests pasaron."
