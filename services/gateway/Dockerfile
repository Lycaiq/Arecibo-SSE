# =============================================================================
# Multi-stage build para el SSE Gateway.
# Misma estrategia que el Publisher: imagen final sobre scratch.
# =============================================================================

# --- Etapa de compilación ---
FROM golang:1.23-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-w -s" -o gateway ./cmd/main.go

# --- Imagen final mínima ---
FROM scratch

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

COPY --from=builder /app/gateway /gateway

EXPOSE 8081

ENTRYPOINT ["/gateway"]
