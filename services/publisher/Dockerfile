# =============================================================================
# Multi-stage build para el Publisher.
# La imagen final pesa ~10MB en vez de ~800MB de la imagen base de Go.
# =============================================================================

# --- Etapa de compilación ---
FROM golang:1.23-alpine AS builder

WORKDIR /app

# Copiamos go.mod y go.sum primero para aprovechar la caché de Docker.
# Si el código cambia pero las dependencias no, esta capa no se reconstruye.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO_ENABLED=0 produce un binario estático sin dependencias de libc.
# GOOS y GOARCH están explícitos para claridad aunque Alpine ya usa linux/amd64.
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-w -s" -o publisher ./cmd/main.go

# --- Imagen final mínima ---
FROM scratch

# Copiamos certificados CA para que el servicio pueda hacer TLS si lo necesita.
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

COPY --from=builder /app/publisher /publisher

EXPOSE 8080

ENTRYPOINT ["/publisher"]
