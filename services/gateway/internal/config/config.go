package config

import (
	"fmt"
	"os"
)

// Config agrupa la configuración del gateway.
type Config struct {
	NatsURL         string
	Port            string
	// AllowedOrigin controla el CORS. En producción esto debería ser el dominio real.
	AllowedOrigin   string
}

// Load lee desde variables de entorno con valores por defecto para desarrollo local.
func Load() (*Config, error) {
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:4222"
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8081"
	}

	allowedOrigin := os.Getenv("ALLOWED_ORIGIN")
	if allowedOrigin == "" {
		// En desarrollo aceptamos cualquier origen para no obstaculizar el frontend local.
		allowedOrigin = "*"
	}

	return &Config{
		NatsURL:       natsURL,
		Port:          port,
		AllowedOrigin: allowedOrigin,
	}, nil
}

// Addr devuelve la dirección de escucha en formato ":PORT".
func (c *Config) Addr() string {
	return fmt.Sprintf(":%s", c.Port)
}
