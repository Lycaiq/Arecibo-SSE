package config

import (
	"fmt"
	"os"
)

// Config agrupa todo lo que necesita el servicio para arrancar.
// Centralizar la config aquí evita que cada paquete lea os.Getenv por su cuenta.
type Config struct {
	NatsURL string
	Port    string
}

// Load lee la config desde variables de entorno con valores por defecto razonables.
// No usamos librerías externas de config — para dos variables es un overkill.
func Load() (*Config, error) {
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:4222"
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	return &Config{
		NatsURL: natsURL,
		Port:    port,
	}, nil
}

// Addr devuelve la dirección de escucha en formato ":PORT" que espera net/http.
func (c *Config) Addr() string {
	return fmt.Sprintf(":%s", c.Port)
}
