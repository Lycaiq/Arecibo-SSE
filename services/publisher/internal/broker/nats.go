package broker

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

// Client envuelve la conexión a NATS y expone solo lo que necesita el publisher.
// Mantenerlo pequeño facilita el mock en tests.
type Client struct {
	conn *nats.Conn
}

// New crea la conexión con reconexión automática e infinita.
// MaxReconnects=-1 es crítico en entornos de contenedores donde NATS puede
// reiniciarse por un deploy sin que queramos que el publisher muera con él.
func New(url string) (*Client, error) {
	opts := []nats.Option{
		nats.Name("arecibo-publisher"),

		// Reintentar indefinidamente con pausa de 2s entre intentos.
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2 * time.Second),

		// Timeout de conexión inicial — si NATS no responde en 10s algo está mal.
		nats.Timeout(10 * time.Second),

		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			if err != nil {
				log.Printf("nats: desconectado: %v", err)
			}
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Printf("nats: reconectado a %s", nc.ConnectedUrl())
		}),
		nats.ClosedHandler(func(_ *nats.Conn) {
			log.Println("nats: conexión cerrada definitivamente")
		}),
	}

	conn, err := nats.Connect(url, opts...)
	if err != nil {
		return nil, fmt.Errorf("conectando a nats en %s: %w", url, err)
	}

	return &Client{conn: conn}, nil
}

// Publish serializa el payload a JSON y lo manda al topic.
// El gateway recibirá este JSON raw y lo reenviará directamente al cliente SSE,
// por eso la serialización ocurre aquí y no en el handler.
func (c *Client) Publish(topic string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("serializando payload para %s: %w", topic, err)
	}

	if err := c.conn.Publish(topic, data); err != nil {
		return fmt.Errorf("publicando en %s: %w", topic, err)
	}

	return nil
}

// Close drena los mensajes pendientes en el buffer antes de cerrar.
// Drain() es más seguro que Close() porque espera a que NATS confirme el flush.
func (c *Client) Close() {
	if err := c.conn.Drain(); err != nil {
		log.Printf("nats: error drenando conexión: %v", err)
	}
}
