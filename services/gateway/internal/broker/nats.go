package broker

import (
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

// msgBufferSize es el tamaño del buffer por suscripción.
// Si el cliente SSE no puede consumir mensajes a tiempo y el buffer se llena,
// descartamos el mensaje más nuevo en vez de bloquear el dispatcher global de NATS.
// El dispatcher de NATS es un goroutine compartido por TODAS las suscripciones
// de la misma conexión — bloquear ahí degradaría a todos los clientes conectados.
const msgBufferSize = 64

// Client gestiona la conexión a NATS para el gateway.
// Una sola conexión es suficiente: NATS multiplexa las suscripciones internamente.
type Client struct {
	conn *nats.Conn
}

// New crea la conexión con reconexión automática e infinita, igual que en el publisher.
func New(url string) (*Client, error) {
	opts := []nats.Option{
		nats.Name("arecibo-gateway"),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2 * time.Second),
		nats.Timeout(10 * time.Second),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			if err != nil {
				log.Printf("nats: desconectado: %v", err)
			}
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Printf("nats: reconectado a %s", nc.ConnectedUrl())
		}),
	}

	conn, err := nats.Connect(url, opts...)
	if err != nil {
		return nil, fmt.Errorf("conectando a nats en %s: %w", url, err)
	}

	return &Client{conn: conn}, nil
}

// Subscribe crea una suscripción al topic y devuelve un canal de lectura y una función de cancelación.
//
// CONTRATO: el caller DEBE llamar a cancel() cuando termine, sin excepción.
// Si no lo hace, la suscripción NATS queda viva y el canal sin drenar — memory leak garantizado.
//
// El canal devuelto es de solo lectura para el caller; quien escribe en él es el callback de NATS.
// El non-blocking send (select/default) es la clave para no bloquear el dispatcher compartido.
func (c *Client) Subscribe(topic string) (<-chan []byte, func(), error) {
	msgCh := make(chan []byte, msgBufferSize)

	sub, err := c.conn.Subscribe(topic, func(msg *nats.Msg) {
		select {
		case msgCh <- msg.Data:
			// mensaje encolado correctamente
		default:
			// El buffer está lleno: este cliente SSE está demasiado lento.
			// Mejor descartar un mensaje que paralizar al resto de suscriptores.
			log.Printf("broker: buffer lleno en topic %q — mensaje descartado", topic)
		}
	})
	if err != nil {
		return nil, nil, fmt.Errorf("suscribiendo a %s: %w", topic, err)
	}

	cancel := func() {
		// Primero desuscribimos para que el callback deje de escribir en el canal.
		if err := sub.Unsubscribe(); err != nil {
			log.Printf("broker: error al desuscribir de %q: %v", topic, err)
		}
		// Drenamos lo que quedó en el buffer para que nadie quede bloqueado intentando leer.
		for len(msgCh) > 0 {
			<-msgCh
		}
	}

	return msgCh, cancel, nil
}

// Close drena mensajes pendientes y cierra la conexión limpiamente.
func (c *Client) Close() {
	if err := c.conn.Drain(); err != nil {
		log.Printf("nats: error drenando conexión: %v", err)
	}
}
