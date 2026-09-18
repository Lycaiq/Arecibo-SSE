import { useCallback, useEffect, useState } from 'react';
import type { ConnectionStatus, DisplayEvent, SSEEventData } from '../types';

// Máximo de eventos que mantenemos en memoria para no crecer infinitamente.
// 200 eventos es suficiente para cualquier dashboard razonable.
const MAX_EVENTS_DEFAULT = 200;

interface UseSSEReturn {
  events: DisplayEvent[];
  status: ConnectionStatus;
  clear: () => void;
}

/**
 * Gestiona la conexión SSE con el gateway.
 *
 * El hook re-conecta automáticamente cuando cambia el topic.
 * La limpieza del EventSource ocurre en el cleanup del useEffect,
 * lo que garantiza que al desmontar el componente o cambiar de topic
 * no queden conexiones abiertas en el browser.
 *
 * EventSource reconecta automáticamente por diseño del protocolo SSE,
 * así que no necesitamos lógica de retry manual.
 */
export function useSSE(
  topic: string,
  maxEvents: number = MAX_EVENTS_DEFAULT,
): UseSSEReturn {
  const [events, setEvents] = useState<DisplayEvent[]>([]);
  const [status, setStatus] = useState<ConnectionStatus>('idle');

  useEffect(() => {
    // No conectamos si no hay topic — el estado queda en idle.
    const trimmed = topic.trim();
    if (!trimmed) {
      setStatus('idle');
      return;
    }

    setStatus('connecting');
    setEvents([]);

    // URL relativa → Vite proxy la redirige al gateway en desarrollo.
    // En producción, nginx o el ingress hacen lo mismo.
    const url = `/subscribe?topic=${encodeURIComponent(trimmed)}`;
    const es = new EventSource(url);

    es.onopen = () => {
      setStatus('connected');
    };

    es.onmessage = (event: MessageEvent<string>) => {
      try {
        const payload = JSON.parse(event.data) as SSEEventData;

        const displayEvent: DisplayEvent = {
          id: crypto.randomUUID(),
          receivedAt: Date.now(),
          ...payload,
        };

        // Insertamos al frente para que el feed muestre los más recientes arriba.
        setEvents((prev) => [displayEvent, ...prev].slice(0, maxEvents));
      } catch {
        // Mensajes no-JSON (como los comentarios ": keepalive") llegan aquí a veces.
        // Los ignoramos — no deberían llegar a onmessage, pero por si acaso.
      }
    };

    es.onerror = () => {
      // Si readyState es CLOSED, el browser no va a reconectar.
      // Si es CONNECTING, está en medio del backoff de reconexión automática.
      setStatus(
        es.readyState === EventSource.CLOSED ? 'error' : 'connecting',
      );
    };

    // Cleanup: se ejecuta cuando el topic cambia O cuando el componente se desmonta.
    // Es el equivalente a cancelar la suscripción NATS en el backend.
    return () => {
      es.close();
      setStatus('idle');
    };
  }, [topic, maxEvents]);

  const clear = useCallback(() => setEvents([]), []);

  return { events, status, clear };
}
