// Tipos compartidos entre hooks y componentes.
// Mantenerlos aquí evita importaciones circulares.

/** Payload que llega desde el servidor (formato definido por el Publisher). */
export interface SSEEventData {
  topic: string;
  data: Record<string, unknown>;
  timestamp: string;
}

/** Evento enriquecido con ID local y timestamp de recepción para el renderizado. */
export interface DisplayEvent extends SSEEventData {
  /** UUID generado localmente — solo para React keys y orden de lista. */
  id: string;
  /** Momento exacto en que el browser lo recibió, en milisegundos. */
  receivedAt: number;
}

/** Estado de la conexión SSE desde el punto de vista del cliente. */
export type ConnectionStatus = 'idle' | 'connecting' | 'connected' | 'error';
