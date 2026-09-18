import type { DisplayEvent } from '../types';

interface Props {
  event: DisplayEvent;
}

/** Formatea el timestamp de recepción como HH:MM:SS.mmm — importante en un feed de tiempo real. */
function formatTime(ms: number): string {
  const d = new Date(ms);
  const hh = d.getHours().toString().padStart(2, '0');
  const mm = d.getMinutes().toString().padStart(2, '0');
  const ss = d.getSeconds().toString().padStart(2, '0');
  const mmm = d.getMilliseconds().toString().padStart(3, '0');
  return `${hh}:${mm}:${ss}.${mmm}`;
}

/** Genera un color de acento consistente para cada topic usando un hash simple. */
function topicColor(topic: string): string {
  // Paleta de colores que funcionan bien en fondo oscuro.
  const palette = [
    '#6366f1', // indigo
    '#22c55e', // verde
    '#f59e0b', // ámbar
    '#ec4899', // rosa
    '#06b6d4', // cyan
    '#a855f7', // violeta
    '#f97316', // naranja
    '#14b8a6', // teal
  ];
  let hash = 0;
  for (let i = 0; i < topic.length; i++) {
    hash = (hash * 31 + topic.charCodeAt(i)) >>> 0;
  }
  return palette[hash % palette.length];
}

export function EventCard({ event }: Props) {
  const color = topicColor(event.topic);

  return (
    <article className="event-card" style={{ borderLeftColor: color }}>
      <header className="event-card__header">
        <span className="event-card__topic" style={{ color }}>
          {event.topic}
        </span>
        <time
          className="event-card__time"
          dateTime={new Date(event.receivedAt).toISOString()}
          title={`Publicado: ${event.timestamp}`}
        >
          {formatTime(event.receivedAt)}
        </time>
      </header>

      <pre className="event-card__data">
        {JSON.stringify(event.data, null, 2)}
      </pre>
    </article>
  );
}
