import { useState } from 'react';
import './App.css';
import { Controls } from './components/Controls';
import { EventCard } from './components/EventCard';
import { StatusBadge } from './components/StatusBadge';
import { usePublish } from './hooks/usePublish';
import { useSSE } from './hooks/useSSE';

// Topic por defecto al cargar la app — facilita el onboarding.
const DEFAULT_TOPIC = 'arecibo.notifications';

export default function App() {
  // El topic "activo" es el que realmente tiene una conexión SSE abierta.
  // Solo cambia cuando el usuario hace submit del formulario.
  const [activeTopic, setActiveTopic] = useState(DEFAULT_TOPIC);

  const { events, status, clear } = useSSE(activeTopic);
  const { publish, status: publishStatus } = usePublish();

  return (
    <div className="app">
      {/* Barra superior */}
      <header className="app__header">
        <div className="app__logo">
          <span className="app__logo-icon" aria-hidden="true">📡</span>
          <h1 className="app__title">Arecibo<span>SSE</span></h1>
        </div>

        <div className="app__meta">
          {activeTopic && (
            <span className="app__active-topic" title="Topic activo">
              {activeTopic}
            </span>
          )}
          <StatusBadge status={status} />
        </div>
      </header>

      {/* Panel de controles */}
      <Controls
        activeTopic={activeTopic}
        status={status}
        eventCount={events.length}
        publishStatus={publishStatus}
        onConnect={setActiveTopic}
        onPublish={publish}
        onClear={clear}
      />

      {/* Feed de eventos */}
      <main className="app__feed">
        {events.length === 0 ? (
          <EmptyState status={status} topic={activeTopic} />
        ) : (
          <ol className="event-list" aria-label="Eventos en tiempo real" aria-live="polite">
            {events.map((event) => (
              <li key={event.id}>
                <EventCard event={event} />
              </li>
            ))}
          </ol>
        )}
      </main>

      <footer className="app__footer">
        <span>Arecibo-SSE · NATS + Server-Sent Events</span>
      </footer>
    </div>
  );
}

// Estado vacío que guía al usuario dependiendo de en qué punto está.
function EmptyState({
  status,
  topic,
}: {
  status: ReturnType<typeof useSSE>['status'];
  topic: string;
}) {
  if (!topic) {
    return (
      <div className="empty-state">
        <p className="empty-state__icon">📡</p>
        <p>Ingresa un topic NATS para empezar a recibir eventos.</p>
      </div>
    );
  }

  if (status === 'connecting') {
    return (
      <div className="empty-state">
        <p className="empty-state__icon empty-state__icon--pulse">⏳</p>
        <p>Conectando a <strong>{topic}</strong>...</p>
      </div>
    );
  }

  if (status === 'error') {
    return (
      <div className="empty-state empty-state--error">
        <p className="empty-state__icon">⚠️</p>
        <p>No se pudo conectar al gateway. Verifica que el servicio esté activo.</p>
      </div>
    );
  }

  return (
    <div className="empty-state">
      <p className="empty-state__icon">👂</p>
      <p>
        Escuchando en <strong>{topic}</strong>.
        <br />
        Usa el botón <em>Enviar evento</em> para probar el flujo completo.
      </p>
    </div>
  );
}
