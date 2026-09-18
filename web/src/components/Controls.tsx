import { type FormEvent, useState } from 'react';
import type { ConnectionStatus } from '../types';

interface Props {
  activeTopic:    string;
  status:         ConnectionStatus;
  eventCount:     number;
  publishStatus:  'idle' | 'loading' | 'success' | 'error';
  onConnect:      (topic: string) => void;
  onPublish:      (topic: string, data: Record<string, unknown>) => void;
  onClear:        () => void;
}

export function Controls({
  activeTopic,
  status,
  eventCount,
  publishStatus,
  onConnect,
  onPublish,
  onClear,
}: Props) {
  // Input local — solo actualizamos el topic activo al hacer submit explícito.
  const [topicInput, setTopicInput] = useState(activeTopic);

  function handleConnect(e: FormEvent) {
    e.preventDefault();
    if (topicInput.trim()) onConnect(topicInput.trim());
  }

  function handlePublish() {
    const topic = activeTopic || topicInput.trim();
    if (!topic) return;

    // Payload de ejemplo que ejercita el flujo completo API → NATS → SSE → browser.
    onPublish(topic, {
      message: 'Evento de prueba desde el dashboard',
      level:   'info',
      source:  'arecibo-web',
      ts:      Date.now(),
    });
  }

  const PUBLISH_LABEL: Record<typeof publishStatus, string> = {
    idle:    'Enviar evento',
    loading: 'Enviando...',
    success: '✓ Enviado',
    error:   '✗ Error',
  };

  return (
    <section className="controls">
      {/* Formulario de conexión a un topic */}
      <form className="controls__form" onSubmit={handleConnect}>
        <label htmlFor="topic-input" className="sr-only">
          Topic NATS
        </label>
        <input
          id="topic-input"
          className="controls__input"
          type="text"
          placeholder="Topic NATS (ej: alerts.critical)"
          value={topicInput}
          onChange={(e) => setTopicInput(e.target.value)}
          spellCheck={false}
          autoComplete="off"
        />
        <button
          className="btn btn--primary"
          type="submit"
          disabled={!topicInput.trim() || topicInput.trim() === activeTopic}
        >
          {status === 'connecting' ? 'Conectando...' : 'Conectar'}
        </button>
      </form>

      {/* Acciones sobre la conexión activa */}
      <div className="controls__actions">
        <button
          className={`btn btn--secondary btn--publish btn--${publishStatus}`}
          type="button"
          onClick={handlePublish}
          disabled={!activeTopic || publishStatus === 'loading'}
          title={!activeTopic ? 'Conéctate a un topic primero' : undefined}
        >
          {PUBLISH_LABEL[publishStatus]}
        </button>

        <button
          className="btn btn--ghost"
          type="button"
          onClick={onClear}
          disabled={eventCount === 0}
        >
          Limpiar ({eventCount})
        </button>
      </div>
    </section>
  );
}
