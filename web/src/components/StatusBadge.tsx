import type { ConnectionStatus } from '../types';

interface Props {
  status: ConnectionStatus;
}

// Mapeo de estado a etiqueta visible y clase CSS.
const STATUS_LABEL: Record<ConnectionStatus, string> = {
  idle:       'Desconectado',
  connecting: 'Conectando...',
  connected:  'Conectado',
  error:      'Error',
};

export function StatusBadge({ status }: Props) {
  return (
    <span className={`status-badge status-badge--${status}`}>
      <span className="status-badge__dot" aria-hidden="true" />
      {STATUS_LABEL[status]}
    </span>
  );
}
