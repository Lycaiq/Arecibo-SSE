import { useCallback, useState } from 'react';

type PublishStatus = 'idle' | 'loading' | 'success' | 'error';

interface UsePublishReturn {
  publish: (topic: string, data: Record<string, unknown>) => Promise<void>;
  status: PublishStatus;
}

/**
 * Encapsula el POST al publisher API.
 * El estado 'success' o 'error' se resetea a 'idle' tras 2s para que el botón
 * vuelva a su estado normal sin que el usuario tenga que hacer nada.
 */
export function usePublish(): UsePublishReturn {
  const [status, setStatus] = useState<PublishStatus>('idle');

  const publish = useCallback(
    async (topic: string, data: Record<string, unknown>) => {
      setStatus('loading');

      try {
        const res = await fetch('/publish', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ topic, data }),
        });

        setStatus(res.ok ? 'success' : 'error');
      } catch {
        setStatus('error');
      } finally {
        setTimeout(() => setStatus('idle'), 2000);
      }
    },
    [],
  );

  return { publish, status };
}
