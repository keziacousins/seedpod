import { useEffect } from 'preact/hooks';
import { sseClient } from '../lib/sse-client';

export function useSSE(eventType, handler) {
  useEffect(() => {
    if (!eventType || !handler) return;

    sseClient.on(eventType, handler);

    return () => {
      sseClient.off(eventType, handler);
    };
  }, [eventType, handler]);
}
