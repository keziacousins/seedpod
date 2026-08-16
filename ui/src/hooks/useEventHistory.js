import { useState, useEffect, useCallback, useRef } from "preact/hooks";
import { eventStore } from "../lib/event-store";

export function useEventHistory(maxEvents = 100, isPaused = false) {
  const [events, setEvents] = useState(() => eventStore.getEvents());
  const pausedEventsRef = useRef(null);

  const clearEvents = useCallback(() => {
    eventStore.clearEvents();
    pausedEventsRef.current = null;
  }, []);

  useEffect(() => {
    eventStore.setMaxEvents(maxEvents);

    const unsubscribe = eventStore.subscribe((newEvents) => {
      if (!isPaused) {
        setEvents(newEvents);
        pausedEventsRef.current = null;
      } else {
        pausedEventsRef.current = newEvents;
      }
    });

    if (!isPaused && pausedEventsRef.current !== null) {
      setEvents(pausedEventsRef.current);
      pausedEventsRef.current = null;
    }

    return unsubscribe;
  }, [maxEvents, isPaused]);

  return {
    events,
    clearEvents,
    eventCount: events.length,
  };
}
