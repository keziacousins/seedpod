import { useEffect, useState, useRef } from "preact/hooks";
import { sseClient } from "../lib/sse-client";
import { eventStore } from "../lib/event-store";

export function ConnectionStatus({ onToggleHud, hudVisible }) {
  const [connected, setConnected] = useState(false);
  const [pulse, setPulse] = useState(false);
  const pulseTimeoutRef = useRef(null);

  useEffect(() => {

    // Check initial connection state
    setConnected(sseClient.isConnected());

    // Listen for connection state changes
    const handleConnected = () => {
      setConnected(true);
    };

    const handleDisconnected = () => {
      setConnected(false);
    };

    // Subscribe to event store updates to trigger pulse animation
    const unsubscribe = eventStore.subscribe(() => {
      // Trigger pulse animation on new event
      setPulse(true);

      // Clear existing timeout if any
      if (pulseTimeoutRef.current) {
        clearTimeout(pulseTimeoutRef.current);
      }

      // Reset pulse after animation completes (0.8s total)
      pulseTimeoutRef.current = setTimeout(() => {
        setPulse(false);
      }, 800);
    });

    sseClient.on("connected", handleConnected);
    sseClient.on("disconnected", handleDisconnected);

    return () => {
      sseClient.off("connected", handleConnected);
      sseClient.off("disconnected", handleDisconnected);
      unsubscribe();
      if (pulseTimeoutRef.current) {
        clearTimeout(pulseTimeoutRef.current);
      }
    };
  }, []);

  return (
    <button
      className="connection-status-button"
      onClick={onToggleHud}
      title={hudVisible ? "Hide event feed" : "Show event feed"}
    >
      <span
        className={`connection-status-indicator ${pulse ? "pulse" : ""}`}
        style={{
          background: connected ? "var(--green)" : "var(--base01)",
          opacity: connected ? 1 : 0.5,
        }}
      />
      <span
        className="connection-status-text"
        style={{ opacity: connected ? 1 : 0.7 }}
      >
        {connected ? "Connected" : "Disconnected"}
      </span>
    </button>
  );
}
