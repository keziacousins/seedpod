import { memo } from "preact/compat";
import { useMemo, useRef, useEffect, useState } from "preact/hooks";
import { route } from "preact-router";
import { useEventHistory } from "../hooks/useEventHistory";
import { formatTime } from "../lib/time-utils";

/**
 * Mini Event HUD - Compact display of recent SSE events
 *
 * Fixed 4-row scrollable list showing all buffered events. Auto-scrolls to top
 * when new events arrive, but respects user scroll position (with 30s inactivity timeout).
 * Includes pause and clear controls.
 */
// Backlog #21: a BIRTH transition is `old_status: ""` -- deliberately empty, sent by
// `core/machine.py` at all four birth sites to preserve v1's UI-visible broadcast
// shape. `data.old_status || "?"` rendered that empty string as `?`, so a value the
// server states precisely ("this record did not exist before") displayed identically
// to one it failed to send. Empty means birth and renders as `→ pending`; only a
// genuinely absent field is `?`.
//
// Third instance of this conflation class in the HUD (after `|| "updated"` and the
// `pod_status_changed` `data.status` read that no payload ever carried), which is why
// it is a named helper rather than another inline `||` chain.
export function transition(data) {
  const to = data?.new_status || data?.status || "?";
  const from = data?.old_status;
  if (from === "") return `→ ${to}`;
  return `${from || "?"} → ${to}`;
}

function MiniEventHudComponent() {
  console.log("[MiniEventHud] Rendering...");

  const [isPaused, setIsPaused] = useState(false);
  const [includeVerbose, setIncludeVerbose] = useState(false);

  // Manage event history internally - this won't affect App re-renders
  // Pass isPaused to the hook so it stops updating when paused
  const { events, clearEvents, eventCount } = useEventHistory(100, isPaused);
  console.log(
    `[MiniEventHud] Current event count: ${eventCount}, isPaused: ${isPaused}`,
  );

  const scrollContainerRef = useRef(null);
  const lastScrollTimeRef = useRef(Date.now());
  const scrollInactivityTimerRef = useRef(null);
  const [shouldAutoScroll, setShouldAutoScroll] = useState(true);
  const prevEventCountRef = useRef(eventCount);

  // Noisy event types that are hidden by default
  const verboseEventTypes = [
    "job_started",
    "job_completed",
    "job_failed",
    "workflow_progress",
  ];

  // Filter events based on includeVerbose setting
  const visibleEvents = useMemo(() => {
    const filtered = includeVerbose
      ? events
      : events.filter(
          (event) =>
            !event.type.startsWith("job_") &&
            !verboseEventTypes.includes(event.type),
        );
    console.log(
      `[MiniEventHud] Showing ${filtered.length} of ${events.length} events (includeVerbose: ${includeVerbose})`,
    );
    return filtered;
  }, [events, includeVerbose]);

  // Handle user scroll - disable auto-scroll when user scrolls
  const handleScroll = () => {
    lastScrollTimeRef.current = Date.now();

    // If user scrolled, temporarily disable auto-scroll
    if (scrollContainerRef.current?.scrollTop > 10) {
      console.log("[MiniEventHud] User scrolled down, disabling auto-scroll");
      setShouldAutoScroll(false);
    } else {
      // User scrolled back to top
      console.log("[MiniEventHud] User at top, enabling auto-scroll");
      setShouldAutoScroll(true);
    }

    // Clear existing timer
    if (scrollInactivityTimerRef.current) {
      clearTimeout(scrollInactivityTimerRef.current);
    }

    // Set 30s inactivity timer to re-enable auto-scroll
    scrollInactivityTimerRef.current = setTimeout(() => {
      console.log("[MiniEventHud] 30s inactivity, re-enabling auto-scroll");
      setShouldAutoScroll(true);
      // Scroll to top when re-enabled
      if (scrollContainerRef.current) {
        scrollContainerRef.current.scrollTop = 0;
      }
    }, 30000); // 30 seconds
  };

  // Auto-scroll to top when new events arrive (if enabled and not paused)
  useEffect(() => {
    const hasNewEvents = eventCount > prevEventCountRef.current;

    if (
      hasNewEvents &&
      shouldAutoScroll &&
      !isPaused &&
      scrollContainerRef.current
    ) {
      console.log("[MiniEventHud] New event arrived, auto-scrolling to top");
      scrollContainerRef.current.scrollTop = 0;
    }

    prevEventCountRef.current = eventCount;
  }, [eventCount, shouldAutoScroll, isPaused]);

  const handlePauseToggle = () => {
    setIsPaused(!isPaused);
    console.log(
      `[MiniEventHud] ${!isPaused ? "Paused" : "Resumed"} event updates`,
    );
  };

  const handleClear = () => {
    clearEvents();
    console.log("[MiniEventHud] Cleared all events");
  };

  // Cleanup timer on unmount
  useEffect(() => {
    return () => {
      if (scrollInactivityTimerRef.current) {
        clearTimeout(scrollInactivityTimerRef.current);
      }
    };
  }, []);

  const formatEventData = (event) => {
    // Format data differently based on event type
    const data = event.data;

    switch (event.type) {
      case "cluster_state_changed":
        return `${data.cluster_id?.slice(0, 8) || "?"} | ${transition(data)}`;

      case "deployment_status_changed":
        // v2 sends old_status/new_status (obligation 1), never a bare `status` —
        // reading `status` alone rendered the "updated" fallback for every event.
        return `${data.deployment_id?.slice(0, 8) || "?"} | ${transition(data)}`;

      case "workflow_progress":
        return `${data.cluster_id?.slice(0, 8) || "?"} | ${data.step_path || data.message || "…"}${data.attempt > 1 ? ` (attempt ${data.attempt})` : ""}`;

      case "job_started":
        return `${data.workflow || "workflow"} | ${data.cluster_id?.slice(0, 8) || ""}`.trim();

      case "job_completed":
        return `${data.workflow || "workflow"} | ${data.cluster_id?.slice(0, 8) || ""} | ✓`.trim();

      case "job_failed":
        return `${data.workflow || "workflow"} | ${data.cluster_id?.slice(0, 8) || ""} | ✗ ${data.error || ""}`.trim();

      case "server_shutdown":
        return data.message || "Server shutting down";

      case "reconnected":
        return "Reconnected to server";

      // `pod_status_changed` removed (DR-0035): v2 never emitted it, and this
      // case read `data.status` — a field v1's payload never had either (it sent
      // `phase`), so it rendered "?" even against v1. Same conflation class as the
      // `|| "updated"` bug smoke 6 fixed.
      case "snapshot_restore_completed":
        return `${data.cluster_id?.slice(0, 8) || "?"} | ${data.status || "?"} | ${data.services_restored?.length || 0} services`;

      case "reconciliation_skipped":
        return `${data.cluster_id?.slice(0, 8) || "?"} | ${data.provider || "?"} unreachable`;

      default:
        // Generic fallback - show first useful field
        if (!data) return "(no data)";
        if (typeof data === "string") return data;
        if (data.message) return data.message;
        if (data.cluster_id) return data.cluster_id.slice(0, 8);
        if (data.workflow) return data.workflow;

        // Last resort: show all keys with values
        const keys = Object.keys(data).filter(
          (k) => data[k] !== undefined && data[k] !== null,
        );
        if (keys.length === 0) return "(empty)";

        return keys
          .slice(0, 3)
          .map((k) => `${k}: ${String(data[k]).slice(0, 20)}`)
          .join(" | ");
    }
  };

  const getEventTypeClass = (eventType) => {
    // Return CSS class based on event type
    if (eventType.includes("failed") || eventType.includes("error"))
      return "event-type-error";
    if (eventType.includes("completed") || eventType.includes("success"))
      return "event-type-success";
    if (eventType.includes("skipped")) return "event-type-warning";
    if (eventType.includes("started") || eventType.includes("changed"))
      return "event-type-info";
    return "event-type-default";
  };

  const handleEventClick = (event) => {
    // Navigate to relevant page based on event type
    if (event.data?.cluster_id && event.type.includes("cluster")) {
      route(`/clusters/${event.data.cluster_id}`);
    } else if (event.data?.deployment_id) {
      route(`/deployments/${event.data.deployment_id}`);
    }
  };

  return (
    <div className="mini-event-hud">
      <div className="mini-event-hud-header">
        <span className="mini-event-hud-title">Recent Events</span>
        <div className="mini-event-hud-controls">
          <button
            className={`mini-event-hud-control-btn ${includeVerbose ? "active" : ""}`}
            onClick={() => setIncludeVerbose(!includeVerbose)}
            title={
              includeVerbose
                ? "Hide verbose events (jobs, pods)"
                : "Show all events (jobs, pods)"
            }
          >
            ⚙
          </button>
          <button
            className="mini-event-hud-control-btn"
            onClick={handlePauseToggle}
            title={isPaused ? "Resume event updates" : "Pause event updates"}
          >
            {isPaused ? "▶" : "⏸"}
          </button>
          <button
            className="mini-event-hud-control-btn"
            onClick={handleClear}
            title="Clear all events"
          >
            🗑
          </button>
        </div>
      </div>

      <div
        className="mini-event-hud-events"
        ref={scrollContainerRef}
        onScroll={handleScroll}
      >
        {visibleEvents.length === 0 ? (
          <div className="mini-event-hud-empty">No events received yet</div>
        ) : (
          visibleEvents.map((event) => (
            <div
              key={event.id}
              className={`mini-event-hud-event ${event.data?.cluster_id || event.data?.deployment_id ? "clickable" : ""}`}
              onClick={() => handleEventClick(event)}
            >
              <span className="mini-event-time">
                {formatTime(event.timestamp)}
              </span>
              <span
                className={`mini-event-type ${getEventTypeClass(event.type)}`}
              >
                {event.type}
              </span>
              <span className="mini-event-data">{formatEventData(event)}</span>
            </div>
          ))
        )}
      </div>
    </div>
  );
}

// Memoize component to prevent unnecessary re-renders
export const MiniEventHud = memo(MiniEventHudComponent);
