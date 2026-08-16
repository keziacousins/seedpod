/**
 * Event Store - Centralized storage for SSE event history
 *
 * Singleton that maintains a bounded history of SSE events and notifies subscribers.
 */

class EventStore {
  constructor() {
    this.events = [];
    this.maxEvents = 100;
    this.subscribers = new Set();
    this.sseClient = null;
    this.eventTypes = [];
    this.isInitialized = false;
  }

  initialize(sseClient, eventTypes = []) {
    if (this.isInitialized) return;

    this.sseClient = sseClient;
    this.eventTypes = eventTypes;

    // NOTE: do NOT add "keepalive" here. The server sends a `keepalive` envelope
    // every 30s on an idle connection so the client's heartbeat monitor can see it
    // (api/routers/events.py). It is deliberately unsubscribed: sse-client.js's
    // onmessage refreshes liveness before dispatching, so the heartbeat works
    // without it, and subscribing would flood the buffer and the MiniEventHud with
    // one entry every 30s forever.
    const typesToSubscribe =
      eventTypes.length > 0
        ? eventTypes
        : [
            "cluster_state_changed",
            "deployment_status_changed",
            // DR-0035: `pod_status_changed` is consciously dropped — v2 never
            // emitted it (v2 replaced v1's watch_pods task with per-poll
            // ctx.progress). `workflow_progress` is the in-workflow live signal,
            // and it was MISSING here: the HUD has a formatter case for it and
            // lists it as verbose, but the store never subscribed, so it could
            // never reach the buffer.
            "workflow_progress",
            "snapshot_restore_completed",
            "job_started",
            "job_completed",
            "job_failed",
            "server_shutdown",
            "reconnected",
            "reconciliation_skipped",
          ];

    typesToSubscribe.forEach((type) => {
      sseClient.on(type, (data) => this._handleSSEEvent(type, data));
    });

    this.isInitialized = true;
  }

  _handleSSEEvent(type, data) {
    this.addEvent(type, data);
  }

  addEvent(type, data) {
    const event = {
      id: `${Date.now()}-${Math.random()}`,
      type: type,
      timestamp: data?.timestamp || new Date().toISOString(),
      data: data?.data || data,
      receivedAt: new Date().toISOString(),
    };

    // Add to beginning (newest first) and limit size
    this.events = [event, ...this.events].slice(0, this.maxEvents);

    this.notifySubscribers();
  }

  subscribe(callback) {
    this.subscribers.add(callback);
    return () => {
      this.subscribers.delete(callback);
    };
  }

  notifySubscribers() {
    this.subscribers.forEach((callback) => {
      try {
        callback(this.events);
      } catch (err) {
        console.error("[EventStore] Error in subscriber:", err);
      }
    });
  }

  getEvents() {
    return this.events;
  }

  clearEvents() {
    this.events = [];
    this.notifySubscribers();
  }

  setMaxEvents(max) {
    this.maxEvents = max;
    if (this.events.length > max) {
      this.events = this.events.slice(0, max);
      this.notifySubscribers();
    }
  }

  getEventCount() {
    return this.events.length;
  }
}

export const eventStore = new EventStore();
