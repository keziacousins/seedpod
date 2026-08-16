class SSEClient {
  constructor() {
    this.eventSource = null;
    this.listeners = new Map();
    this.reconnectAttempts = 0;
    this.maxReconnectAttempts = Infinity;
    this.reconnectDelay = 1000;
    this.maxReconnectDelay = 30000;
    this.shutdownReconnectDelay = 15000;
    this.reconnectTimer = null;
    this.token = null;
    this.serverShutdown = false;
    this.hasReconnected = false;

    // Heartbeat monitoring (server sends keepalive every 5s)
    this.lastHeartbeatTime = null;
    this.heartbeatCheckInterval = null;
    this.heartbeatTimeout = 120000; // 2 minutes
  }

  connect(token) {
    if (this.eventSource) {
      this.disconnect();
    }

    this.token = token;

    // Empty base = same-origin (production). Dev overrides via VITE_API_URL.
    const API_BASE_URL = import.meta.env.VITE_API_URL || "";
    const url = `${API_BASE_URL}/api/events/stream?token=${encodeURIComponent(token)}`;

    this.eventSource = new EventSource(url);

    this.eventSource.onopen = () => {
      const wasReconnecting = this.reconnectAttempts > 0;
      console.log("[SSE] Connected" + (wasReconnecting ? " (reconnected)" : ""));

      this.startHeartbeatMonitor();
      this.emit("connected");

      if (wasReconnecting) {
        this.hasReconnected = true;
        this.emit("reconnected");
      }

      this.reconnectAttempts = 0;
      this.serverShutdown = false;
      if (this.reconnectTimer) {
        clearTimeout(this.reconnectTimer);
        this.reconnectTimer = null;
      }
    };

    this.eventSource.onerror = () => {
      this.emit("error");
      this.emit("disconnected");

      if (this.eventSource.readyState === EventSource.CLOSED) {
        this.handleReconnect(token);
      }
    };

    this.eventSource.onmessage = (event) => {
      this.updateHeartbeat();

      try {
        const data = JSON.parse(event.data);

        if (data.type) {
          this.emit(data.type, data);

          if (data.type === "server_shutdown") {
            console.log("[SSE] Server shutting down, will reconnect in 15s");
            this.serverShutdown = true;
          }
        }
      } catch (err) {
        console.error("[SSE] Failed to parse message:", err);
      }
    };
  }

  handleReconnect(token) {
    if (this.reconnectAttempts >= this.maxReconnectAttempts) {
      console.error("[SSE] Max reconnect attempts reached");
      this.emit("disconnected");
      return;
    }

    this.reconnectAttempts++;

    const delay = this.serverShutdown
      ? this.shutdownReconnectDelay
      : Math.min(
          this.reconnectDelay * Math.pow(2, this.reconnectAttempts - 1),
          this.maxReconnectDelay,
        );

    console.log(`[SSE] Reconnecting in ${delay}ms (attempt ${this.reconnectAttempts})`);
    this.emit("disconnected");

    this.reconnectTimer = setTimeout(() => {
      this.connect(token || this.token);
    }, delay);
  }

  disconnect() {
    this.stopHeartbeatMonitor();

    if (this.eventSource) {
      this.eventSource.close();
      this.eventSource = null;
      this.emit("disconnected");
    }
  }

  on(event, callback) {
    if (!this.listeners.has(event)) {
      this.listeners.set(event, []);
    }
    this.listeners.get(event).push(callback);
  }

  off(event, callback) {
    if (!this.listeners.has(event)) return;

    const callbacks = this.listeners.get(event);
    const index = callbacks.indexOf(callback);
    if (index > -1) {
      callbacks.splice(index, 1);
    }
  }

  emit(event, data) {
    const callbacks = this.listeners.get(event);
    if (!callbacks || callbacks.length === 0) return;

    callbacks.forEach((callback) => {
      try {
        callback(data);
      } catch (err) {
        console.error(`[SSE] Error in handler for ${event}:`, err);
      }
    });
  }

  isConnected() {
    return this.eventSource && this.eventSource.readyState === EventSource.OPEN;
  }

  wasReconnectedSincePageLoad() {
    const result = this.hasReconnected;
    this.hasReconnected = false;
    return result;
  }

  startHeartbeatMonitor() {
    this.stopHeartbeatMonitor();
    this.lastHeartbeatTime = Date.now();

    this.heartbeatCheckInterval = setInterval(() => {
      const timeSinceLastHeartbeat = Date.now() - this.lastHeartbeatTime;

      if (timeSinceLastHeartbeat > this.heartbeatTimeout) {
        console.warn(`[SSE] Heartbeat timeout (${timeSinceLastHeartbeat}ms), forcing reconnect`);

        if (this.eventSource) {
          this.eventSource.close();
          this.eventSource = null;
        }

        this.handleReconnect(this.token);
      }
    }, 30000);
  }

  stopHeartbeatMonitor() {
    if (this.heartbeatCheckInterval) {
      clearInterval(this.heartbeatCheckInterval);
      this.heartbeatCheckInterval = null;
    }
    this.lastHeartbeatTime = null;
  }

  updateHeartbeat() {
    this.lastHeartbeatTime = Date.now();
  }
}

export const sseClient = new SSEClient();
