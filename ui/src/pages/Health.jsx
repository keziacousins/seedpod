import { useEffect, useState } from "preact/hooks";
import { Card } from "../components/Card";
import { apiClient } from "../lib/api-client";
import { sseClient } from "../lib/sse-client";
import { formatDateTime } from "../lib/time-utils";

export function Health() {
  const [health, setHealth] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [sseConnected, setSSEConnected] = useState(false);

  useEffect(() => {
    loadHealth();
    const interval = setInterval(loadHealth, 5000); // Refresh every 5 seconds

    // Listen for SSE connection state
    setSSEConnected(sseClient.isConnected());
    const handleConnected = () => setSSEConnected(true);
    const handleDisconnected = () => setSSEConnected(false);

    sseClient.on("connected", handleConnected);
    sseClient.on("disconnected", handleDisconnected);

    return () => {
      clearInterval(interval);
      sseClient.off("connected", handleConnected);
      sseClient.off("disconnected", handleDisconnected);
    };
  }, []);

  const loadHealth = async () => {
    try {
      setLoading(true);
      const data = await apiClient.get("/api/health/detailed");
      setHealth(data);
      setError(null);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  if (loading && !health) {
    return <div className="loading">Loading health status...</div>;
  }

  if (error && !health) {
    return <div className="error">Error: {error}</div>;
  }

  const isHealthy = health?.status === "healthy";

  return (
    <div className="page">
      <h2>System Health</h2>

      <Card title="Overall Status">
        <div style="display: flex; align-items: center; gap: 1rem; margin-bottom: 1rem;">
          <span
            style={{
              width: "16px",
              height: "16px",
              borderRadius: "50%",
              background: isHealthy ? "var(--green)" : "var(--red)",
            }}
          />
          <span style={{ fontSize: "1.25rem", fontWeight: "bold" }}>
            {isHealthy ? "Healthy" : "Unhealthy"}
          </span>
        </div>
        <div className="info-row">
          <span className="label">Service:</span>
          <span>{health?.service || "Seedpod"}</span>
        </div>
        <div className="info-row">
          <span className="label">Version:</span>
          <span>{health?.version || "1.0.0"}</span>
        </div>
        <div className="info-row">
          <span className="label">Timestamp:</span>
          <span>{formatDateTime(health?.timestamp)}</span>
        </div>
      </Card>

      <Card title="Real-time Events (SSE)">
        <div style="display: flex; align-items: center; gap: 1rem;">
          <span
            style={{
              width: "16px",
              height: "16px",
              borderRadius: "50%",
              background: sseConnected ? "var(--green)" : "var(--base01)",
              opacity: sseConnected ? 1 : 0.5,
            }}
          />
          <span style={{ fontSize: "1.125rem" }}>
            {sseConnected ? "Connected" : "Disconnected"}
          </span>
        </div>
        {!sseConnected && (
          <p style="margin-top: 0.5rem; color: var(--base0); opacity: 0.7;">
            Attempting to reconnect...
          </p>
        )}
      </Card>

      {health?.database && (
        <Card title="Database">
          <div className="info-row">
            <span className="label">Status:</span>
            <span
              style={{
                color: health.database.connected
                  ? "var(--green)"
                  : "var(--red)",
              }}
            >
              {health.database.connected ? "Connected" : "Disconnected"}
            </span>
          </div>
          <div className="info-row">
            <span className="label">Clusters:</span>
            <span>{health.database.cluster_count}</span>
          </div>
          <div className="info-row">
            <span className="label">Deployments:</span>
            <span>{health.database.deployment_count}</span>
          </div>
          <div className="info-row">
            <span className="label">API Keys:</span>
            <span>{health.database.api_key_count}</span>
          </div>
        </Card>
      )}

      {health?.scheduler && (
        <Card title="Background Jobs">
          <div className="info-row">
            <span className="label">Status:</span>
            <span
              style={{
                color: health.scheduler.running ? "var(--green)" : "var(--red)",
              }}
            >
              {health.scheduler.running ? "Running" : "Stopped"}
            </span>
          </div>
          <div className="info-row">
            <span className="label">Total Jobs:</span>
            <span>{health.scheduler.job_count}</span>
          </div>
        </Card>
      )}

      {health?.reconciler && (
        <Card title="State Reconciliation">
          <div className="info-row">
            <span className="label">Status:</span>
            <span
              style={{
                color: health.reconciler.running
                  ? "var(--green)"
                  : "var(--yellow)",
              }}
            >
              {health.reconciler.running ? "Running" : "Not Running"}
            </span>
          </div>
          {health.reconciler.last_sync && (
            <div className="info-row">
              <span className="label">Last Sync:</span>
              <span>{formatDateTime(health.reconciler.last_sync)}</span>
            </div>
          )}
        </Card>
      )}
    </div>
  );
}
