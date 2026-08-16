import { useEffect, useState, useRef, useCallback } from "preact/hooks";
import { Card } from "../components/Card";
import { StatusBadge } from "../components/StatusBadge";
import { Breadcrumb } from "../components/Breadcrumb";
import { apiClient } from "../lib/api-client";
import { sseClient } from "../lib/sse-client";

export function ContainerDetail({
  clusterId,
  namespace,
  podName,
  containerName,
  init,
}) {
  const [pod, setPod] = useState(null);
  const [container, setContainer] = useState(null);
  const [logs, setLogs] = useState("");
  const [tailLines, setTailLines] = useState(100);
  const [showPreviousLogs, setShowPreviousLogs] = useState(false);
  const [loading, setLoading] = useState(true);
  const [logsLoading, setLogsLoading] = useState(false);
  const [error, setError] = useState(null);

  const isInitContainer = init === "true";

  // Use ref to avoid stale closure in SSE handler
  const loadContainerDetailsRef = useRef(null);

  const loadContainerDetails = useCallback(
    async (silent = false) => {
      try {
        if (!silent) setLoading(true);
        const data = await apiClient.get(
          `/api/clusters/${clusterId}/pods/${namespace}/${podName}`,
        );
        setPod(data.pod);

        // Find the container in either initContainers or containers
        let foundContainer = null;
        if (isInitContainer && data.pod.initContainers) {
          foundContainer = data.pod.initContainers.find(
            (c) => c.name === containerName,
          );
        } else if (data.pod.containers) {
          foundContainer = data.pod.containers.find(
            (c) => c.name === containerName,
          );
        }

        if (!foundContainer) {
          setError(`Container ${containerName} not found in pod`);
        } else {
          setContainer(foundContainer);
        }
        setError(null);
      } catch (err) {
        setError(err.message);
      } finally {
        if (!silent) setLoading(false);
      }
    },
    [clusterId, namespace, podName, containerName, isInitContainer],
  );

  // Keep ref updated
  useEffect(() => {
    loadContainerDetailsRef.current = loadContainerDetails;
  }, [loadContainerDetails]);

  // Initial load
  useEffect(() => {
    loadContainerDetails();
  }, [loadContainerDetails]);

  // Load logs when container or settings change
  useEffect(() => {
    if (container) {
      loadLogs();
    }
  }, [container, tailLines, showPreviousLogs]);

  // SSE listener for workflow progress (DR-0035). v2 has no `pod_status_changed`
  // topic: it deliberately replaced v1's per-deployment watch_pods task with
  // `deploy.await_wave`'s per-poll ctx.progress -> `workflow_progress` (5s cadence),
  // and this page was never told — so it had a dead listener and never refreshed.
  // Cluster-scoped by design: `workflow_progress` carries no pod_name, and
  // over-filtering it would reintroduce exactly that silence. The limitation is
  // real and accepted (DR-0035 decision 2): progress flows only DURING a workflow
  // run, so churn on an idle cluster waits for a manual refresh or a reconnect.
  useEffect(() => {
    const handleWorkflowProgress = (event) => {
      const eventData = event.data || event;
      if (eventData.cluster_id === clusterId) {
        loadContainerDetailsRef.current?.(true); // silent reload
      }
    };

    const handleReconnected = () => {
      loadContainerDetailsRef.current?.(true); // silent reload
    };

    sseClient.on("workflow_progress", handleWorkflowProgress);
    sseClient.on("reconnected", handleReconnected);

    return () => {
      sseClient.off("workflow_progress", handleWorkflowProgress);
      sseClient.off("reconnected", handleReconnected);
    };
  }, [clusterId]);

  const loadLogs = async () => {
    try {
      setLogsLoading(true);
      const params = new URLSearchParams({
        tail_lines: tailLines,
        container: containerName,
      });
      if (showPreviousLogs) {
        params.append("previous", "true");
      }

      const data = await apiClient.get(
        `/api/clusters/${clusterId}/pods/${namespace}/${podName}/logs?${params}`,
      );
      setLogs(data.logs || "No logs available");
    } catch (err) {
      setLogs(`Failed to load logs: ${err.message}`);
    } finally {
      setLogsLoading(false);
    }
  };

  const getContainerState = (state) => {
    if (state.running) {
      return `Running (started ${state.running.startedAt || "unknown"})`;
    } else if (state.waiting) {
      return `Waiting (${state.waiting.reason || "unknown"})`;
    } else if (state.terminated) {
      return `Terminated (${state.terminated.reason || "unknown"}, exit code ${state.terminated.exitCode})`;
    }
    return "Unknown";
  };

  if (loading)
    return <div className="loading">Loading container details...</div>;
  if (error) return <div className="error">Error: {error}</div>;
  if (!container) return <div className="error">Container not found</div>;

  const breadcrumb = [
    { label: "Clusters", href: "/clusters" },
    { label: clusterId, href: `/clusters/${clusterId}` },
    { label: "Pods", href: `/clusters/${clusterId}?tab=pods` },
    {
      label: podName,
      href: `/clusters/${clusterId}/pods/${namespace}/${podName}`,
    },
    { label: containerName },
  ];

  return (
    <div className="page">
      <Breadcrumb path={breadcrumb} />

      <Card
        title={
          <span>
            Container: {containerName}
            {isInitContainer && (
              <span style="margin-left: 0.5rem; font-size: 0.75rem; color: var(--base0); background: var(--base02); padding: 0.125rem 0.5rem; border-radius: 3px;">
                INIT
              </span>
            )}
          </span>
        }
      >
        <div
          className="container-info"
          style="display: grid; grid-template-columns: 1fr 1fr; gap: 0.5rem 2rem;"
        >
          <div className="info-row">
            <span className="label">Pod:</span>
            <span>{podName}</span>
          </div>
          <div className="info-row">
            <span className="label">Namespace:</span>
            <span>{namespace}</span>
          </div>
          <div className="info-row">
            <span className="label">Ready:</span>
            <StatusBadge status={container.ready ? "True" : "False"} />
          </div>
          <div className="info-row">
            <span className="label">Restarts:</span>
            <span>{container.restarts}</span>
          </div>
          <div className="info-row" style="grid-column: 1 / -1;">
            <span className="label">State:</span>
            <span>{getContainerState(container.state)}</span>
          </div>
          <div className="info-row" style="grid-column: 1 / -1;">
            <span className="label">Image:</span>
            <span style="word-break: break-all; font-size: 0.9rem;">
              {container.image}
            </span>
          </div>
        </div>

        {container.ports && container.ports.length > 0 && (
          <div style="margin-top: 1rem; padding-top: 1rem; border-top: 1px solid var(--base01);">
            <h4 style="margin-top: 0; margin-bottom: 0.5rem; font-size: 0.95rem;">
              Ports
            </h4>
            <div style="font-size: 0.9rem;">
              {container.ports
                .map(
                  (p) =>
                    `${p.containerPort}${p.protocol ? `/${p.protocol}` : ""}`,
                )
                .join(", ")}
            </div>
          </div>
        )}

        {container.env && container.env.length > 0 && (
          <div style="margin-top: 1rem; padding-top: 1rem; border-top: 1px solid var(--base01);">
            <h4 style="margin-top: 0; margin-bottom: 0.5rem; font-size: 0.95rem;">
              Environment Variables
            </h4>
            <div style="font-family: monospace; font-size: 0.85rem; max-height: 200px; overflow-y: auto;">
              {container.env.map((envVar, idx) => (
                <div key={idx}>
                  <span style="color: var(--cyan);">{envVar.name}</span>:{" "}
                  <span>{envVar.value || "(not set)"}</span>
                </div>
              ))}
            </div>
          </div>
        )}
      </Card>

      <Card
        title="Logs"
        actions={
          <div style="display: flex; gap: 1rem; align-items: center;">
            <select
              value={tailLines}
              onChange={(e) => setTailLines(Number(e.target.value))}
              style="padding: 0.25rem 0.5rem; background: var(--base02); color: var(--base0); border: 1px solid var(--base01); border-radius: 4px;"
            >
              <option value={50}>50 lines</option>
              <option value={100}>100 lines</option>
              <option value={250}>250 lines</option>
              <option value={500}>500 lines</option>
              <option value={1000}>1000 lines</option>
            </select>
            <label style="display: flex; align-items: center; gap: 0.5rem; cursor: pointer;">
              <input
                type="checkbox"
                checked={showPreviousLogs}
                onChange={(e) => setShowPreviousLogs(e.target.checked)}
                style="cursor: pointer;"
              />
              <span style="font-size: 0.9rem;">Previous</span>
            </label>
            <button
              onClick={loadLogs}
              className="btn-secondary"
              disabled={logsLoading}
            >
              {logsLoading ? "Loading..." : "Refresh"}
            </button>
          </div>
        }
      >
        <pre
          className="logs-container"
          style="background: var(--base03); padding: 1rem; border-radius: 4px; overflow-x: auto; max-height: 600px; font-size: 0.85rem; line-height: 1.4;"
        >
          {logs}
        </pre>
      </Card>
    </div>
  );
}
