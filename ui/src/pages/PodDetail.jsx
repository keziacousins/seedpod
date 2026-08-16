import { useEffect, useState, useRef, useCallback } from "preact/hooks";
import { route } from "preact-router";
import { Card } from "../components/Card";
import { Table } from "../components/Table";
import { StatusBadge } from "../components/StatusBadge";
import { Breadcrumb } from "../components/Breadcrumb";
import { CopyableText } from "../components/CopyableText";
import { apiClient } from "../lib/api-client";
import { sseClient } from "../lib/sse-client";

export function PodDetail({ clusterId, namespace, podName }) {
  const [pod, setPod] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  // Use ref to avoid stale closure in SSE handler
  const loadPodDetailsRef = useRef(null);

  const loadPodDetails = useCallback(
    async (silent = false) => {
      try {
        if (!silent) setLoading(true);
        const data = await apiClient.get(
          `/api/clusters/${clusterId}/pods/${namespace}/${podName}`,
        );
        setPod(data.pod);
        setError(null);
      } catch (err) {
        setError(err.message);
      } finally {
        if (!silent) setLoading(false);
      }
    },
    [clusterId, namespace, podName],
  );

  // Keep ref updated
  useEffect(() => {
    loadPodDetailsRef.current = loadPodDetails;
  }, [loadPodDetails]);

  // Initial load
  useEffect(() => {
    loadPodDetails();
  }, [loadPodDetails]);

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
        loadPodDetailsRef.current?.(true); // silent reload
      }
    };

    const handleReconnected = () => {
      loadPodDetailsRef.current?.(true); // silent reload
    };

    sseClient.on("workflow_progress", handleWorkflowProgress);
    sseClient.on("reconnected", handleReconnected);

    return () => {
      sseClient.off("workflow_progress", handleWorkflowProgress);
      sseClient.off("reconnected", handleReconnected);
    };
  }, [clusterId]);

  const getContainerState = (state) => {
    if (state.running) {
      return "Running";
    } else if (state.waiting) {
      return `Waiting: ${state.waiting.reason || "Unknown"}`;
    } else if (state.terminated) {
      return `Terminated: ${state.terminated.reason || "Unknown"} (exit ${state.terminated.exitCode})`;
    }
    return "Unknown";
  };

  const handleContainerClick = (container, isInit = false) => {
    route(
      `/clusters/${clusterId}/pods/${namespace}/${podName}/containers/${container.name}?init=${isInit}`,
    );
  };

  if (loading) return <div className="loading">Loading pod details...</div>;
  if (error) return <div className="error">Error: {error}</div>;
  if (!pod) return <div className="error">Pod not found</div>;

  const breadcrumb = [
    { label: "Clusters", href: "/clusters" },
    { label: clusterId, href: `/clusters/${clusterId}` },
    { label: "Pods", href: `/clusters/${clusterId}?tab=pods` },
    { label: podName },
  ];

  // Container table columns
  const containerColumns = [
    {
      key: "name",
      label: "Name",
      render: (name, container) => (
        <span>
          {name}
          {container.isInit && (
            <span style="margin-left: 0.5rem; font-size: 0.7rem; color: var(--base0); background: var(--base02); padding: 0.125rem 0.4rem; border-radius: 3px;">
              INIT
            </span>
          )}
        </span>
      ),
    },
    {
      key: "ready",
      label: "Ready",
      render: (ready) => <StatusBadge status={ready ? "True" : "False"} />,
    },
    {
      key: "state",
      label: "State",
      render: (state) => getContainerState(state),
    },
    { key: "restarts", label: "Restarts" },
    {
      key: "image",
      label: "Image",
      render: (image) => {
        // Shorten image name - show just repo:tag
        if (!image) return "-";
        const parts = image.split("/");
        return parts[parts.length - 1];
      },
    },
  ];

  // Combine init containers and regular containers
  const allContainers = [
    ...(pod.initContainers || []).map((c) => ({ ...c, isInit: true })),
    ...(pod.containers || []).map((c) => ({ ...c, isInit: false })),
  ];

  // Get readiness conditions
  const readinessConditions =
    pod.conditions?.filter((c) =>
      [
        "PodReadyToStartContainers",
        "Initialized",
        "Ready",
        "ContainersReady",
      ].includes(c.type),
    ) || [];

  return (
    <div className="page">
      <Breadcrumb path={breadcrumb} />

      <Card title={`Pod: ${podName}`}>
        <div
          className="pod-info"
          style="display: grid; grid-template-columns: 1fr 1fr; gap: 0.5rem 2rem;"
        >
          <div className="info-row">
            <span className="label">Status:</span>
            <StatusBadge status={pod.status} />
          </div>
          <div className="info-row">
            <span className="label">Namespace:</span>
            <span>{pod.namespace}</span>
          </div>
          <div className="info-row">
            <span className="label">Age:</span>
            <span>{pod.age}</span>
          </div>
          <div className="info-row">
            <span className="label">Node:</span>
            <span>{pod.node || "-"}</span>
          </div>
          <div className="info-row">
            <span className="label">Pod IP:</span>
            {pod.ip ? <CopyableText value={pod.ip} /> : <span>-</span>}
          </div>
          <div className="info-row">
            <span className="label">Host IP:</span>
            {pod.hostIP ? <CopyableText value={pod.hostIP} /> : <span>-</span>}
          </div>
        </div>

        {((pod.labels && Object.keys(pod.labels).length > 0) ||
          readinessConditions.length > 0) && (
          <div style="margin-top: 1.5rem; padding-top: 1rem; border-top: 1px solid var(--base01); display: grid; grid-template-columns: 1fr 1fr; gap: 2rem;">
            {pod.labels && Object.keys(pod.labels).length > 0 && (
              <div>
                <h4 style="margin-top: 0; margin-bottom: 0.5rem; font-size: 0.95rem;">
                  Labels
                </h4>
                <div style="font-family: monospace; font-size: 0.85rem;">
                  {Object.entries(pod.labels).map(([key, value]) => (
                    <div key={key}>
                      <span style="color: var(--cyan);">{key}</span>:{" "}
                      <span>{value}</span>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {readinessConditions.length > 0 && (
              <div>
                <h4 style="margin-top: 0; margin-bottom: 0.5rem; font-size: 0.95rem;">
                  Readiness Conditions
                </h4>
                <div style="display: grid; gap: 0.5rem;">
                  {readinessConditions.map((condition, idx) => (
                    <div
                      key={idx}
                      style="display: flex; align-items: center; gap: 1rem; font-size: 0.9rem;"
                    >
                      <span style="min-width: 140px; color: var(--base0);">
                        {condition.type}:
                      </span>
                      <StatusBadge status={condition.status} />
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
        )}
      </Card>

      <Card title="Containers">
        <Table
          columns={containerColumns}
          data={allContainers}
          onRowClick={(container) =>
            handleContainerClick(container, container.isInit)
          }
          emptyMessage="No containers found"
        />
      </Card>
    </div>
  );
}
