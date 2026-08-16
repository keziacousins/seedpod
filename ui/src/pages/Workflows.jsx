import { useEffect, useState } from "preact/hooks";
import { Table } from "../components/Table";
import { TabNav } from "../components/TabNav";
import { Guid } from "../components/Guid";
import { StatusBadge } from "../components/StatusBadge";
import { formatTime } from "../lib/time-utils";
import { apiClient } from "../lib/api-client";
import { sseClient } from "../lib/sse-client";

// Replaces the v1 Jobs page (GET /api/jobs is gone). Runs come from the
// workflow_runs table via GET /api/workflows ({workflows: [...]}, DR-0017);
// the schedules half is the timers outbox via GET /api/timers (DR-0003).
export function Workflows() {
  const [runs, setRuns] = useState([]);
  const [timers, setTimers] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [activeTab, setActiveTab] = useState("runs");

  const load = async (showLoader = true) => {
    try {
      if (showLoader) setLoading(true);
      const [w, t] = await Promise.all([
        apiClient.get("/api/workflows"),
        apiClient.get("/api/timers"),
      ]);
      setRuns(w.workflows || []);
      setTimers(t.timers || []);
      setError(null);
    } catch (err) {
      setError(err.message);
    } finally {
      if (showLoader) setLoading(false);
    }
  };

  useEffect(() => {
    load();
  }, []);

  // Refetch on run lifecycle + progress events (job_* wire topics are kept for
  // the UI; workflow_progress is the new live signal). Reconnect refetches too.
  useEffect(() => {
    const refetch = () => load(false);
    const refetchLoud = () => load(true);
    const onConnected = () => {
      if (sseClient.wasReconnectedSincePageLoad()) load(true);
    };
    const topics = [
      "job_started",
      "job_completed",
      "job_failed",
      "workflow_progress",
    ];
    topics.forEach((t) => sseClient.on(t, refetch));
    sseClient.on("reconnected", refetchLoud);
    sseClient.on("connected", onConnected);
    return () => {
      topics.forEach((t) => sseClient.off(t, refetch));
      sseClient.off("reconnected", refetchLoud);
      sseClient.off("connected", onConnected);
    };
  }, []);

  const formatFireAt = (fireAt) => {
    if (!fireAt) return "N/A";
    const diffMs = new Date(fireAt) - new Date();
    if (diffMs < 0) return "Overdue";
    if (diffMs < 60000) return "< 1 min";
    if (diffMs < 3600000) return `in ${Math.floor(diffMs / 60000)} min`;
    if (diffMs < 86400000) return `in ${Math.floor(diffMs / 3600000)} hr`;
    return `in ${Math.floor(diffMs / 86400000)} days`;
  };

  const formatDuration = (row) => {
    if (!row.started_at) return "-";
    if (!row.finished_at) {
      return row.status === "running" || row.status === "compensating" ? (
        <span style="color: var(--text-light);">In progress…</span>
      ) : (
        "-"
      );
    }
    const ms = new Date(row.finished_at) - new Date(row.started_at);
    if (ms < 1000) return `${ms}ms`;
    if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
    return `${(ms / 60000).toFixed(1)}m`;
  };

  const runColumns = [
    {
      key: "workflow",
      label: "Workflow",
      render: (value) => <span style="color: var(--text);">{value}</span>,
    },
    {
      key: "status",
      label: "Status",
      render: (status) => <StatusBadge status={status} />,
    },
    {
      key: "cluster_id",
      label: "Cluster",
      render: (clusterId) =>
        clusterId ? (
          <Guid value={clusterId} linkTo={`/clusters/${clusterId}`} />
        ) : (
          <span style="color: var(--text-light);">-</span>
        ),
    },
    {
      key: "deployment_id",
      label: "Deployment",
      render: (deploymentId) =>
        deploymentId ? (
          <Guid value={deploymentId} linkTo={`/deployments/${deploymentId}`} />
        ) : (
          <span style="color: var(--text-light);">-</span>
        ),
    },
    {
      key: "started_at",
      label: "Started",
      render: (value, row) => (
        <span style="color: var(--text); font-size: 0.9em;">
          {value ? formatTime(value) : formatTime(row.created_at)}
        </span>
      ),
    },
    {
      key: "duration",
      label: "Duration",
      render: (_, row) => formatDuration(row),
    },
    {
      key: "detail",
      label: "Detail",
      render: (_, row) => {
        const bits = [];
        if (row.failed_step) {
          bits.push(
            <span
              key="step"
              className="status-badge status-red"
              title={`Failed at step: ${row.failed_step}`}
            >
              ✗ {row.failed_step}
            </span>,
          );
        }
        if (row.undo_incomplete) {
          bits.push(
            <span
              key="undo"
              className="status-badge status-bright-orange"
              title="Compensation did not fully complete — a leak may remain"
            >
              undo incomplete
            </span>,
          );
        }
        if (row.error && !row.failed_step) {
          bits.push(
            <span
              key="err"
              style="color: var(--error); font-size: 0.85em;"
              title={row.error}
            >
              {row.error.slice(0, 50)}
              {row.error.length > 50 ? "…" : ""}
            </span>,
          );
        }
        if (bits.length === 0)
          return <span style="color: var(--text-light);">-</span>;
        return (
          <div style="display: flex; flex-wrap: wrap; align-items: center; gap: 0.35rem;">
            {bits}
          </div>
        );
      },
    },
  ];

  const timerColumns = [
    {
      key: "timer_key",
      label: "Timer",
      render: (value) => <span style="color: var(--text);">{value}</span>,
    },
    {
      key: "aggregate_type",
      label: "Aggregate",
      render: (type, row) => (
        <div style="display: flex; align-items: center; gap: 0.5rem;">
          <span style="color: var(--text-light); font-size: 0.85em;">
            {type}:
          </span>
          {type === "cluster" ? (
            <Guid
              value={row.aggregate_id}
              linkTo={`/clusters/${row.aggregate_id}`}
            />
          ) : (
            <Guid value={row.aggregate_id} />
          )}
        </div>
      ),
    },
    {
      key: "fire_at",
      label: "Fires",
      render: (value) => (
        <span style="color: var(--text);" title={value}>
          {formatFireAt(value)}
        </span>
      ),
    },
  ];

  if (loading) return <div className="loading">Loading workflows...</div>;

  return (
    <div className="page">
      <div className="page-header">
        <h2>Workflows</h2>
      </div>

      {error && <div className="error-banner">{error}</div>}

      <TabNav
        tabs={[
          { id: "runs", label: "Runs", count: runs.length },
          { id: "schedules", label: "Schedules", count: timers.length },
        ]}
        activeTab={activeTab}
        onTabChange={setActiveTab}
      />

      <div>
        {activeTab === "runs" &&
          (runs.length === 0 ? (
            <div style="padding: 1rem; color: var(--text-light); font-style: italic;">
              No workflow runs
            </div>
          ) : (
            <Table columns={runColumns} data={runs} keyField="id" />
          ))}

        {activeTab === "schedules" &&
          (timers.length === 0 ? (
            <div style="padding: 1rem; color: var(--text-light); font-style: italic;">
              No scheduled timers
            </div>
          ) : (
            <Table
              columns={timerColumns}
              data={timers}
              keyField="timer_key"
            />
          ))}
      </div>
    </div>
  );
}
