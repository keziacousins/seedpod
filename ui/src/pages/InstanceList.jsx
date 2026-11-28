import { useEffect, useState } from "preact/hooks";
import { route } from "preact-router";
import { Table } from "../components/Table";
import { StatusBadge } from "../components/StatusBadge";
import { CreateInstanceModal } from "../components/CreateInstanceModal";
import { apiClient } from "../lib/api-client";
import { sseClient } from "../lib/sse-client";
import { formatDateTime, formatRemaining } from "../lib/time-utils";

// DR-0055: a bare VM from a template. The API sends every instance the key may see;
// "Show destroyed" filters on this side.
// A destroyed instance has no lease left, so its times are not shown as a countdown.
const when = (value, row) =>
  value && row.status !== "destroyed" ? <span title={formatDateTime(value)}>{formatRemaining(value)}</span> : "-";

const columns = [
  { key: "slug", label: "Slug" },
  { key: "status", label: "Status", render: (s) => <StatusBadge status={s} /> },
  { key: "provider", label: "Provider" },
  { key: "environment", label: "Environment" },
  {
    key: "template",
    label: "Template / Size",
    render: (t, row) => `${t} / ${row.size}`,
  },
  { key: "address", label: "Address", render: (a) => a || "-" },
  { key: "expires_at", label: "Expires", render: when },
  { key: "hard_expires_at", label: "Hard limit", render: when },
  { key: "hourly_price", label: "Price", render: (p) => `$${p}/h` },
];

export function InstanceList() {
  const [instances, setInstances] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [showDestroyed, setShowDestroyed] = useState(false);
  const [showCreate, setShowCreate] = useState(false);

  const load = async (showLoader = true) => {
    try {
      if (showLoader) setLoading(true);
      const data = await apiClient.get("/api/instances");
      setInstances(data.instances);
      setError(null);
    } catch (err) {
      setError(err.message);
    } finally {
      if (showLoader) setLoading(false);
    }
  };

  useEffect(() => {
    load();
    const refetch = () => load(false);
    sseClient.on("instance_state_changed", refetch);
    sseClient.on("reconnected", refetch);
    return () => {
      sseClient.off("instance_state_changed", refetch);
      sseClient.off("reconnected", refetch);
    };
  }, []);

  if (loading) return <div className="loading">Loading instances...</div>;

  return (
    <div className="page">
      <div className="page-header">
        <h2>Instances</h2>
        <div style="display: flex; align-items: center; gap: 1rem;">
          <label style="display: flex; align-items: center; gap: 0.5rem; cursor: pointer;">
            <input
              type="checkbox"
              checked={showDestroyed}
              onChange={(e) => setShowDestroyed(e.target.checked)}
            />
            <span>Show destroyed</span>
          </label>
          <button className="btn-primary" onClick={() => setShowCreate(true)}>
            Create instance
          </button>
        </div>
      </div>
      {error && <div className="error-banner">{error}</div>}
      <Table
        columns={columns}
        data={showDestroyed ? instances : instances.filter((i) => i.status !== "destroyed")}
        keyField="id"
        onRowClick={(row) => route(`/instances/${row.id}`)}
      />
      {showCreate && (
        <CreateInstanceModal
          onClose={() => setShowCreate(false)}
          onCreated={(instance) => route(`/instances/${instance.id}`)}
        />
      )}
    </div>
  );
}
