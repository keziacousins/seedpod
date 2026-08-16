import { useState, useEffect } from "preact/hooks";
import { Modal } from "./Modal";
import { apiClient } from "../lib/api-client";

export function CreateSnapshotModal({
  onClose,
  onCreated,
  preselectedClusterId,
}) {
  const [clusters, setClusters] = useState([]);
  const [loading, setLoading] = useState(true);
  const [creating, setCreating] = useState(false);
  const [error, setError] = useState(null);

  const [form, setForm] = useState({
    cluster_id: preselectedClusterId || "",
    name: "",
    description: "",
  });

  useEffect(() => {
    loadActiveClusters();
  }, []);

  const loadActiveClusters = async () => {
    try {
      setLoading(true);
      const data = await apiClient.get("/api/clusters?status=active");
      setClusters(data.clusters);

      // If preselected cluster is provided, verify it's in the list
      if (preselectedClusterId) {
        const found = data.find((c) => c.id === preselectedClusterId);
        if (!found) {
          console.warn("Preselected cluster not found or not active");
        }
      }
    } catch (err) {
      console.error("Failed to load clusters:", err);
    } finally {
      setLoading(false);
    }
  };

  const handleCreate = async () => {
    setError(null);

    if (!form.cluster_id) {
      setError("Please select a cluster");
      return;
    }
    if (!form.name.trim()) {
      setError("Please enter a snapshot name");
      return;
    }

    try {
      setCreating(true);
      await apiClient.post("/api/snapshots", {
        cluster_id: form.cluster_id,
        name: form.name.trim(),
        description: form.description.trim() || null,
      });

      if (onCreated) {
        onCreated();
      }
      onClose();
    } catch (err) {
      setError(err.message);
    } finally {
      setCreating(false);
    }
  };

  // Get selected cluster info for display
  const selectedCluster = clusters.find((c) => c.id === form.cluster_id);

  return (
    <Modal onClose={onClose} title="Create Database Snapshot">
      {loading ? (
        <p>Loading clusters...</p>
      ) : clusters.length === 0 ? (
        <p style="color: var(--color-text-muted);">
          No active clusters available. Snapshots can only be created from
          active clusters.
        </p>
      ) : (
        <>
          <div className="form-group">
            <label>Source Cluster *</label>
            <select
              value={form.cluster_id}
              onChange={(e) => setForm({ ...form, cluster_id: e.target.value })}
              disabled={!!preselectedClusterId}
            >
              <option value="">Select a cluster...</option>
              {clusters.map((cluster) => (
                <option key={cluster.id} value={cluster.id}>
                  {cluster.slug || cluster.id.slice(0, 8)} -{" "}
                  {cluster.branch || "no branch"}
                </option>
              ))}
            </select>
            {selectedCluster && (
              <small style="color: var(--color-text-muted);">
                Profile:{" "}
                {selectedCluster.provider_config?.deployment_profile ||
                  "unknown"}
              </small>
            )}
          </div>

          <div className="form-group">
            <label>Snapshot Name *</label>
            <input
              type="text"
              value={form.name}
              onChange={(e) => setForm({ ...form, name: e.target.value })}
              placeholder="e.g., pre-migration-backup"
              maxLength={100}
            />
            <small style="color: var(--color-text-muted);">
              A descriptive name to identify this snapshot
            </small>
          </div>

          <div className="form-group">
            <label>Description</label>
            <textarea
              value={form.description}
              onChange={(e) =>
                setForm({ ...form, description: e.target.value })
              }
              placeholder="Optional notes about this snapshot..."
              rows={2}
              maxLength={500}
            />
          </div>

          <div style="margin: 1rem 0; padding: 0.75rem; background: var(--color-bg-muted); border-radius: 4px; font-size: 0.875rem;">
            <strong>Note:</strong> Snapshot creation runs in the background and
            may take several minutes for large databases. All services with
            persistence configuration will be included.
          </div>
        </>
      )}

      {error && <div className="modal-error">{error}</div>}

      <div className="modal-actions">
        <button className="btn-secondary" onClick={onClose} disabled={creating}>
          Cancel
        </button>
        <button
          className="btn-primary"
          onClick={handleCreate}
          disabled={
            creating ||
            loading ||
            clusters.length === 0 ||
            !form.cluster_id ||
            !form.name.trim()
          }
        >
          {creating ? "Creating..." : "Create Snapshot"}
        </button>
      </div>
    </Modal>
  );
}
