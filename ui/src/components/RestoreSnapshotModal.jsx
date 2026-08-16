import { useState, useEffect } from "preact/hooks";
import { Modal } from "./Modal";
import { apiClient } from "../lib/api-client";

function formatBytes(bytes) {
  if (!bytes || bytes === 0) return "0 B";
  const k = 1024;
  const sizes = ["B", "KB", "MB", "GB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + " " + sizes[i];
}

export function RestoreSnapshotModal({ snapshot, onClose, onRestored }) {
  const [clusters, setClusters] = useState([]);
  const [loading, setLoading] = useState(true);
  const [restoring, setRestoring] = useState(false);

  const [targetClusterId, setTargetClusterId] = useState("");
  const [selectedServices, setSelectedServices] = useState({});
  const [confirmed, setConfirmed] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    loadActiveClusters();
    // Initialize all services as selected
    if (snapshot?.services) {
      const initial = {};
      snapshot.services.forEach((s) => {
        initial[s.service_name] = true;
      });
      setSelectedServices(initial);
    }
  }, [snapshot]);

  const loadActiveClusters = async () => {
    try {
      setLoading(true);
      const data = await apiClient.get("/api/clusters?status=active");
      setClusters(data.clusters);
    } catch (err) {
      console.error("Failed to load clusters:", err);
    } finally {
      setLoading(false);
    }
  };

  const handleServiceToggle = (serviceName) => {
    setSelectedServices((prev) => ({
      ...prev,
      [serviceName]: !prev[serviceName],
    }));
  };

  const handleSelectAll = () => {
    const all = {};
    snapshot.services.forEach((s) => {
      all[s.service_name] = true;
    });
    setSelectedServices(all);
  };

  const handleSelectNone = () => {
    const none = {};
    snapshot.services.forEach((s) => {
      none[s.service_name] = false;
    });
    setSelectedServices(none);
  };

  const getSelectedServiceNames = () => {
    return Object.entries(selectedServices)
      .filter(([_, selected]) => selected)
      .map(([name, _]) => name);
  };

  const handleRestore = async () => {
    setError(null);

    if (!targetClusterId) {
      setError("Please select a target cluster");
      return;
    }

    const servicesToRestore = getSelectedServiceNames();
    if (servicesToRestore.length === 0) {
      setError("Please select at least one service to restore");
      return;
    }

    if (!confirmed) {
      setError("Please confirm that you understand this will overwrite data");
      return;
    }

    try {
      setRestoring(true);

      // If all services selected, don't pass services filter (restore all)
      const allSelected = servicesToRestore.length === snapshot.services.length;

      await apiClient.post(`/api/snapshots/${snapshot.id}/restore`, {
        cluster_id: targetClusterId,
        services: allSelected ? null : servicesToRestore,
        run_migrations: true,
      });

      onRestored();
    } catch (err) {
      setError(err.message);
    } finally {
      setRestoring(false);
    }
  };

  const selectedCount = getSelectedServiceNames().length;
  const totalCount = snapshot?.services?.length || 0;

  return (
    <Modal onClose={onClose} title="Restore Snapshot">
      <div style="margin-bottom: 1rem;">
        <p>
          <strong>Snapshot:</strong> {snapshot.name}
        </p>
        {snapshot.description && (
          <p style="color: var(--color-text-muted); font-size: 0.875rem;">
            {snapshot.description}
          </p>
        )}
      </div>

      {loading ? (
        <p>Loading clusters...</p>
      ) : clusters.length === 0 ? (
        <p style="color: var(--color-text-muted);">
          No active clusters available. Snapshots can only be restored to active
          clusters.
        </p>
      ) : (
        <>
          <div className="form-group">
            <label>Target Cluster *</label>
            <select
              value={targetClusterId}
              onChange={(e) => setTargetClusterId(e.target.value)}
            >
              <option value="">Select a cluster...</option>
              {clusters.map((cluster) => (
                <option key={cluster.id} value={cluster.id}>
                  {cluster.slug || cluster.id.slice(0, 8)} -{" "}
                  {cluster.branch || "no branch"}
                </option>
              ))}
            </select>
          </div>

          <div className="form-group">
            <label>Services to Restore</label>
            <div style="display: flex; gap: 0.5rem; margin-bottom: 0.5rem;">
              <button
                type="button"
                className="btn-secondary"
                style="font-size: 0.75rem; padding: 0.25rem 0.5rem;"
                onClick={handleSelectAll}
              >
                Select All
              </button>
              <button
                type="button"
                className="btn-secondary"
                style="font-size: 0.75rem; padding: 0.25rem 0.5rem;"
                onClick={handleSelectNone}
              >
                Select None
              </button>
            </div>
            <div className="service-checkbox-list">
              {snapshot.services.map((service) => {
                console.log("Rendering service:", service);
                return (
                  <div
                    key={service.service_name}
                    className="service-checkbox-item"
                    onClick={() => handleServiceToggle(service.service_name)}
                  >
                    <input
                      type="checkbox"
                      checked={selectedServices[service.service_name] || false}
                      onChange={() => handleServiceToggle(service.service_name)}
                    />
                    <span className="service-info">
                      <span className="service-name">
                        {service.service_name}
                      </span>
                      {service.database && (
                        <span className="service-meta">
                          ({service.database})
                        </span>
                      )}
                    </span>
                    <span className="service-size">
                      {formatBytes(service.size_bytes)}
                    </span>
                  </div>
                );
              })}
            </div>
            <small style="color: var(--color-text-muted);">
              {selectedCount} of {totalCount} services selected
            </small>
          </div>

          <div style="margin: 1rem 0; padding: 0.75rem; background: var(--yellow); color: var(--base03); border-radius: 4px;">
            <strong>Warning:</strong> This will overwrite existing data in the
            selected cluster's databases.
          </div>

          <label style="display: flex; align-items: center; cursor: pointer; margin-bottom: 1rem;">
            <input
              type="checkbox"
              checked={confirmed}
              onChange={(e) => setConfirmed(e.target.checked)}
              style="margin-right: 0.5rem;"
            />
            <span>I understand this will overwrite data</span>
          </label>
        </>
      )}

      {error && <div className="modal-error">{error}</div>}

      <div className="modal-actions">
        <button
          className="btn-secondary"
          onClick={onClose}
          disabled={restoring}
        >
          Cancel
        </button>
        <button
          className="btn-primary"
          onClick={handleRestore}
          disabled={
            restoring ||
            loading ||
            clusters.length === 0 ||
            !targetClusterId ||
            selectedCount === 0 ||
            !confirmed
          }
        >
          {restoring
            ? "Restoring..."
            : `Restore ${selectedCount} Service${selectedCount !== 1 ? "s" : ""}`}
        </button>
      </div>
    </Modal>
  );
}
