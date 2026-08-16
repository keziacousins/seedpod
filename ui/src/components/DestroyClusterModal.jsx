import { useState } from "preact/hooks";
import { Modal } from "./Modal";
import { apiClient } from "../lib/api-client";

export function DestroyClusterModal({ cluster, onClose, onDestroyed }) {
  const [destroying, setDestroying] = useState(false);
  const [snapshotBeforeDestroy, setSnapshotBeforeDestroy] = useState(false);
  const [error, setError] = useState(null);

  const isDiscovered = cluster?.origin === "discovered";

  const handleDestroy = async () => {
    try {
      setError(null);
      setDestroying(true);

      // Build query params
      const params = new URLSearchParams();
      if (isDiscovered) {
        params.append("force", "true");
      }
      if (snapshotBeforeDestroy) {
        params.append("snapshot_before_destroy", "true");
      }

      const queryString = params.toString();
      const url = `/api/clusters/${cluster.id}${queryString ? `?${queryString}` : ""}`;

      await apiClient.delete(url);

      if (onDestroyed) {
        onDestroyed();
      }
      onClose();
    } catch (err) {
      setError(err.message);
      setDestroying(false);
    }
  };

  return (
    <Modal onClose={onClose} title="Destroy Cluster">
      <p style="margin-bottom: 1rem;">
        {isDiscovered ? (
          <>
            This is a <strong>discovered cluster</strong> (not created by
            infra-manager). Destroying it will force-delete all associated
            resources.
          </>
        ) : (
          <>
            Are you sure you want to destroy cluster{" "}
            <strong>{cluster.slug || cluster.id.slice(0, 8)}</strong>?
          </>
        )}
      </p>

      <div style="margin: 1rem 0; padding: 0.75rem; background: var(--color-bg-muted); border-radius: 4px;">
        <label style="display: flex; align-items: center; gap: 0.5rem; cursor: pointer;">
          <input
            type="checkbox"
            checked={snapshotBeforeDestroy}
            onChange={(e) => setSnapshotBeforeDestroy(e.target.checked)}
            disabled={destroying}
          />
          <span>Create snapshot before destroying</span>
        </label>
        <small style="display: block; margin-top: 0.5rem; color: var(--color-text-muted);">
          Preserves database data for future restoration. Only available for
          healthy clusters with persistence configured.
        </small>
      </div>

      <div style="margin: 1rem 0; padding: 0.75rem; background: rgba(239, 68, 68, 0.15); border-radius: 4px; border: 1px solid var(--color-danger, #ef4444); color: var(--color-danger, #ef4444);">
        <strong>Warning:</strong> This action cannot be undone. All cluster
        resources will be permanently deleted.
      </div>

      {error && <div className="modal-error">{error}</div>}

      <div className="modal-actions">
        <button
          className="btn-secondary"
          onClick={onClose}
          disabled={destroying}
        >
          Cancel
        </button>
        <button
          className="btn-danger"
          onClick={handleDestroy}
          disabled={destroying}
        >
          {destroying
            ? snapshotBeforeDestroy
              ? "Snapshotting & Destroying..."
              : "Destroying..."
            : "Destroy Cluster"}
        </button>
      </div>
    </Modal>
  );
}
