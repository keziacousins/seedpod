import { useEffect, useState, useCallback } from "preact/hooks";
import { route } from "preact-router";
import { Card } from "../components/Card";
import { Table } from "../components/Table";
import { StatusBadge } from "../components/StatusBadge";
import { Breadcrumb } from "../components/Breadcrumb";
import { InfoGrid, InfoGridRow } from "../components/InfoGrid";
import { Guid } from "../components/Guid";
import { RestoreSnapshotModal } from "../components/RestoreSnapshotModal";
import { ConfirmModal } from "../components/ConfirmModal";
import { apiClient } from "../lib/api-client";
import { formatDateTime } from "../lib/time-utils";

function formatBytes(bytes) {
  if (!bytes || bytes === 0) return "0 B";
  const k = 1024;
  const sizes = ["B", "KB", "MB", "GB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + " " + sizes[i];
}

export function SnapshotDetail({ snapshotId }) {
  const [snapshot, setSnapshot] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [showRestoreModal, setShowRestoreModal] = useState(false);
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false);
  const [deleting, setDeleting] = useState(false);

  const loadSnapshot = useCallback(async () => {
    try {
      setLoading(true);
      const data = await apiClient.get(`/api/snapshots/${snapshotId}`);
      setSnapshot(data);
      setError(null);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }, [snapshotId]);

  useEffect(() => {
    loadSnapshot();
  }, [loadSnapshot]);

  const handleDelete = async () => {
    try {
      setDeleting(true);
      await apiClient.delete(`/api/snapshots/${snapshotId}`);
      route("/snapshots");
    } catch (err) {
      setDeleting(false);
      throw err; // Re-throw to let ConfirmModal handle it
    }
  };

  if (loading)
    return <div className="loading">Loading snapshot details...</div>;
  if (error) return <div className="error">Error: {error}</div>;
  if (!snapshot) return <div className="error">Snapshot not found</div>;

  const breadcrumb = [
    { label: "Snapshots", href: "/snapshots" },
    { label: snapshot.name },
  ];

  const serviceColumns = [
    {
      key: "service_name",
      label: "Service",
      render: (value) => <span style="font-weight: 500;">{value}</span>,
    },
    {
      key: "persistence_type",
      label: "Type",
      render: (value) => (
        <span className="status-badge status-muted">{value}</span>
      ),
    },
    {
      key: "database",
      label: "Database",
      render: (value) => value || "-",
    },
    {
      key: "size_bytes",
      label: "Size",
      render: (value) => formatBytes(value),
    },
  ];

  const actions = (
    <>
      <button onClick={() => setShowRestoreModal(true)} className="btn-primary">
        Restore to Cluster
      </button>
      <button
        onClick={() => setShowDeleteConfirm(true)}
        className="btn-danger"
        disabled={deleting}
      >
        {deleting ? "Deleting..." : "Delete"}
      </button>
    </>
  );

  return (
    <div className="page">
      <Breadcrumb path={breadcrumb} />

      <Card title={`Snapshot: ${snapshot.name}`} actions={actions}>
        <InfoGrid>
          <InfoGridRow label="ID">
            <Guid value={snapshot.id} />
          </InfoGridRow>
          {snapshot.is_auto && (
            <InfoGridRow label="Type">
              <span
                className="status-badge status-muted"
                title="Auto-snapshot created before TTL expiry or destruction"
              >
                Auto-snapshot
              </span>
            </InfoGridRow>
          )}
          <InfoGridRow label="Description">
            <span>{snapshot.description || "-"}</span>
          </InfoGridRow>
          <InfoGridRow label="Source Cluster">
            <Guid
              value={snapshot.source_cluster_id}
              linkTo={`/clusters/${snapshot.source_cluster_id}`}
            />
            {snapshot.source_cluster_slug && (
              <span style="margin-left: 0.5rem; color: var(--color-text-muted);">
                ({snapshot.source_cluster_slug})
              </span>
            )}
          </InfoGridRow>
          <InfoGridRow label="Branch">
            <span>{snapshot.branch || "-"}</span>
          </InfoGridRow>
          <InfoGridRow label="Deployment Profile">
            <span
              onClick={() =>
                route(`/config/profiles/${snapshot.deployment_profile}`)
              }
              className="config-pill"
              style="cursor: pointer;"
            >
              {snapshot.deployment_profile}
            </span>
          </InfoGridRow>
          <InfoGridRow label="Total Size">
            <span>{formatBytes(snapshot.total_size_bytes)}</span>
          </InfoGridRow>
          <InfoGridRow label="Created By">
            <span>{snapshot.created_by}</span>
          </InfoGridRow>
          <InfoGridRow label="Created At">
            <span>{formatDateTime(snapshot.created_at)}</span>
          </InfoGridRow>
        </InfoGrid>
      </Card>

      <div style="margin-top: 2rem;">
        <h3 style="margin-bottom: 1rem;">Snapshotted Services</h3>
        <Table
          columns={serviceColumns}
          data={snapshot.services || []}
          keyField="service_name"
          emptyMessage="No services in this snapshot."
        />
      </div>

      {/* Restore Modal */}
      {showRestoreModal && (
        <RestoreSnapshotModal
          snapshot={snapshot}
          onClose={() => setShowRestoreModal(false)}
          onRestored={() => {
            setShowRestoreModal(false);
            // Optionally navigate somewhere
          }}
        />
      )}

      {/* Delete Confirmation Modal */}
      {showDeleteConfirm && (
        <ConfirmModal
          title="Delete Snapshot"
          message={`Are you sure you want to delete snapshot "${snapshot.name}"? This cannot be undone.`}
          confirmLabel="Delete"
          onConfirm={handleDelete}
          onCancel={() => setShowDeleteConfirm(false)}
          loading={deleting}
        />
      )}
    </div>
  );
}
