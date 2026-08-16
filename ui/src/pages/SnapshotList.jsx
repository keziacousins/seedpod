import { useEffect, useState, useCallback } from "preact/hooks";
import { route } from "preact-router";
import { Table } from "../components/Table";
import { Guid } from "../components/Guid";
import { DeleteButton } from "../components/DeleteButton";
import { CreateSnapshotModal } from "../components/CreateSnapshotModal";
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

export function SnapshotList() {
  const [snapshots, setSnapshots] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [showCreateModal, setShowCreateModal] = useState(false);
  const [snapshotToDelete, setSnapshotToDelete] = useState(null);
  const [deleting, setDeleting] = useState(false);

  // Filters
  const [branchFilter, setBranchFilter] = useState("");
  const [profileFilter, setProfileFilter] = useState("");

  // Available filter options (derived from data)
  const [branches, setBranches] = useState([]);
  const [profiles, setProfiles] = useState([]);

  const loadSnapshots = useCallback(
    async (showLoader = true) => {
      try {
        if (showLoader) setLoading(true);

        // Build query params
        const params = new URLSearchParams();
        if (branchFilter) params.append("branch", branchFilter);
        if (profileFilter) params.append("profile", profileFilter);

        const queryString = params.toString();
        const url = `/api/snapshots${queryString ? `?${queryString}` : ""}`;

        const data = await apiClient.get(url);
        const snapshots = data.snapshots;
        setSnapshots(snapshots);

        // Extract unique branches and profiles for filters
        const uniqueBranches = [
          ...new Set(snapshots.map((s) => s.branch).filter(Boolean)),
        ];
        const uniqueProfiles = [
          ...new Set(snapshots.map((s) => s.deployment_profile).filter(Boolean)),
        ];
        setBranches(uniqueBranches);
        setProfiles(uniqueProfiles);

        setError(null);
      } catch (err) {
        setError(err.message);
      } finally {
        if (showLoader) setLoading(false);
      }
    },
    [branchFilter, profileFilter],
  );

  useEffect(() => {
    loadSnapshots();
  }, [loadSnapshots]);

  const handleDelete = async () => {
    if (!snapshotToDelete) return;

    try {
      setDeleting(true);
      await apiClient.delete(`/api/snapshots/${snapshotToDelete.id}`);
      setSnapshotToDelete(null);
      loadSnapshots();
    } catch (err) {
      setDeleting(false);
      throw err; // Re-throw to let ConfirmModal handle it
    } finally {
      setDeleting(false);
    }
  };

  const columns = [
    {
      key: "name",
      label: "Name",
      render: (value, row) => (
        <span style="display: flex; align-items: center; gap: 0.5rem;">
          <span style="font-weight: 500; color: var(--color-primary);">
            {value}
          </span>
          {row.is_auto && (
            <span
              className="status-badge status-muted"
              title="Auto-snapshot created before TTL expiry or destruction"
            >
              Auto
            </span>
          )}
        </span>
      ),
    },
    {
      key: "source_cluster_slug",
      label: "Source Cluster",
      render: (value, row) =>
        row.source_cluster_id ? (
          <span style="display: flex; align-items: center; gap: 0.5rem;">
            {value && <span>{value}</span>}
            <Guid
              value={row.source_cluster_id}
              linkTo={`/clusters/${row.source_cluster_id}`}
            />
          </span>
        ) : (
          "-"
        ),
    },
    {
      key: "branch",
      label: "Branch",
      render: (value) =>
        value || <span style="color: var(--color-text-muted);">-</span>,
    },
    {
      key: "deployment_profile",
      label: "Profile",
      render: (value) => (
        <span
          onClick={(e) => {
            e.stopPropagation();
            route(`/config/profiles/${value}`);
          }}
          className="config-pill"
          style="cursor: pointer;"
        >
          {value}
        </span>
      ),
    },
    {
      key: "total_size_bytes",
      label: "Size",
      render: (value) => formatBytes(value),
    },
    {
      key: "created_by",
      label: "Created By",
    },
    {
      key: "created_at",
      label: "Created",
      render: (value) => (value ? formatDateTime(value) : "-"),
    },
    {
      key: "actions",
      label: "",
      render: (_, row) => (
        <div style="display: flex; gap: 0.5rem; align-items: center;">
          <DeleteButton
            onClick={() => setSnapshotToDelete(row)}
            title="Delete snapshot"
          />
        </div>
      ),
    },
  ];

  if (loading) return <div className="loading">Loading snapshots...</div>;

  return (
    <div className="page">
      <div className="page-header">
        <h2>Database Snapshots</h2>
        <button
          className="btn-primary"
          onClick={() => setShowCreateModal(true)}
        >
          Create Snapshot
        </button>
      </div>

      {error && <div className="error-banner">{error}</div>}

      <p style="color: var(--color-text-muted); margin-bottom: 1rem;">
        Snapshots preserve database state from ephemeral clusters. Use them to
        restore data when creating new clusters.
      </p>

      {/* Filters */}
      <div style="display: flex; gap: 1rem; margin-bottom: 1rem;">
        <div className="form-group" style="margin: 0; flex: 0 0 200px;">
          <select
            value={branchFilter}
            onChange={(e) => setBranchFilter(e.target.value)}
            style="width: 100%;"
          >
            <option value="">All Branches</option>
            {branches.map((branch) => (
              <option key={branch} value={branch}>
                {branch}
              </option>
            ))}
          </select>
        </div>
        <div className="form-group" style="margin: 0; flex: 0 0 200px;">
          <select
            value={profileFilter}
            onChange={(e) => setProfileFilter(e.target.value)}
            style="width: 100%;"
          >
            <option value="">All Profiles</option>
            {profiles.map((profile) => (
              <option key={profile} value={profile}>
                {profile}
              </option>
            ))}
          </select>
        </div>
        {(branchFilter || profileFilter) && (
          <button
            className="btn-secondary"
            onClick={() => {
              setBranchFilter("");
              setProfileFilter("");
            }}
            style="height: fit-content;"
          >
            Clear Filters
          </button>
        )}
      </div>

      <Table
        columns={columns}
        data={snapshots}
        keyField="id"
        onRowClick={(row) => route(`/snapshots/${row.id}`)}
        emptyMessage="No snapshots found. Create one from an active cluster."
      />

      {/* Create Snapshot Modal */}
      {showCreateModal && (
        <CreateSnapshotModal
          onClose={() => setShowCreateModal(false)}
          onCreated={() => {
            setShowCreateModal(false);
            loadSnapshots();
          }}
        />
      )}

      {/* Delete Confirmation Modal */}
      {snapshotToDelete && (
        <ConfirmModal
          title="Delete Snapshot"
          message={`Are you sure you want to delete snapshot "${snapshotToDelete.name}"?`}
          confirmLabel="Delete"
          onConfirm={handleDelete}
          onCancel={() => setSnapshotToDelete(null)}
          loading={deleting}
        />
      )}
    </div>
  );
}
