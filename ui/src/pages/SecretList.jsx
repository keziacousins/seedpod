import { useEffect, useState } from "preact/hooks";
import { Table } from "../components/Table";
import { TabNav } from "../components/TabNav";
import { HiddenSecret } from "../components/HiddenSecret";
import { CopyableText } from "../components/CopyableText";
import { EditButton } from "../components/EditButton";
import { DeleteButton } from "../components/DeleteButton";
import { ConfirmModal } from "../components/ConfirmModal";
import { Modal } from "../components/Modal";
import { apiClient } from "../lib/api-client";
import { formatDateTime } from "../lib/time-utils";

export function SecretList() {
  const [secrets, setSecrets] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [environment, setEnvironment] = useState("ephemeral");
  const [showCreateModal, setShowCreateModal] = useState(false);
  const [showEditModal, setShowEditModal] = useState(false);
  const [secretToDelete, setSecretToDelete] = useState(null);
  const [deleting, setDeleting] = useState(false);
  const [createForm, setCreateForm] = useState({ key_name: "", value: "" });
  const [createError, setCreateError] = useState(null);
  const [editForm, setEditForm] = useState({ key_name: "", value: "" });
  const [editError, setEditError] = useState(null);

  useEffect(() => {
    loadSecrets(secrets.length === 0);
  }, [environment]);

  const loadSecrets = async (showLoader = false) => {
    try {
      if (showLoader) setLoading(true);
      const data = await apiClient.get(
        `/api/secrets?environment=${environment}`,
      );
      setSecrets(data.secrets);
      setError(null);
    } catch (err) {
      setError(err.message);
    } finally {
      if (showLoader) setLoading(false);
    }
  };

  const handleCreate = async () => {
    setCreateError(null);

    if (!createForm.key_name || !createForm.value) {
      setCreateError("Please fill in both key name and value");
      return;
    }

    try {
      await apiClient.post("/api/secrets", {
        environment,
        key_name: createForm.key_name,
        value: createForm.value,
      });
      setCreateForm({ key_name: "", value: "" });
      setShowCreateModal(false);
      loadSecrets();
    } catch (err) {
      setCreateError(err.message);
    }
  };

  const handleReveal = async (key_name) => {
    try {
      const data = await apiClient.get(
        `/api/secrets/${environment}/${key_name}/reveal`,
      );
      return data.value;
    } catch (err) {
      if (err.message.includes("403")) {
        throw new Error(
          "You do not have permission to reveal secrets in this environment",
        );
      } else {
        throw new Error(`Failed to reveal secret: ${err.message}`);
      }
    }
  };

  const handleEdit = async (key_name) => {
    // Reveal the current value first
    try {
      const value = await handleReveal(key_name);
      setEditForm({ key_name, value });
      setEditError(null);
      setShowEditModal(true);
    } catch (err) {
      // Show error inline in a toast or similar - for now just log it
      console.error(`Failed to load secret for editing: ${err.message}`);
    }
  };

  const handleUpdate = async () => {
    setEditError(null);

    if (!editForm.value) {
      setEditError("Value cannot be empty");
      return;
    }

    try {
      await apiClient.post("/api/secrets", {
        environment,
        key_name: editForm.key_name,
        value: editForm.value,
      });
      setEditForm({ key_name: "", value: "" });
      setShowEditModal(false);
      loadSecrets();
    } catch (err) {
      setEditError(err.message);
    }
  };

  const handleDelete = async () => {
    if (!secretToDelete) return;

    try {
      setDeleting(true);
      await apiClient.delete(`/api/secrets/${environment}/${secretToDelete}`);
      setSecretToDelete(null);
      loadSecrets();
    } catch (err) {
      setDeleting(false);
      throw err; // Re-throw to let ConfirmModal handle it
    } finally {
      setDeleting(false);
    }
  };

  const columns = [
    {
      key: "key_name",
      label: "Key",
      render: (value) => <CopyableText value={value} />,
    },
    {
      key: "value",
      label: "Secret",
      render: (_, row) => (
        <HiddenSecret
          environment={row.environment}
          keyName={row.key_name}
          onReveal={() => handleReveal(row.key_name)}
        />
      ),
    },
    {
      key: "environment",
      label: "Environment",
      render: (value) => (
        <span className="status-badge status-muted">{value}</span>
      ),
    },
    {
      key: "key_class",
      label: "Key Class",
      render: (value) => (
        <span className="status-badge status-muted">{value}</span>
      ),
    },
    {
      key: "created_at",
      label: "Created",
      render: (value) => formatDateTime(value),
    },
    {
      key: "updated_at",
      label: "Updated",
      render: (value) => formatDateTime(value),
    },
    {
      key: "actions",
      label: "",
      render: (_, row) => (
        <div style="display: flex; gap: 0.5rem;">
          <EditButton
            onClick={() => handleEdit(row.key_name)}
            title="Edit secret"
          />
          <DeleteButton
            onClick={() => setSecretToDelete(row.key_name)}
            title="Delete secret"
          />
        </div>
      ),
    },
  ];

  if (loading) return <div className="loading">Loading secrets...</div>;

  return (
    <div className="page">
      <div className="page-header">
        <h2>Secrets Management</h2>
        <button
          className="btn-primary"
          onClick={() => setShowCreateModal(true)}
        >
          Create Secret
        </button>
      </div>

      {error && <div className="error-banner">{error}</div>}

      <TabNav
        tabs={[
          { id: "local", label: "Local" },
          { id: "ephemeral", label: "Ephemeral" },
          { id: "staging", label: "Staging" },
          { id: "production", label: "Production 🔒" },
        ]}
        activeTab={environment}
        onTabChange={setEnvironment}
      />

      {/* Secrets Table */}
      <Table columns={columns} data={secrets} keyField="key_name" />

      {/* Create Modal */}
      {showCreateModal && (
        <Modal
          onClose={() => setShowCreateModal(false)}
          title={`Create Secret in ${environment}`}
        >
          <div className="form-group">
            <label>Key Name</label>
            <input
              type="text"
              value={createForm.key_name}
              onChange={(e) =>
                setCreateForm({ ...createForm, key_name: e.target.value })
              }
              placeholder="DATABASE_URL"
            />
          </div>
          <div className="form-group">
            <label>Value</label>
            <textarea
              value={createForm.value}
              onChange={(e) =>
                setCreateForm({ ...createForm, value: e.target.value })
              }
              placeholder="postgresql://..."
              rows={4}
            />
          </div>
          {createError && <div className="modal-error">{createError}</div>}

          <div className="modal-actions">
            <button
              className="btn-secondary"
              onClick={() => setShowCreateModal(false)}
            >
              Cancel
            </button>
            <button className="btn-primary" onClick={handleCreate}>
              Create
            </button>
          </div>
        </Modal>
      )}

      {/* Edit Modal */}
      {showEditModal && (
        <Modal
          onClose={() => setShowEditModal(false)}
          title={`Edit Secret: ${editForm.key_name}`}
        >
          <div className="form-group">
            <label>Key Name</label>
            <input
              type="text"
              value={editForm.key_name}
              disabled
              style="background: var(--gray-900); cursor: not-allowed;"
            />
          </div>
          <div className="form-group">
            <label>Value</label>
            <textarea
              value={editForm.value}
              onChange={(e) =>
                setEditForm({ ...editForm, value: e.target.value })
              }
              placeholder="postgresql://..."
              rows={4}
            />
          </div>
          {editError && <div className="modal-error">{editError}</div>}

          <div className="modal-actions">
            <button
              className="btn-secondary"
              onClick={() => setShowEditModal(false)}
            >
              Cancel
            </button>
            <button className="btn-primary" onClick={handleUpdate}>
              Update
            </button>
          </div>
        </Modal>
      )}

      {/* Delete Confirmation Modal */}
      {secretToDelete && (
        <ConfirmModal
          title="Delete Secret"
          message={`Are you sure you want to delete secret "${secretToDelete}"?`}
          confirmLabel="Delete"
          onConfirm={handleDelete}
          onCancel={() => setSecretToDelete(null)}
          loading={deleting}
        />
      )}
    </div>
  );
}
