import { useEffect, useState, useCallback, useRef } from "preact/hooks";
import { route } from "preact-router";
import { Table } from "../components/Table";
import { Guid } from "../components/Guid";
import { StatusBadge } from "../components/StatusBadge";
import { EditButton } from "../components/EditButton";
import { DeleteButton } from "../components/DeleteButton";
import { TagPicker } from "../components/TagPicker";
import { PresetEditModal } from "../components/PresetEditModal";
import { PresetDeployModal } from "../components/PresetDeployModal";
import { ConfirmModal } from "../components/ConfirmModal";
import { Modal } from "../components/Modal";
import { apiClient } from "../lib/api-client";
import { formatDateTime } from "../lib/time-utils";

export function PresetList() {
  const [presets, setPresets] = useState([]);
  const [profiles, setProfiles] = useState([]);
  const [providers, setProviders] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [showCreateModal, setShowCreateModal] = useState(false);
  const [showEditModal, setShowEditModal] = useState(false);
  const [showDeployModal, setShowDeployModal] = useState(false);
  const [deployingPreset, setDeployingPreset] = useState(null);
  const [presetToDelete, setPresetToDelete] = useState(null);
  const [deleting, setDeleting] = useState(false);
  const [createError, setCreateError] = useState(null);
  const [createForm, setCreateForm] = useState({
    name: "",
    description: "",
    profile_name: "",
    default_branch: "main",
    default_ttl_hours: "",
    service_overrides: {},
    naming_strategy: { type: "generated" },
  });
  const [editingPreset, setEditingPreset] = useState(null);

  const loadPresets = useCallback(async (showLoader = true) => {
    try {
      if (showLoader) setLoading(true);
      const data = await apiClient.get("/api/presets");
      // Sort by name alphabetically (DR-0017: list responses are {presets: [...]})
      const presets = data.presets;
      presets.sort((a, b) => a.name.localeCompare(b.name));
      setPresets(presets);
      setError(null);
    } catch (err) {
      setError(err.message);
    } finally {
      if (showLoader) setLoading(false);
    }
  }, []);

  const loadProfiles = useCallback(async () => {
    try {
      const data = await apiClient.get("/api/registry/profiles");
      setProfiles(data.profiles || []);
    } catch (err) {
      console.error("Failed to load profiles:", err);
    }
  }, []);

  const loadProviders = useCallback(async () => {
    try {
      const data = await apiClient.get("/api/registry/providers");
      setProviders(data.providers || []);
    } catch (err) {
      console.error("Failed to load providers:", err);
    }
  }, []);

  useEffect(() => {
    loadPresets();
    loadProfiles();
    loadProviders();
  }, [loadPresets, loadProfiles, loadProviders]);

  const handleCreate = async () => {
    setCreateError(null);

    if (!createForm.name || !createForm.profile_name) {
      setCreateError("Name and Profile are required");
      return;
    }

    try {
      // Convert service_overrides to API format if any are set
      const serviceOverrides =
        Object.keys(createForm.service_overrides).length > 0
          ? createForm.service_overrides
          : null;

      // Parse TTL if provided
      const ttlHours = createForm.default_ttl_hours
        ? parseInt(createForm.default_ttl_hours, 10)
        : null;

      await apiClient.post("/api/presets", {
        name: createForm.name,
        description: createForm.description || null,
        profile_name: createForm.profile_name,
        default_branch: createForm.default_branch || null,
        default_ttl_hours: ttlHours && !isNaN(ttlHours) ? ttlHours : null,
        service_overrides: serviceOverrides,
        naming_strategy: createForm.naming_strategy || { type: "generated" },
      });
      setCreateForm({
        name: "",
        description: "",
        profile_name: "",
        default_branch: "main",
        default_ttl_hours: "",
        service_overrides: {},
        naming_strategy: { type: "generated" },
      });
      setShowCreateModal(false);
      loadPresets();
    } catch (err) {
      setCreateError(err.message);
    }
  };

  const handleEdit = (preset) => {
    setEditingPreset(preset);
    setShowEditModal(true);
  };

  const handleDelete = async () => {
    if (!presetToDelete) return;

    try {
      setDeleting(true);
      await apiClient.delete(`/api/presets/${presetToDelete.id}`);
      setPresetToDelete(null);
      loadPresets();
    } catch (err) {
      setDeleting(false);
      throw err; // Re-throw to let ConfirmModal handle it
    } finally {
      setDeleting(false);
    }
  };

  const handleDeployClick = (preset) => {
    setDeployingPreset(preset);
    setShowDeployModal(true);
  };

  const columns = [
    {
      key: "name",
      label: "Name",
      render: (value, row) => (
        <span style="font-weight: 500; color: var(--color-primary);">
          {value}
        </span>
      ),
    },
    {
      key: "profile_name",
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
      key: "environment",
      label: "Environment",
      render: (value) => (
        <span className="status-badge status-muted">{value}</span>
      ),
    },
    {
      key: "default_branch",
      label: "Default Branch",
      render: (value) =>
        value || <span style="color: var(--color-text-muted);">-</span>,
    },
    {
      key: "use_count",
      label: "Uses",
      render: (value) => value || 0,
    },
    {
      key: "last_used_at",
      label: "Last Used",
      render: (value) =>
        value ? (
          formatDateTime(value)
        ) : (
          <span style="color: var(--color-text-muted);">Never</span>
        ),
    },
    {
      key: "actions",
      label: "",
      render: (_, row) => (
        <div style="display: flex; gap: 0.5rem; align-items: center;">
          <button
            className="btn-primary btn-small"
            onClick={(e) => {
              e.stopPropagation();
              handleDeployClick(row);
            }}
            title="Deploy from this preset"
          >
            Deploy
          </button>
          <EditButton onClick={() => handleEdit(row)} title="Edit preset" />
          <DeleteButton
            onClick={() => setPresetToDelete(row)}
            title="Delete preset"
          />
        </div>
      ),
    },
  ];

  if (loading) return <div className="loading">Loading presets...</div>;

  return (
    <div className="page">
      <div className="page-header">
        <h2>Deployment Presets</h2>
        <button
          className="btn-primary"
          onClick={() => setShowCreateModal(true)}
        >
          Create Preset
        </button>
      </div>

      {error && <div className="error-banner">{error}</div>}

      <p style="color: var(--color-text-muted); margin-bottom: 1rem;">
        Presets are saved deployment configurations for quick on-demand
        deployments without webhook triggers.
      </p>

      <Table
        columns={columns}
        data={presets}
        keyField="id"
        onRowClick={(row) => route(`/presets/${row.id}`)}
        emptyMessage="No presets found. Create one to get started."
      />

      {/* Create Modal */}
      {showCreateModal && (
        <Modal
          onClose={() => setShowCreateModal(false)}
          title="Create Deployment Preset"
        >
          <div className="form-group">
            <label>Name *</label>
            <input
              type="text"
              value={createForm.name}
              onChange={(e) =>
                setCreateForm({ ...createForm, name: e.target.value })
              }
              placeholder="e.g., local-dev-stack"
            />
          </div>
          <div className="form-group">
            <label>Description</label>
            <textarea
              value={createForm.description}
              onChange={(e) =>
                setCreateForm({ ...createForm, description: e.target.value })
              }
              placeholder="Optional description..."
              rows={2}
            />
          </div>
          <div className="form-group">
            <label>Deployment Profile *</label>
            <select
              value={createForm.profile_name}
              onChange={(e) =>
                setCreateForm({ ...createForm, profile_name: e.target.value })
              }
            >
              <option value="">Select a profile...</option>
              {profiles.map((p) => (
                <option key={p.name} value={p.name}>
                  {p.name}
                  {p.provider && p.provider !== "unknown"
                    ? ` (${p.provider})`
                    : ""}
                </option>
              ))}
            </select>
          </div>
          <div className="form-group">
            <label>Default Branch</label>
            <input
              type="text"
              value={createForm.default_branch}
              onChange={(e) =>
                setCreateForm({
                  ...createForm,
                  default_branch: e.target.value,
                })
              }
              placeholder="main"
            />
            <small style="color: var(--color-text-muted);">
              Used for image discovery when no specific tag is set
            </small>
          </div>
          <div className="form-group">
            <label>Default TTL (hours)</label>
            <input
              type="number"
              min="1"
              max="168"
              value={createForm.default_ttl_hours}
              onChange={(e) =>
                setCreateForm({
                  ...createForm,
                  default_ttl_hours: e.target.value,
                })
              }
              placeholder="24"
            />
            <small style="color: var(--color-text-muted);">
              How long before clusters auto-expire (1-168 hours, default: 24)
            </small>
          </div>
          <div className="form-group">
            <label>Naming Strategy</label>
            <select
              value={createForm.naming_strategy?.type || "generated"}
              onChange={(e) => {
                const newType = e.target.value;
                if (newType === "generated") {
                  setCreateForm({
                    ...createForm,
                    naming_strategy: { type: "generated" },
                  });
                } else if (newType === "fixed") {
                  setCreateForm({
                    ...createForm,
                    naming_strategy: {
                      type: "fixed",
                      name: createForm.naming_strategy?.name || "",
                    },
                  });
                } else if (newType === "pattern") {
                  setCreateForm({
                    ...createForm,
                    naming_strategy: {
                      type: "pattern",
                      pattern:
                        createForm.naming_strategy?.pattern || "{branch}",
                    },
                  });
                }
              }}
            >
              <option value="generated">Generated (auto-generated slug)</option>
              <option value="fixed">Fixed (stable name)</option>
              <option value="pattern">Pattern (template-based)</option>
            </select>
            <small style="color: var(--color-text-muted);">
              How cluster names/slugs are determined
            </small>
          </div>
          {createForm.naming_strategy?.type === "fixed" && (
            <div className="form-group">
              <label>Fixed Cluster Name</label>
              <input
                type="text"
                value={createForm.naming_strategy.name || ""}
                onChange={(e) =>
                  setCreateForm({
                    ...createForm,
                    naming_strategy: {
                      ...createForm.naming_strategy,
                      name: e.target.value,
                    },
                  })
                }
                placeholder="e.g., staging, test-env"
              />
              <small style="color: var(--color-text-muted);">
                Stable name that persists across cluster recreations
              </small>
            </div>
          )}
          {createForm.naming_strategy?.type === "pattern" && (
            <div className="form-group">
              <label>Name Pattern</label>
              <input
                type="text"
                value={createForm.naming_strategy.pattern || ""}
                onChange={(e) =>
                  setCreateForm({
                    ...createForm,
                    naming_strategy: {
                      ...createForm.naming_strategy,
                      pattern: e.target.value,
                    },
                  })
                }
                placeholder="e.g., {branch}, {repo}-{branch}"
              />
              <small style="color: var(--color-text-muted);">
                Template with placeholders: {"{branch}"}, {"{repo}"},{" "}
                {"{commit}"}
              </small>
            </div>
          )}
          {createForm.profile_name && (
            <div className="form-group">
              <label>Service Tag Overrides</label>
              <small style="color: var(--color-text-muted); display: block; margin-bottom: 0.5rem;">
                Optionally pin specific services to fixed image tags
              </small>
              <TagPicker
                services={
                  profiles.find((p) => p.name === createForm.profile_name)
                    ?.services || []
                }
                value={createForm.service_overrides}
                onChange={(overrides) =>
                  setCreateForm({
                    ...createForm,
                    service_overrides: overrides,
                  })
                }
              />
            </div>
          )}

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
      {showEditModal && editingPreset && (
        <PresetEditModal
          preset={editingPreset}
          profiles={profiles}
          onClose={() => {
            setShowEditModal(false);
            setEditingPreset(null);
          }}
          onSaved={() => {
            setShowEditModal(false);
            setEditingPreset(null);
            loadPresets();
          }}
        />
      )}

      {/* Deploy Modal */}
      {showDeployModal && deployingPreset && (
        <PresetDeployModal
          preset={deployingPreset}
          profile={profiles.find(
            (p) => p.name === deployingPreset.profile_name,
          )}
          providers={providers}
          onClose={() => {
            setShowDeployModal(false);
            setDeployingPreset(null);
          }}
        />
      )}

      {/* Delete Confirmation Modal */}
      {presetToDelete && (
        <ConfirmModal
          title="Delete Preset"
          message={`Are you sure you want to delete preset "${presetToDelete.name}"?`}
          confirmLabel="Delete"
          onConfirm={handleDelete}
          onCancel={() => setPresetToDelete(null)}
          loading={deleting}
        />
      )}
    </div>
  );
}
