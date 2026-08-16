import { useState, useEffect } from "preact/hooks";
import { Modal } from "./Modal";
import { TagPicker } from "./TagPicker";
import { apiClient } from "../lib/api-client";

export function PresetEditModal({ preset, profiles, onClose, onSaved }) {
  const [form, setForm] = useState({
    name: "",
    description: "",
    profile_name: "",
    default_branch: "",
    default_ttl_hours: "",
    service_overrides: {},
    naming_strategy: { type: "generated" },
  });
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (preset) {
      setForm({
        name: preset.name || "",
        description: preset.description || "",
        profile_name: preset.profile_name || "",
        default_branch: preset.default_branch || "",
        default_ttl_hours: preset.default_ttl_hours?.toString() || "",
        service_overrides: preset.service_overrides || {},
        naming_strategy: preset.naming_strategy || { type: "generated" },
      });
    }
  }, [preset]);

  const handleSave = async () => {
    setError(null);

    if (!form.name || !form.profile_name) {
      setError("Name and Profile are required");
      return;
    }

    try {
      setSaving(true);

      // Parse TTL if provided
      const ttlHours = form.default_ttl_hours
        ? parseInt(form.default_ttl_hours, 10)
        : null;

      await apiClient.put(`/api/presets/${preset.id}`, {
        name: form.name,
        description: form.description || null,
        profile_name: form.profile_name,
        default_branch: form.default_branch || null,
        default_ttl_hours: ttlHours && !isNaN(ttlHours) ? ttlHours : null,
        service_overrides: form.service_overrides,
        naming_strategy: form.naming_strategy || { type: "generated" },
      });

      onSaved();
    } catch (err) {
      setError(err.message);
    } finally {
      setSaving(false);
    }
  };

  return (
    <Modal onClose={onClose} title="Edit Preset">
      <div className="form-group">
        <label>Name *</label>
        <input
          type="text"
          value={form.name}
          onChange={(e) => setForm({ ...form, name: e.target.value })}
        />
      </div>
      <div className="form-group">
        <label>Description</label>
        <textarea
          value={form.description}
          onChange={(e) => setForm({ ...form, description: e.target.value })}
          rows={2}
        />
      </div>
      <div className="form-group">
        <label>Deployment Profile *</label>
        <select
          value={form.profile_name}
          onChange={(e) => setForm({ ...form, profile_name: e.target.value })}
        >
          <option value="">Select a profile...</option>
          {profiles.map((p) => (
            <option key={p.name} value={p.name}>
              {p.name}
              {p.provider && p.provider !== "unknown" ? ` (${p.provider})` : ""}
            </option>
          ))}
        </select>
      </div>
      <div className="form-group">
        <label>Default Branch</label>
        <input
          type="text"
          value={form.default_branch}
          onChange={(e) => setForm({ ...form, default_branch: e.target.value })}
          placeholder="main"
        />
      </div>
      <div className="form-group">
        <label>Default TTL (hours)</label>
        <input
          type="number"
          min="1"
          max="168"
          value={form.default_ttl_hours}
          onChange={(e) =>
            setForm({ ...form, default_ttl_hours: e.target.value })
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
          value={form.naming_strategy?.type || "generated"}
          onChange={(e) => {
            const newType = e.target.value;
            if (newType === "generated") {
              setForm({ ...form, naming_strategy: { type: "generated" } });
            } else if (newType === "fixed") {
              setForm({
                ...form,
                naming_strategy: {
                  type: "fixed",
                  name: form.naming_strategy?.name || "",
                },
              });
            } else if (newType === "pattern") {
              setForm({
                ...form,
                naming_strategy: {
                  type: "pattern",
                  pattern: form.naming_strategy?.pattern || "{branch}",
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
      {form.naming_strategy?.type === "fixed" && (
        <div className="form-group">
          <label>Fixed Cluster Name</label>
          <input
            type="text"
            value={form.naming_strategy.name || ""}
            onChange={(e) =>
              setForm({
                ...form,
                naming_strategy: {
                  ...form.naming_strategy,
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
      {form.naming_strategy?.type === "pattern" && (
        <div className="form-group">
          <label>Name Pattern</label>
          <input
            type="text"
            value={form.naming_strategy.pattern || ""}
            onChange={(e) =>
              setForm({
                ...form,
                naming_strategy: {
                  ...form.naming_strategy,
                  pattern: e.target.value,
                },
              })
            }
            placeholder="e.g., {branch}, {repo}-{branch}"
          />
          <small style="color: var(--color-text-muted);">
            Template with placeholders: {"{branch}"}, {"{repo}"}, {"{commit}"}
          </small>
        </div>
      )}
      {form.profile_name && (
        <div className="form-group">
          <label>Service Tag Overrides</label>
          <small style="color: var(--color-text-muted); display: block; margin-bottom: 0.5rem;">
            Optionally pin specific services to fixed image tags
          </small>
          <TagPicker
            services={
              profiles.find((p) => p.name === form.profile_name)?.services || []
            }
            value={form.service_overrides}
            onChange={(overrides) =>
              setForm({ ...form, service_overrides: overrides })
            }
          />
        </div>
      )}

      {error && <div className="modal-error">{error}</div>}

      <div className="modal-actions">
        <button className="btn-secondary" onClick={onClose} disabled={saving}>
          Cancel
        </button>
        <button className="btn-primary" onClick={handleSave} disabled={saving}>
          {saving ? "Saving..." : "Save Changes"}
        </button>
      </div>
    </Modal>
  );
}
