import { useEffect, useState } from "preact/hooks";
import { Modal } from "./Modal";
import { apiClient } from "../lib/api-client";
import { formatDuration } from "../lib/time-utils";

// The API's own error text is shown as it is: 422 (validation), 409 (slug taken),
// 429 (names the limit) and 403 (environment).
export function CreateInstanceModal({ onClose, onCreated }) {
  const [templates, setTemplates] = useState([]);
  const [form, setForm] = useState({
    slug: "", template: "", size: "", environment: "ephemeral", lease: "", key: "",
  });
  const [error, setError] = useState(null);
  const [creating, setCreating] = useState(false);

  const template = templates.find((t) => t.name === form.template);
  const set = (field) => (e) => setForm({ ...form, [field]: e.target.value });

  // A template's own default lease and first size replace the last template's.
  const pick = (t) =>
    setForm((f) => ({
      ...f, template: t.name, size: t.sizes.find((z) => z.hourly_price != null)?.name ?? "",
      lease: String(t.lease_seconds),
    }));

  useEffect(() => {
    apiClient
      .get("/api/instance-templates")
      .then((data) => {
        setTemplates(data.templates);
        if (data.templates[0]) pick(data.templates[0]);
      })
      .catch((err) => setError(err.message));
  }, []);

  const handleCreate = async () => {
    try {
      setError(null);
      setCreating(true);
      const instance = await apiClient.post("/api/instances", {
        slug: form.slug,
        template: form.template,
        size: form.size,
        environment: form.environment,
        lease_seconds: parseInt(form.lease, 10) || null,
        ssh_public_key: form.key.trim(),
      });
      onClose();
      onCreated(instance);
    } catch (err) {
      setError(err.message);
      setCreating(false);
    }
  };

  return (
    <Modal onClose={onClose} title="Create Instance">
      <div className="form-group">
        <label>Slug</label>
        <input type="text" value={form.slug} onInput={set("slug")} placeholder="e.g., build-box" />
      </div>
      <div className="form-group">
        <label>Template</label>
        {templates.length === 0 && !error && (
          <p style="color: var(--text-light);">No templates are available for the configured providers.</p>
        )}
        <select
          value={form.template}
          onChange={(e) => pick(templates.find((t) => t.name === e.target.value))}
        >
          {templates.map((t) => (
            <option key={t.name} value={t.name}>
              {t.name} ({t.provider}{t.region ? `, ${t.region}` : ""})
            </option>
          ))}
        </select>
      </div>
      <div className="form-group">
        <label>Size</label>
        <select value={form.size} onChange={set("size")}>
          {(template?.sizes || []).map((z) => (
            // The API refuses a size with no price (DR-0055 decision 6), so it cannot be picked.
            <option key={z.name} value={z.name} disabled={z.hourly_price == null}>
              {z.name}: {z.vcpu} vCPU / {z.memory_mb} MB /{" "}
              {z.hourly_price == null ? "no price" : `$${z.hourly_price}/h`}
            </option>
          ))}
        </select>
      </div>
      <div className="form-group">
        <label>Environment</label>
        <select value={form.environment} onChange={set("environment")}>
          <option value="ephemeral">Ephemeral</option>
          <option value="staging">Staging</option>
          <option value="production">Production</option>
        </select>
      </div>
      <div className="form-group">
        <label>Lease (seconds)</label>
        <input
          type="number"
          min="1"
          max={template?.max_lifetime_seconds}
          value={form.lease}
          onInput={set("lease")}
        />
        <small style="color: var(--color-text-muted);">
          {formatDuration(parseInt(form.lease, 10))}
          {template && `. At most ${formatDuration(template.max_lifetime_seconds)}.`}
        </small>
      </div>
      <div className="form-group">
        <label>SSH public key</label>
        <textarea rows="3" value={form.key} onInput={set("key")} placeholder="ssh-ed25519 AAAA... user@host" />
      </div>

      {error && <div className="modal-error">{error}</div>}

      <div className="modal-actions">
        <button className="btn-secondary" onClick={onClose} disabled={creating}>
          Cancel
        </button>
        <button
          className="btn-primary"
          onClick={handleCreate}
          disabled={creating || !template || !form.slug || !form.key.trim()}
        >
          {creating ? "Creating..." : "Create"}
        </button>
      </div>
    </Modal>
  );
}
