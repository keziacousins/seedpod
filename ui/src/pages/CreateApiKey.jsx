import { useEffect, useState } from "preact/hooks";
import { route } from "preact-router";
import { Card } from "../components/Card";
import { Breadcrumb } from "../components/Breadcrumb";
import { CopyableText } from "../components/CopyableText";
import { apiClient } from "../lib/api-client";

export function CreateApiKey() {
  const [formData, setFormData] = useState({
    username: "",
    environment: "",
    description: "",
    expires_hours: "8760", // 1 year default
    permissions: {},
  });
  const [availablePermissions, setAvailablePermissions] = useState({});
  const [permissionCategories, setPermissionCategories] = useState({});
  const [createdKey, setCreatedKey] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [formError, setFormError] = useState(null);

  useEffect(() => {
    loadPermissions();
  }, []);

  const loadPermissions = async () => {
    try {
      const data = await apiClient.get("/api/permissions");
      setAvailablePermissions(data.permissions);
      setPermissionCategories(data.categories);
      setLoading(false);
    } catch (err) {
      setError(err.message);
      setLoading(false);
    }
  };

  const handlePermissionToggle = (permission) => {
    setFormData((prev) => ({
      ...prev,
      permissions: {
        ...prev.permissions,
        [permission]: !prev.permissions[permission],
      },
    }));
  };

  const handleCreate = async (e) => {
    e.preventDefault();
    setFormError(null);

    if (!formData.username) {
      setFormError("Username is required");
      return;
    }

    // Filter to only enabled permissions
    const enabledPermissions = Object.fromEntries(
      Object.entries(formData.permissions).filter(([_, enabled]) => enabled),
    );

    if (Object.keys(enabledPermissions).length === 0) {
      setFormError("Please select at least one permission");
      return;
    }

    try {
      const response = await apiClient.post("/api/keys", {
        username: formData.username,
        environment: formData.environment || null,
        description: formData.description || null,
        expires_hours: formData.expires_hours
          ? parseInt(formData.expires_hours)
          : null,
        permissions: enabledPermissions,
      });

      setCreatedKey(response.api_key);
    } catch (err) {
      setFormError(err.message);
    }
  };

  const handleClose = () => {
    route("/keys");
  };

  const breadcrumb = [
    { label: "API Keys", href: "/keys" },
    { label: "Create New Key" },
  ];

  if (loading) return <div className="loading">Loading...</div>;
  if (error) return <div className="error">Error: {error}</div>;

  // Success view - show created key once
  if (createdKey) {
    return (
      <div className="page">
        <Breadcrumb path={breadcrumb} />

        <Card title="API Key Created Successfully">
          <div
            className="warning-box"
            style="margin-bottom: 1.5rem; padding: 1rem; background: var(--yellow); border-left: 4px solid var(--bright-yellow);"
          >
            <strong>⚠️ Important:</strong> This key will only be shown once.
            Copy it now and store it securely!
          </div>

          <div style="margin-bottom: 1.5rem;">
            <label style="display: block; margin-bottom: 0.5rem; font-weight: bold;">
              API Key
            </label>
            <CopyableText value={createdKey} showFull={true} />
          </div>

          <button className="btn-primary" onClick={handleClose}>
            Done
          </button>
        </Card>
      </div>
    );
  }

  // Form view
  return (
    <div className="page">
      <Breadcrumb path={breadcrumb} />

      <Card title="Create New API Key">
        <form onSubmit={handleCreate}>
          {/* Two-column layout for form fields */}
          <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; margin-bottom: 1.5rem;">
            <div className="form-group">
              <label style="display: block; margin-bottom: 0.5rem; font-weight: bold;">
                Username *
              </label>
              <input
                type="text"
                value={formData.username}
                onChange={(e) =>
                  setFormData({ ...formData, username: e.target.value })
                }
                placeholder="e.g., github-actions-exampleco-core"
                required
                style="width: 100%; padding: 0.5rem; border: 1px solid var(--border); background: var(--bg); color: var(--text);"
              />
            </div>

            <div className="form-group">
              <label style="display: block; margin-bottom: 0.5rem; font-weight: bold;">
                Environment
              </label>
              <select
                value={formData.environment}
                onChange={(e) =>
                  setFormData({ ...formData, environment: e.target.value })
                }
                style="width: 100%; padding: 0.5rem; border: 1px solid var(--border); background: var(--bg); color: var(--text);"
              >
                <option value="">All Environments</option>
                <option value="ephemeral">Ephemeral</option>
                <option value="staging">Staging</option>
                <option value="production">Production</option>
              </select>
            </div>

            <div className="form-group" style="grid-column: 1 / -1;">
              <label style="display: block; margin-bottom: 0.5rem; font-weight: bold;">
                Description
              </label>
              <textarea
                value={formData.description}
                onChange={(e) =>
                  setFormData({ ...formData, description: e.target.value })
                }
                placeholder="e.g., GitHub Actions deployment key for exampleco-core repository"
                rows={2}
                style="width: 100%; padding: 0.5rem; border: 1px solid var(--border); background: var(--bg); color: var(--text); font-family: inherit;"
              />
            </div>

            <div className="form-group">
              <label style="display: block; margin-bottom: 0.5rem; font-weight: bold;">
                Expires In
              </label>
              <select
                value={formData.expires_hours}
                onChange={(e) =>
                  setFormData({ ...formData, expires_hours: e.target.value })
                }
                style="width: 100%; padding: 0.5rem; border: 1px solid var(--border); background: var(--bg); color: var(--text);"
              >
                <option value="720">30 days</option>
                <option value="2160">90 days</option>
                <option value="4380">6 months</option>
                <option value="8760">1 year</option>
                <option value="">Never</option>
              </select>
            </div>
          </div>

          {/* Permissions table */}
          <div className="form-group" style="margin-bottom: 1.5rem;">
            <label style="display: block; margin-bottom: 0.5rem; font-weight: bold;">
              Permissions *
            </label>

            {Object.entries(permissionCategories).map(([category, perms]) => (
              <div key={category} style="margin-bottom: 1.5rem;">
                <h4 style="margin: 0 0 0.5rem 0; color: var(--cyan); font-size: 0.875rem; text-transform: uppercase;">
                  {category}
                </h4>
                <table className="data-table" style="table-layout: fixed;">
                  <thead>
                    <tr>
                      <th style="width: 50px;"></th>
                      <th style="width: 280px;">Permission</th>
                      <th>Description</th>
                    </tr>
                  </thead>
                  <tbody>
                    {perms.map((perm) => (
                      <tr
                        key={perm}
                        style="cursor: pointer;"
                        onClick={() => handlePermissionToggle(perm)}
                      >
                        <td style="text-align: center; width: 50px;">
                          <input
                            type="checkbox"
                            checked={formData.permissions[perm] || false}
                            onChange={() => handlePermissionToggle(perm)}
                            onClick={(e) => e.stopPropagation()}
                          />
                        </td>
                        <td style="width: 280px;">
                          <code style="color: var(--text); font-size: 0.875rem;">
                            {perm}
                          </code>
                        </td>
                        <td style="color: var(--text); font-size: 0.875rem; font-family: inherit;">
                          {availablePermissions[perm]}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ))}

            <small style="color: var(--text-light);">
              Note: Permissions cannot be changed after creation. To modify
              permissions, create a new key and revoke the old one.
            </small>
          </div>

          {formError && (
            <div className="modal-error" style="margin-bottom: 1rem;">
              {formError}
            </div>
          )}

          <div style="display: flex; gap: 1rem;">
            <button type="submit" className="btn-primary">
              Create API Key
            </button>
            <button
              type="button"
              className="btn-secondary"
              onClick={() => route("/keys")}
            >
              Cancel
            </button>
          </div>
        </form>
      </Card>
    </div>
  );
}
