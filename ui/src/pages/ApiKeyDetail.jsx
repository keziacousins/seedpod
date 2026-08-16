import { useEffect, useState } from "preact/hooks";
import { route } from "preact-router";
import { Card } from "../components/Card";
import { StatusBadge } from "../components/StatusBadge";
import { Breadcrumb } from "../components/Breadcrumb";
import { InfoGrid, InfoGridRow } from "../components/InfoGrid";
import { Guid } from "../components/Guid";
import { ConfirmModal } from "../components/ConfirmModal";
import { apiClient } from "../lib/api-client";
import { formatDateTime, parseUTC } from "../lib/time-utils";

export function ApiKeyDetail({ keyId }) {
  const [apiKey, setApiKey] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [editMode, setEditMode] = useState(false);
  const [editData, setEditData] = useState({
    description: "",
    expires_at: "",
  });
  const [showRevokeConfirm, setShowRevokeConfirm] = useState(false);
  const [revoking, setRevoking] = useState(false);
  const [editError, setEditError] = useState(null);

  useEffect(() => {
    loadApiKey();
  }, [keyId]);

  const loadApiKey = async () => {
    try {
      setLoading(true);
      const data = await apiClient.get(`/api/keys/${keyId}`);
      setApiKey(data);
      setEditData({
        description: data.description || "",
        expires_at: data.expires_at || "",
      });
      setError(null);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const handleRevoke = async () => {
    try {
      setRevoking(true);
      await apiClient.delete(`/api/keys/${keyId}`);
      route("/keys");
    } catch (err) {
      setRevoking(false);
      throw err; // Re-throw to let ConfirmModal handle it
    }
  };

  const handleSave = async () => {
    setEditError(null);
    try {
      await apiClient.patch(`/api/keys/${keyId}`, editData);
      setEditMode(false);
      await loadApiKey();
    } catch (err) {
      setEditError(err.message);
    }
  };

  const handleCancel = () => {
    setEditMode(false);
    setEditData({
      description: apiKey.description || "",
      expires_at: apiKey.expires_at || "",
    });
  };

  const formatDate = (dateStr) => {
    if (!dateStr) return "Never";
    // API returns UTC without 'Z', append it for correct parsing
    return formatDateTime(dateStr);
  };

  const formatTimeRemaining = (expiresAt) => {
    if (!expiresAt) return "Never";
    const now = new Date();
    const expires = parseUTC(expiresAt);
    const diffMs = expires - now;
    if (diffMs <= 0) return "Expired";

    const days = Math.floor(diffMs / (1000 * 60 * 60 * 24));
    const hours = Math.floor(
      (diffMs % (1000 * 60 * 60 * 24)) / (1000 * 60 * 60),
    );
    if (days > 0) return `${days}d ${hours}h`;
    return `${hours}h`;
  };

  const getStatusBadge = (key) => {
    if (!key.is_active) return <StatusBadge status="failed" />;
    if (!key.is_valid) return <StatusBadge status="expired" />;
    return <StatusBadge status="active" />;
  };

  if (loading) return <div className="loading">Loading API key details...</div>;
  if (error) return <div className="error">Error: {error}</div>;
  if (!apiKey) return <div className="error">API key not found</div>;

  const breadcrumb = [
    { label: "API Keys", href: "/keys" },
    { label: `Key ${keyId}` },
  ];

  const actions = (
    <>
      {!editMode ? (
        <>
          <button
            onClick={() => setEditMode(true)}
            className="btn-secondary"
            disabled={!apiKey.is_active}
            title={
              !apiKey.is_active
                ? "Cannot edit revoked key"
                : "Edit description and expiration"
            }
          >
            Edit
          </button>
          <button
            onClick={() => setShowRevokeConfirm(true)}
            className="btn-danger"
            disabled={!apiKey.is_active || revoking}
            title={apiKey.is_active ? "Revoke key" : "Already revoked"}
          >
            {revoking ? "Revoking..." : "Revoke"}
          </button>
        </>
      ) : (
        <>
          <button onClick={handleSave} className="btn-primary">
            Save
          </button>
          <button onClick={handleCancel} className="btn-secondary">
            Cancel
          </button>
          {editError && (
            <span style="color: var(--danger); margin-left: 0.5rem;">
              {editError}
            </span>
          )}
        </>
      )}
    </>
  );

  return (
    <div className="page">
      <Breadcrumb path={breadcrumb} />

      <Card
        title={
          <span style="display: flex; align-items: center; gap: 0.5rem;">
            API Key: <Guid value={keyId} />
          </span>
        }
        actions={actions}
      >
        <InfoGrid>
          <InfoGridRow label="Status">{getStatusBadge(apiKey)}</InfoGridRow>
          <InfoGridRow label="Username">
            <span>{apiKey.username}</span>
          </InfoGridRow>
          <InfoGridRow label="Environment">
            {apiKey.environment ? (
              <span className="badge">{apiKey.environment}</span>
            ) : (
              <span className="text-muted">All</span>
            )}
          </InfoGridRow>
          <InfoGridRow label="Created">
            <span>{formatDate(apiKey.created_at)}</span>
          </InfoGridRow>
          <InfoGridRow label="Expires">
            {editMode ? (
              <div style="display: flex; flex-direction: column; gap: 0.5rem;">
                <select
                  value={editData.expires_at ? "custom" : "never"}
                  onChange={(e) => {
                    if (e.target.value === "never") {
                      setEditData({ ...editData, expires_at: "" });
                    }
                  }}
                  style="width: 100%; padding: 0.5rem; border: 1px solid var(--border); background: var(--bg); color: var(--text);"
                >
                  <option value="never">Never</option>
                  <option value="custom">Custom Date</option>
                </select>
                {editData.expires_at && (
                  <input
                    type="datetime-local"
                    value={
                      editData.expires_at
                        ? parseUTC(editData.expires_at)
                            .toISOString()
                            .slice(0, 16)
                        : ""
                    }
                    onChange={(e) =>
                      setEditData({
                        ...editData,
                        expires_at: e.target.value
                          ? new Date(e.target.value)
                              .toISOString()
                              .replace("Z", "")
                          : "",
                      })
                    }
                    style="width: 100%; padding: 0.5rem; border: 1px solid var(--border); background: var(--bg); color: var(--text);"
                  />
                )}
              </div>
            ) : (
              <span>
                {formatDate(apiKey.expires_at)}
                {apiKey.expires_at && (
                  <span style="margin-left: 0.5rem; color: var(--text-light);">
                    ({formatTimeRemaining(apiKey.expires_at)})
                  </span>
                )}
              </span>
            )}
          </InfoGridRow>
          <InfoGridRow label="Last Used">
            <span>
              {apiKey.last_used_at ? (
                formatDate(apiKey.last_used_at)
              ) : (
                <span className="text-muted">Never</span>
              )}
            </span>
          </InfoGridRow>
          <InfoGridRow label="Description" fullWidth>
            {editMode ? (
              <textarea
                value={editData.description}
                onChange={(e) =>
                  setEditData({ ...editData, description: e.target.value })
                }
                placeholder="e.g., GitHub Actions deployment key for exampleco-core repository"
                rows={3}
                style="width: 100%; padding: 0.5rem; border: 1px solid var(--border); background: var(--bg); color: var(--text);"
              />
            ) : (
              <span>
                {apiKey.description || (
                  <span className="text-muted">No description</span>
                )}
              </span>
            )}
          </InfoGridRow>
        </InfoGrid>
      </Card>

      <Card title="Permissions" style="margin-top: 2rem;">
        <div style="margin-bottom: 1rem;">
          <div
            className="warning-box"
            style="padding: 0.75rem; background: var(--yellow); border-left: 4px solid var(--bright-yellow); margin-bottom: 1rem;"
          >
            <strong>Note:</strong> Permissions cannot be changed after creation.
            To modify permissions, create a new key and revoke this one.
          </div>
        </div>
        <div style="display: flex; flex-wrap: wrap; gap: 0.5rem;">
          {Object.keys(apiKey.permissions).length > 0 ? (
            Object.keys(apiKey.permissions)
              .filter((perm) => apiKey.permissions[perm])
              .map((perm) => (
                <span
                  key={perm}
                  style="background: var(--gray-light); padding: 0.5rem 0.75rem; border-radius: 4px; font-family: monospace; font-size: 0.875rem; color: var(--text);"
                >
                  {perm}
                </span>
              ))
          ) : (
            <span className="text-muted">No permissions assigned</span>
          )}
        </div>
      </Card>

      {/* Revoke Confirmation Modal */}
      {showRevokeConfirm && (
        <ConfirmModal
          title="Revoke API Key"
          message="Are you sure you want to revoke this API key? This action cannot be undone."
          confirmLabel="Revoke"
          onConfirm={handleRevoke}
          onCancel={() => setShowRevokeConfirm(false)}
          loading={revoking}
        />
      )}
    </div>
  );
}
