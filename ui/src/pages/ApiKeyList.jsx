import { useEffect, useState, useCallback, useRef } from "preact/hooks";
import { route } from "preact-router";
import { Table } from "../components/Table";
import { StatusBadge } from "../components/StatusBadge";
import { Guid } from "../components/Guid";
import { DeleteButton } from "../components/DeleteButton";
import { ConfirmModal } from "../components/ConfirmModal";
import { apiClient } from "../lib/api-client";
import { formatDateTime, parseUTC } from "../lib/time-utils";

export function ApiKeyList() {
  const [keys, setKeys] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [showRevoked, setShowRevoked] = useState(false);
  const [keyToRevoke, setKeyToRevoke] = useState(null);
  const [revoking, setRevoking] = useState(false);

  // Use refs to always have current values in callbacks
  const showRevokedRef = useRef(showRevoked);
  useEffect(() => {
    showRevokedRef.current = showRevoked;
  }, [showRevoked]);

  const loadKeys = useCallback(async (showLoadingState = true) => {
    try {
      if (showLoadingState) {
        setLoading(true);
      }
      const params = showRevokedRef.current ? "?active_only=false" : "";
      const data = await apiClient.get(`/api/keys${params}`);
      setKeys(data.keys);
      setError(null);
    } catch (err) {
      setError(err.message);
    } finally {
      if (showLoadingState) {
        setLoading(false);
      }
    }
  }, []);

  // Store loadKeys in a ref for SSE handlers
  const loadKeysRef = useRef(loadKeys);
  useEffect(() => {
    loadKeysRef.current = loadKeys;
  }, [loadKeys]);

  // Load data when showRevoked changes
  useEffect(() => {
    loadKeys();
  }, [showRevoked, loadKeys]);

  const handleRevoke = async () => {
    if (!keyToRevoke) return;

    try {
      setRevoking(true);
      await apiClient.delete(`/api/keys/${keyToRevoke.id}`);
      setKeyToRevoke(null);
      loadKeys(false); // Reload without showing loading state
    } catch (err) {
      setRevoking(false);
      throw err; // Re-throw to let ConfirmModal handle it
    } finally {
      setRevoking(false);
    }
  };

  const formatDate = (dateStr) => {
    if (!dateStr) return "-";
    // API returns UTC without 'Z', append it for correct parsing
    return formatDateTime(dateStr);
  };

  const formatTimeAgo = (dateStr) => {
    if (!dateStr) return <span className="text-muted">Never</span>;

    const date = parseUTC(dateStr);
    const now = new Date();
    const diffMs = now - date;
    const diffMins = Math.floor(diffMs / 60000);
    const diffHours = Math.floor(diffMs / 3600000);
    const diffDays = Math.floor(diffMs / 86400000);

    if (diffMins < 1) return "Just now";
    if (diffMins < 60) return `${diffMins}m ago`;
    if (diffHours < 24) return `${diffHours}h ago`;
    if (diffDays < 30) return `${diffDays}d ago`;
    return formatDate(dateStr);
  };

  const getStatusBadge = (row) => {
    if (!row.is_active) return <StatusBadge status="failed" />;
    if (!row.is_valid) return <StatusBadge status="expired" />;
    return <StatusBadge status="active" />;
  };

  const columns = [
    {
      key: "id",
      label: "ID",
      render: (id) => (
        <Guid value={id.toString()} linkTo={`/keys/${id}`} length={4} />
      ),
    },
    {
      key: "username",
      label: "Username",
    },
    {
      key: "environment",
      label: "Environment",
      render: (env) =>
        env ? (
          <span className="badge">{env}</span>
        ) : (
          <span className="text-muted">All</span>
        ),
    },
    {
      key: "is_valid",
      label: "Status",
      render: (_, row) => getStatusBadge(row),
    },
    {
      key: "last_used_at",
      label: "Last Used",
      render: (date) => formatTimeAgo(date),
    },
    {
      key: "expires_at",
      label: "Expires",
      render: (date) => (date ? formatDate(date) : "Never"),
    },
    {
      key: "actions",
      label: "",
      render: (_, row) => (
        <DeleteButton
          onClick={(e) => {
            e.stopPropagation();
            setKeyToRevoke(row);
          }}
          disabled={!row.is_active}
          title={row.is_active ? "Revoke key" : "Already revoked"}
        />
      ),
    },
  ];

  const handleRowClick = (key) => {
    route(`/keys/${key.id}`);
  };

  if (loading) return <div className="loading">Loading API keys...</div>;
  if (error) return <div className="error">Error: {error}</div>;

  return (
    <div className="page">
      <div
        className="page-header"
        style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 1rem;"
      >
        <h2>API Keys</h2>
        <div style="display: flex; align-items: center; gap: 1rem;">
          <label style="display: flex; align-items: center; gap: 0.5rem; cursor: pointer;">
            <input
              type="checkbox"
              checked={showRevoked}
              onChange={(e) => setShowRevoked(e.target.checked)}
            />
            <span>Show revoked</span>
          </label>
          <button className="btn-primary" onClick={() => route("/keys/create")}>
            Create New Key
          </button>
        </div>
      </div>
      <Table
        columns={columns}
        data={keys}
        onRowClick={handleRowClick}
        keyField="id"
      />

      {/* Revoke Confirmation Modal */}
      {keyToRevoke && (
        <ConfirmModal
          title="Revoke API Key"
          message={`Are you sure you want to revoke the API key for "${keyToRevoke.username}"? This action cannot be undone.`}
          confirmLabel="Revoke"
          onConfirm={handleRevoke}
          onCancel={() => setKeyToRevoke(null)}
          loading={revoking}
        />
      )}
    </div>
  );
}
