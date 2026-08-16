import { useEffect, useState, useRef, useCallback } from "preact/hooks";
import { route } from "preact-router";
import { Table } from "../components/Table";
import { StatusBadge } from "../components/StatusBadge";
import { Guid } from "../components/Guid";
import { apiClient } from "../lib/api-client";
import { sseClient } from "../lib/sse-client";
import { formatDateTime, parseUTC } from "../lib/time-utils";

export function ClusterList() {
  const [clusters, setClusters] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [showDestroyed, setShowDestroyed] = useState(false);

  // Use refs to always have current values in callbacks
  const showDestroyedRef = useRef(showDestroyed);
  const errorRef = useRef(error);
  useEffect(() => {
    showDestroyedRef.current = showDestroyed;
    errorRef.current = error;
  }, [showDestroyed, error]);

  const loadClusters = useCallback(async (showLoadingState = true) => {
    console.log(
      `[ClusterList] loadClusters called (showLoadingState=${showLoadingState})`,
    );
    try {
      if (showLoadingState) {
        setLoading(true);
      }
      const params = showDestroyedRef.current ? "?show_destroyed=true" : "";
      const data = await apiClient.get(`/api/clusters${params}`);
      setClusters(data.clusters);
      setError(null);
    } catch (err) {
      console.error("[ClusterList] Error loading clusters:", err);
      setError(err.message);
    } finally {
      if (showLoadingState) {
        setLoading(false);
      }
    }
  }, []); // No deps - uses ref instead

  // Store loadClusters in a ref so SSE handlers always call the latest version
  const loadClustersRef = useRef(loadClusters);
  useEffect(() => {
    loadClustersRef.current = loadClusters;
  }, [loadClusters]);

  // Load data when showDestroyed changes
  useEffect(() => {
    loadClusters();
  }, [showDestroyed, loadClusters]);

  // Setup SSE listeners once on mount - handlers are stable
  useEffect(() => {
    const isConnected = sseClient.isConnected();
    console.log(
      "[ClusterList] Mounting - SSE connected:",
      isConnected,
      "has error:",
      !!error,
    );

    const handleClusterStateChange = (data) => {
      loadClustersRef.current(false); // Don't show loading state
    };

    const handleReconnected = () => {
      console.log(
        "[ClusterList] SSE reconnected - reloading data (clearing any errors)",
      );
      loadClustersRef.current(false); // Don't show loading state
    };

    const handleConnected = () => {
      // If we had an error (e.g., page loaded while backend was down), retry
      if (errorRef.current) {
        loadClustersRef.current(false); // Don't show loading state
      }
    };

    sseClient.on("cluster_state_changed", handleClusterStateChange);
    sseClient.on("reconnected", handleReconnected);
    sseClient.on("connected", handleConnected);

    // Cleanup listeners on unmount only
    return () => {
      sseClient.off("cluster_state_changed", handleClusterStateChange);
      sseClient.off("reconnected", handleReconnected);
      sseClient.off("connected", handleConnected);
    };
  }, []); // Empty deps - runs once, handlers never change identity

  const formatTimeRemaining = (expiresAt) => {
    if (!expiresAt) return "Never";
    const now = new Date();
    const expires = parseUTC(expiresAt);
    const diffMs = expires - now;
    if (diffMs <= 0) return "Expired";

    const hours = Math.floor(diffMs / (1000 * 60 * 60));
    const minutes = Math.floor((diffMs % (1000 * 60 * 60)) / (1000 * 60));
    return `${hours}h ${minutes}m`;
  };

  const columns = [
    {
      key: "id",
      label: "Cluster ID",
      render: (id) => <Guid value={id} linkTo={`/clusters/${id}`} />,
    },
    { key: "repository", label: "Repository" },
    { key: "branch", label: "Branch" },
    {
      key: "status",
      label: "Status",
      render: (status, row) =>
        row.reconciliation_stale ? (
          <span style="display: inline-flex; align-items: center; gap: 0.35rem;">
            <StatusBadge status={status} />
            <span
              title="Reconciliation stale — infrastructure unreachable"
              style="color: var(--yellow); cursor: help;"
            >
              ⚠
            </span>
          </span>
        ) : (
          <StatusBadge status={status} />
        ),
    },
    {
      key: "dns_hostname",
      label: "URL",
      render: (hostname, row) => {
        if (!hostname) return "-";
        return (
          <a
            href={row.cluster_url}
            target="_blank"
            rel="noopener noreferrer"
            onClick={(e) => e.stopPropagation()}
            style={{ color: "var(--blue)" }}
          >
            {hostname}
          </a>
        );
      },
    },
    {
      key: "created_at",
      label: "Created",
      render: (date) => {
        if (!date) return "-";
        // API returns UTC, convert to local timezone
        return formatDateTime(date);
      },
    },
    {
      key: "expires_at",
      label: "TTL",
      render: (expiresAt) => formatTimeRemaining(expiresAt),
    },
  ];

  const handleRowClick = (cluster) => {
    route(`/clusters/${cluster.id}`);
  };

  if (loading) return <div className="loading">Loading clusters...</div>;
  if (error) return <div className="error">Error: {error}</div>;

  return (
    <div className="page">
      <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 1rem;">
        <h2>Clusters</h2>
        <label style="display: flex; align-items: center; gap: 0.5rem; cursor: pointer;">
          <input
            type="checkbox"
            checked={showDestroyed}
            onChange={(e) => setShowDestroyed(e.target.checked)}
          />
          <span>Show destroyed</span>
        </label>
      </div>
      <Table
        columns={columns}
        data={clusters}
        onRowClick={handleRowClick}
        keyField="id"
      />
    </div>
  );
}
