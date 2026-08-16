import { useEffect, useState, useRef, useCallback } from "preact/hooks";
import { route } from "preact-router";
import { Table } from "../components/Table";
import { StatusBadge } from "../components/StatusBadge";
import { Guid } from "../components/Guid";
import { apiClient } from "../lib/api-client";
import { sseClient } from "../lib/sse-client";
import { formatDateTime } from "../lib/time-utils";

export function DeploymentList() {
  const [deployments, setDeployments] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [showHistory, setShowHistory] = useState(false);

  // Use refs to always have current values in callbacks
  const showHistoryRef = useRef(showHistory);
  const errorRef = useRef(error);
  useEffect(() => {
    showHistoryRef.current = showHistory;
    errorRef.current = error;
  }, [showHistory, error]);

  const loadDeployments = useCallback(async (showLoadingState = true) => {
    try {
      if (showLoadingState) {
        setLoading(true);
      }
      const params = showHistoryRef.current ? "?show_history=true" : "";
      const data = await apiClient.get(`/api/deployments${params}`);
      setDeployments(data.deployments);
      setError(null);
    } catch (err) {
      setError(err.message);
    } finally {
      if (showLoadingState) {
        setLoading(false);
      }
    }
  }, []); // No deps - uses ref instead

  // Store loadDeployments in a ref so SSE handlers always call the latest version
  const loadDeploymentsRef = useRef(loadDeployments);
  useEffect(() => {
    loadDeploymentsRef.current = loadDeployments;
  }, [loadDeployments]);

  // Load data when showHistory changes
  useEffect(() => {
    loadDeployments();
  }, [showHistory, loadDeployments]);

  // Setup SSE listeners once on mount - handlers are stable
  useEffect(() => {
    const handleDeploymentStatusChange = (data) => {
      loadDeploymentsRef.current(false); // Don't show loading state
    };

    const handleReconnected = () => {
      loadDeploymentsRef.current(false); // Don't show loading state
    };

    const handleConnected = () => {
      if (errorRef.current) {
        loadDeploymentsRef.current(false); // Don't show loading state
      }
    };

    sseClient.on("deployment_status_changed", handleDeploymentStatusChange);
    sseClient.on("reconnected", handleReconnected);
    sseClient.on("connected", handleConnected);

    // Cleanup listeners on unmount only
    return () => {
      sseClient.off("deployment_status_changed", handleDeploymentStatusChange);
      sseClient.off("reconnected", handleReconnected);
      sseClient.off("connected", handleConnected);
    };
  }, []); // Empty deps - runs once, handlers never change identity

  const columns = [
    {
      key: "deployment_id",
      label: "Deployment ID",
      render: (id) => <Guid value={id} linkTo={`/deployments/${id}`} />,
    },
    {
      key: "cluster_id",
      label: "Cluster ID",
      render: (id) => <Guid value={id} linkTo={`/clusters/${id}`} />,
    },
    {
      key: "manifest_version",
      label: "Profile",
      render: (value) =>
        value ? (
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
        ) : (
          "-"
        ),
    },
    {
      key: "status",
      label: "Status",
      render: (status) => <StatusBadge status={status} />,
    },
    { key: "deployed_by", label: "Deployed By" },
    {
      key: "deployed_at",
      label: "Deployed",
      render: (date) => formatDateTime(date),
    },
  ];

  const handleRowClick = (deployment) => {
    route(`/deployments/${deployment.deployment_id}`);
  };

  if (loading) return <div className="loading">Loading deployments...</div>;
  if (error) return <div className="error">Error: {error}</div>;

  return (
    <div className="page">
      <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 1rem;">
        <h2>Deployments</h2>
        <label style="display: flex; align-items: center; gap: 0.5rem; cursor: pointer;">
          <input
            type="checkbox"
            checked={showHistory}
            onChange={(e) => setShowHistory(e.target.checked)}
          />
          <span>Show history</span>
        </label>
      </div>
      <Table
        columns={columns}
        data={deployments}
        keyField="id"
        onRowClick={handleRowClick}
      />
    </div>
  );
}
