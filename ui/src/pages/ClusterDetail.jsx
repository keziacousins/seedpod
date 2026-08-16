import { useEffect, useState, useCallback, useRef } from "preact/hooks";
import { route } from "preact-router";
import { Card } from "../components/Card";
import { Table } from "../components/Table";
import { TabNav } from "../components/TabNav";
import { StatusBadge } from "../components/StatusBadge";
import { Breadcrumb } from "../components/Breadcrumb";
import { InfoGrid, InfoGridRow } from "../components/InfoGrid";
import { Guid } from "../components/Guid";
import { CopyableText } from "../components/CopyableText";
import { CreateSnapshotModal } from "../components/CreateSnapshotModal";
import { DestroyClusterModal } from "../components/DestroyClusterModal";
import { ConfirmModal } from "../components/ConfirmModal";
import { Modal } from "../components/Modal";
import { apiClient } from "../lib/api-client";
import { sseClient } from "../lib/sse-client";
import { formatDateTime, parseUTC } from "../lib/time-utils";

export function ClusterDetail({ clusterId }) {
  // Read initial tab from query parameter if present
  const urlParams = new URLSearchParams(window.location.search);
  const initialTab = urlParams.get("tab") || "deployments";

  const [cluster, setCluster] = useState(null);
  const [pods, setPods] = useState([]);
  const [deployments, setDeployments] = useState([]);
  const [auditHistory, setAuditHistory] = useState([]);
  const [restoreHistory, setRestoreHistory] = useState([]);
  const [events, setEvents] = useState([]);
  const [activeTab, setActiveTab] = useState(initialTab); // "deployments", "pods", "events", or "history"
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [showSnapshotModal, setShowSnapshotModal] = useState(false);
  const [showDestroyModal, setShowDestroyModal] = useState(false);
  const [showExtendTTLModal, setShowExtendTTLModal] = useState(false);
  const [extendTTLHours, setExtendTTLHours] = useState("2");
  const [extending, setExtending] = useState(false);
  const [showRehabilitateConfirm, setShowRehabilitateConfirm] = useState(false);
  const [rehabilitating, setRehabilitating] = useState(false);
  const [actionError, setActionError] = useState(null);

  const loadClusterDetails = useCallback(
    async (showLoadingState = true) => {
      try {
        if (showLoadingState) {
          setLoading(true);
        }
        const data = await apiClient.get(`/api/clusters/${clusterId}`);
        setCluster(data);
        setError(null);
      } catch (err) {
        setError(err.message);
      } finally {
        if (showLoadingState) {
          setLoading(false);
        }
      }
    },
    [clusterId],
  );

  const loadPods = useCallback(async () => {
    try {
      const data = await apiClient.get(`/api/clusters/${clusterId}/pods`);
      // API returns { cluster_id, namespace, pods: [...] }
      setPods(data.pods || []);
    } catch (err) {
      console.error("Failed to load pods:", err);
      setPods([]); // Set empty array on error
      // Don't set error here - cluster details more important
    }
  }, [clusterId]);

  const loadDeployments = useCallback(async () => {
    try {
      const data = await apiClient.get(
        `/api/clusters/${clusterId}/deployments`,
      );
      setDeployments(data.deployments || []);
    } catch (err) {
      console.error("Failed to load deployments:", err);
      setDeployments([]);
      // Don't set error here - cluster details more important
    }
  }, [clusterId]);

  const loadAuditHistory = useCallback(async () => {
    try {
      const data = await apiClient.get(`/api/clusters/${clusterId}/audit`);
      setAuditHistory(data.audit || []);
    } catch (err) {
      console.error("Failed to load audit history:", err);
      // Don't set error here - cluster details more important
    }
  }, [clusterId]);

  const loadRestoreHistory = useCallback(async () => {
    try {
      const data = await apiClient.get(
        `/api/snapshots/clusters/${clusterId}/restore-history`,
      );
      setRestoreHistory(data.restore_history || []);
    } catch (err) {
      console.error("Failed to load restore history:", err);
    }
  }, [clusterId]);

  const loadEvents = useCallback(async () => {
    try {
      const data = await apiClient.get(
        `/api/clusters/${clusterId}/events?limit=200`,
      );
      setEvents(data.events || []);
    } catch (err) {
      console.error("Failed to load events:", err);
      setEvents([]);
      // Don't set error here - cluster details more important
    }
  }, [clusterId]);

  // Store load functions and error state in refs so SSE handlers always call latest versions
  const loadClusterDetailsRef = useRef(loadClusterDetails);
  const loadPodsRef = useRef(loadPods);
  const loadDeploymentsRef = useRef(loadDeployments);
  const loadAuditHistoryRef = useRef(loadAuditHistory);
  const loadRestoreHistoryRef = useRef(loadRestoreHistory);
  const loadEventsRef = useRef(loadEvents);
  const errorRef = useRef(error);
  useEffect(() => {
    loadClusterDetailsRef.current = loadClusterDetails;
    loadPodsRef.current = loadPods;
    loadDeploymentsRef.current = loadDeployments;
    loadAuditHistoryRef.current = loadAuditHistory;
    loadRestoreHistoryRef.current = loadRestoreHistory;
    loadEventsRef.current = loadEvents;
    errorRef.current = error;
  }, [
    loadClusterDetails,
    loadPods,
    loadDeployments,
    loadAuditHistory,
    loadRestoreHistory,
    loadEvents,
    error,
  ]);

  // Load data when clusterId changes
  useEffect(() => {
    loadClusterDetails();
    loadPods();
    loadDeployments();
    loadAuditHistory();
    loadRestoreHistory();
  }, [
    clusterId,
    loadClusterDetails,
    loadPods,
    loadDeployments,
    loadAuditHistory,
    loadRestoreHistory,
  ]);

  // Setup SSE listeners once on mount - handlers are stable
  useEffect(() => {
    const handleClusterStateChange = (event) => {
      // SSE events have structure: { type, data: {...}, timestamp }
      const eventData = event.data || event;
      console.log(
        "[ClusterDetail] Cluster state change event received:",
        eventData,
      );

      if (eventData.cluster_id === clusterId) {
        console.log(
          "[ClusterDetail] This cluster changed - reloading all data",
        );
        loadClusterDetailsRef.current(false); // Don't show loading state
        loadPodsRef.current();
        loadDeploymentsRef.current();
        loadAuditHistoryRef.current();
        loadEventsRef.current();
      } else {
        console.log(
          "[ClusterDetail] Different cluster changed:",
          eventData.cluster_id,
          "vs",
          clusterId,
        );
      }
    };

    const handleDeploymentStatusChange = (event) => {
      const eventData = event.data || event;
      console.log(
        "[ClusterDetail] Deployment status change event received:",
        eventData,
      );

      if (eventData.cluster_id === clusterId) {
        console.log(
          "[ClusterDetail] Deployment on this cluster changed - reloading deployments and pods",
        );
        loadDeploymentsRef.current();
        loadClusterDetailsRef.current(false); // Don't show loading state
        loadPodsRef.current(); // Pod status may have changed due to deployment
        loadEventsRef.current(); // Events may have changed due to deployment
      }
    };

    // DR-0035: `workflow_progress` replaces the dead `pod_status_changed`
    // listener. It is also what makes an IN-WORKFLOW restore visible here:
    // `deploy.restore_snapshot` emits progress per attempt rather than a second
    // `snapshot_restore_completed` (that step retries up to 19 times by design),
    // so this page behaves the same however the restore was triggered.
    const handleWorkflowProgress = (event) => {
      const eventData = event.data || event;

      if (eventData.cluster_id === clusterId) {
        loadPodsRef.current();
        loadRestoreHistoryRef.current();
      }
    };

    const handleSnapshotRestoreCompleted = (event) => {
      const eventData = event.data || event;
      console.log(
        "[ClusterDetail] Snapshot restore completed event received:",
        eventData,
      );

      if (eventData.cluster_id === clusterId) {
        console.log(
          "[ClusterDetail] Restore on this cluster completed - reloading restore history",
        );
        loadRestoreHistoryRef.current();
      }
    };

    const handleReconnected = () => {
      loadClusterDetailsRef.current(false); // Don't show loading state
      loadPodsRef.current();
      loadDeploymentsRef.current();
      loadAuditHistoryRef.current();
      loadEventsRef.current();
    };

    const handleConnected = () => {
      if (errorRef.current) {
        loadClusterDetailsRef.current(false); // Don't show loading state
        loadPodsRef.current();
        loadDeploymentsRef.current();
        loadAuditHistoryRef.current();
        loadEventsRef.current();
      }
    };

    sseClient.on("cluster_state_changed", handleClusterStateChange);
    sseClient.on("deployment_status_changed", handleDeploymentStatusChange);
    sseClient.on("workflow_progress", handleWorkflowProgress);
    sseClient.on("snapshot_restore_completed", handleSnapshotRestoreCompleted);
    sseClient.on("reconnected", handleReconnected);
    sseClient.on("connected", handleConnected);

    // Cleanup listeners on unmount only
    return () => {
      sseClient.off("cluster_state_changed", handleClusterStateChange);
      sseClient.off("deployment_status_changed", handleDeploymentStatusChange);
      sseClient.off("workflow_progress", handleWorkflowProgress);
      sseClient.off(
        "snapshot_restore_completed",
        handleSnapshotRestoreCompleted,
      );
      sseClient.off("reconnected", handleReconnected);
      sseClient.off("connected", handleConnected);
    };
  }, [clusterId]); // Only clusterId dep - handlers use refs for load functions

  // Refetch pods whenever user switches to the pods tab
  useEffect(() => {
    if (
      activeTab === "pods" &&
      cluster?.status &&
      cluster.status === "active"
    ) {
      loadPods();
    }
  }, [activeTab, cluster?.status, loadPods]);

  // Refetch events whenever user switches to the events tab
  useEffect(() => {
    if (
      activeTab === "events" &&
      cluster?.status &&
      cluster.status === "active"
    ) {
      loadEvents();
    }
  }, [activeTab, cluster?.status, loadEvents]);

  const handleExtend = async () => {
    setActionError(null);
    const hours = parseInt(extendTTLHours, 10);
    if (isNaN(hours) || hours < 1 || hours > 168) {
      setActionError("TTL must be between 1 and 168 hours");
      return;
    }
    try {
      setExtending(true);
      await apiClient.post(`/api/clusters/${clusterId}/extend`, {
        ttl_hours: hours,
      });
      setShowExtendTTLModal(false);
      setExtendTTLHours("2");
      loadClusterDetails();
    } catch (err) {
      setActionError(`Failed to extend TTL: ${err.message}`);
    } finally {
      setExtending(false);
    }
  };

  const handleDestroy = () => {
    setShowDestroyModal(true);
  };

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

  if (loading) return <div className="loading">Loading cluster details...</div>;
  if (error) return <div className="error">Error: {error}</div>;
  if (!cluster) return <div className="error">Cluster not found</div>;

  const breadcrumb = [
    { label: "Clusters", href: "/clusters" },
    { label: cluster.id },
  ];

  const deploymentColumns = [
    {
      key: "deployment_id",
      label: "ID",
      render: (id) => <Guid value={id} linkTo={`/deployments/${id}`} />,
    },
    {
      key: "status",
      label: "Status",
      render: (status) => <StatusBadge status={status} />,
    },
    {
      key: "manifest_version",
      label: "Profile",
      render: (profile) =>
        profile ? (
          <span
            onClick={() => route(`/config/profiles/${profile}`)}
            className="config-pill"
            style="cursor: pointer; transition: background 0.2s;"
            onMouseEnter={(e) => (e.target.style.background = "var(--base01)")}
            onMouseLeave={(e) => (e.target.style.background = "var(--base02)")}
          >
            {profile}
          </span>
        ) : (
          "-"
        ),
    },
    { key: "deployed_by", label: "Deployed By" },
    {
      key: "deployed_at",
      label: "Deployed",
      render: (date) => {
        if (!date) return "-";
        const deployedDate = new Date(date);
        const now = new Date();
        const diffMs = now - deployedDate;
        const diffMins = Math.floor(diffMs / 60000);
        const diffHours = Math.floor(diffMs / 3600000);
        const diffDays = Math.floor(diffMs / 86400000);

        if (diffMins < 1) return "Just now";
        if (diffMins < 60) return `${diffMins}m ago`;
        if (diffHours < 24) return `${diffHours}h ago`;
        return `${diffDays}d ago`;
      },
    },
  ];

  const podColumns = [
    { key: "name", label: "Pod Name" },
    {
      key: "status",
      label: "Status",
      render: (status) => <StatusBadge status={status} />,
    },
    {
      key: "ready",
      label: "Ready",
      render: (ready, row) => {
        if (!ready) return "-";
        // Parse ready string like "2/2" or "0/2"
        const [readyCount, totalCount] = ready.split("/").map(Number);
        const isReady = readyCount === totalCount && totalCount > 0;
        return (
          <span
            className={`status-badge ${isReady ? "status-bright-green" : "status-muted"}`}
          >
            {ready}
          </span>
        );
      },
    },
    { key: "restarts", label: "Restarts" },
    { key: "age", label: "Age" },
    {
      key: "ip",
      label: "IP Address",
      render: (ip) => (ip ? <CopyableText value={ip} /> : "-"),
    },
    {
      key: "image",
      label: "Image",
      render: (image) => {
        // Shorten image name - show just repo:tag
        if (!image) return "-";
        const parts = image.split("/");
        return parts[parts.length - 1];
      },
    },
  ];

  const auditColumns = [
    {
      key: "from_state",
      label: "From",
      render: (state) => (state ? <StatusBadge status={state} /> : "-"),
    },
    {
      key: "to_state",
      label: "To",
      render: (state) => <StatusBadge status={state} />,
    },
    { key: "actor", label: "Actor" },
    { key: "reason", label: "Reason" },
    {
      key: "timestamp",
      label: "Timestamp",
      render: (date) => formatDateTime(date),
    },
  ];

  const eventColumns = [
    {
      key: "last_timestamp",
      label: "Time",
      render: (ts) => {
        if (!ts) return "-";
        const date = new Date(ts);
        const now = new Date();
        const diffMs = now - date;
        const diffMins = Math.floor(diffMs / 60000);
        const diffHours = Math.floor(diffMs / 3600000);
        if (diffMins < 1) return "Just now";
        if (diffMins < 60) return `${diffMins}m ago`;
        if (diffHours < 24) return `${diffHours}h ago`;
        return formatDateTime(ts);
      },
    },
    {
      key: "type",
      label: "Type",
      render: (type) => (
        <span
          style={{
            color: type === "Warning" ? "var(--yellow)" : "var(--green)",
            fontWeight: type === "Warning" ? "bold" : "normal",
          }}
        >
          {type}
        </span>
      ),
    },
    { key: "reason", label: "Reason" },
    {
      key: "involved_object_kind",
      label: "Object",
      render: (kind, row) => {
        const display = `${kind}/${row.involved_object_name}`;
        if (kind === "Pod") {
          return (
            <span
              onClick={(e) => {
                e.stopPropagation();
                route(
                  `/clusters/${clusterId}/pods/${row.namespace}/${row.involved_object_name}`,
                );
              }}
              style={{ color: "var(--blue)", cursor: "pointer" }}
            >
              {display}
            </span>
          );
        }
        return display;
      },
    },
    {
      key: "message",
      label: "Message",
      render: (msg) => (
        <span style={{ fontSize: "0.9em", wordBreak: "break-word" }}>
          {msg}
        </span>
      ),
    },
    {
      key: "count",
      label: "Count",
      render: (count) => (count > 1 ? count : "-"),
    },
  ];

  const handlePodClick = (pod) => {
    route(`/clusters/${clusterId}/pods/${pod.namespace}/${pod.name}`);
  };

  // Disable actions for destroyed/destroying clusters
  const isClusterDestroyed = [
    "destroying",
    "destroyed",
    "zombie",
    "unmanaged",
  ].includes(cluster.status);

  // Snapshot is only available for active clusters
  const canSnapshot = cluster.status === "active";

  // Rehabilitate is available for destroyed/destroy-failed/zombie clusters
  const canRehabilitate = ["destroyed", "destroy-failed", "zombie"].includes(
    cluster.status,
  );

  const handleRehabilitate = async () => {
    try {
      setRehabilitating(true);
      await apiClient.post(`/api/clusters/${clusterId}/rehabilitate`);
      setShowRehabilitateConfirm(false);
      loadClusterDetails();
    } catch (err) {
      setRehabilitating(false);
      throw err; // Re-throw to let ConfirmModal handle it
    } finally {
      setRehabilitating(false);
    }
  };

  const actions = (
    <>
      <button
        onClick={() => setShowSnapshotModal(true)}
        className="btn-secondary"
        disabled={!canSnapshot}
        title={
          canSnapshot
            ? "Create a snapshot of this cluster's data"
            : "Snapshots can only be created for active clusters"
        }
      >
        Create Snapshot
      </button>
      <button
        onClick={() => setShowExtendTTLModal(true)}
        className="btn-secondary"
        disabled={isClusterDestroyed}
        title={
          isClusterDestroyed
            ? "Cannot extend TTL of destroyed/destroying cluster"
            : "Extend cluster TTL"
        }
      >
        Extend TTL
      </button>
      <button
        onClick={handleDestroy}
        className="btn-danger"
        disabled={isClusterDestroyed}
        title={
          isClusterDestroyed
            ? "Cluster is already destroyed/destroying"
            : "Destroy this cluster"
        }
      >
        Destroy
      </button>
      {canRehabilitate && (
        <button
          onClick={() => setShowRehabilitateConfirm(true)}
          className="btn-secondary"
          disabled={rehabilitating}
          title="Attempt to restore cluster to active status if infrastructure still exists"
        >
          {rehabilitating ? "Rehabilitating..." : "Rehabilitate"}
        </button>
      )}
    </>
  );

  // worklist 11: prefer the non-superseded deployment as "current" rather than
  // positional currentDeployment (the API now exposes superseded_by).
  const currentDeployment =
    deployments.find((d) => !d.superseded_by) || deployments.at(0);

  return (
    <div className="page">
      <Breadcrumb path={breadcrumb} />

      {actionError && (
        <div className="modal-error" style="margin-bottom: 1rem;">
          {actionError}
        </div>
      )}

      <Card
        title={
          <span style="display: flex; align-items: center; gap: 0.5rem;">
            Cluster: <Guid value={cluster.id} />
          </span>
        }
        actions={actions}
      >
        <InfoGrid>
          <InfoGridRow label="Status">
            <StatusBadge status={cluster.status} />
          </InfoGridRow>
          <InfoGridRow label="TTL">
            <span>{formatTimeRemaining(cluster.expires_at)}</span>
          </InfoGridRow>
          <InfoGridRow label="Repository">
            <span>{cluster.repository || "-"}</span>
          </InfoGridRow>
          <InfoGridRow label="Branch">
            <span>{cluster.branch}</span>
          </InfoGridRow>
          <InfoGridRow label="Created">
            <span>
              {cluster.created_at ? formatDateTime(cluster.created_at) : "-"}
            </span>
          </InfoGridRow>
          <InfoGridRow label="Origin">
            <span>{cluster.origin}</span>
          </InfoGridRow>
          {cluster.last_reconciled_at && (
            <InfoGridRow label="Last Reconciled">
              <span style="display: flex; align-items: center; gap: 0.35rem;">
                {formatDateTime(cluster.last_reconciled_at)}
                {cluster.reconciliation_stale && (
                  <span style="color: var(--yellow); display: flex; align-items: center; gap: 0.25rem;">
                    ⚠{" "}
                    <span style="font-size: 0.85em;">
                      Infrastructure unreachable
                    </span>
                  </span>
                )}
              </span>
            </InfoGridRow>
          )}
          {cluster.public_ip && (
            <InfoGridRow label="IP">
              <CopyableText value={cluster.public_ip} />
            </InfoGridRow>
          )}
          {cluster.cluster_url && (
            <InfoGridRow label="URL">
              <a
                href={cluster.cluster_url}
                target="_blank"
                rel="noopener noreferrer"
                style={{ color: "var(--blue)", marginRight: "0.5rem" }}
              >
                {cluster.dns_hostname}
              </a>
              <CopyableText value={cluster.cluster_url} showValue={false} />
            </InfoGridRow>
          )}
        </InfoGrid>
      </Card>

      <div style="margin-top: 2rem;">
        <TabNav
          tabs={[
            { id: "deployments", label: "Deployments" },
            // Only show Pods and Events tabs for active or deploying clusters
            ...(cluster.status === "active"
              ? [
                  { id: "pods", label: "Pods" },
                  { id: "events", label: "Events" },
                ]
              : []),
            { id: "history", label: "History" },
          ]}
          activeTab={activeTab}
          onTabChange={setActiveTab}
        />

        {activeTab === "deployments" && (
          <>
            {deployments.length > 0 ? (
              <>
                {/* Current Deployment Card */}
                {currentDeployment &&
                  ["new", "pending", "deploying", "active"].includes(
                    currentDeployment.status,
                  ) && (
                    <Card
                      title={
                        <span style="display: flex; align-items: center; gap: 0.5rem;">
                          Current Deployment:{" "}
                          <Guid
                            value={currentDeployment.deployment_id}
                            linkTo={`/deployments/${currentDeployment.deployment_id}`}
                          />
                        </span>
                      }
                      style="margin-bottom: 2rem; border: 2px solid var(--blue);"
                    >
                      <InfoGrid>
                        <InfoGridRow label="Status">
                          <StatusBadge status={currentDeployment.status} />
                        </InfoGridRow>
                        <InfoGridRow label="Profile">
                          <span
                            onClick={() =>
                              route(
                                `/config/profiles/${currentDeployment.manifest_version}`,
                              )
                            }
                            className="config-pill"
                            style="cursor: pointer; transition: background 0.2s;"
                            onMouseEnter={(e) =>
                              (e.target.style.background = "var(--base01)")
                            }
                            onMouseLeave={(e) =>
                              (e.target.style.background = "var(--base02)")
                            }
                          >
                            {currentDeployment.manifest_version}
                          </span>
                        </InfoGridRow>
                        <InfoGridRow label="Deployed">
                          <span>
                            {currentDeployment.deployed_at
                              ? formatDateTime(currentDeployment.deployed_at)
                              : "-"}
                          </span>
                        </InfoGridRow>
                        <InfoGridRow label="Deployed By">
                          <span>{currentDeployment.deployed_by || "-"}</span>
                        </InfoGridRow>
                        {currentDeployment.failure_reason && (
                          <InfoGridRow label="Error" fullWidth>
                            <span className="error-text">
                              {currentDeployment.failure_reason}
                            </span>
                          </InfoGridRow>
                        )}
                        {currentDeployment.status === "deploying" && (
                          <InfoGridRow label="" fullWidth>
                            <p
                              className="warning-text"
                              style="margin: 0.5rem 0 0 0;"
                            >
                              ⏳ Deployment in progress... Waiting for rollout
                              to complete.
                            </p>
                          </InfoGridRow>
                        )}
                      </InfoGrid>
                      {currentDeployment.resolved_images &&
                        Object.keys(currentDeployment.resolved_images).length > 0 && (
                          <div style="margin-top: 1rem;">
                            <h4 style="margin-bottom: 0.5rem;">Services</h4>
                            <div style="display: flex; flex-wrap: wrap; gap: 0.5rem;">
                              {Object.keys(currentDeployment.resolved_images).map(
                                (service) => (
                                  <span
                                    key={service}
                                    className="status-badge status-muted"
                                  >
                                    {service}
                                  </span>
                                ),
                              )}
                            </div>
                          </div>
                        )}
                    </Card>
                  )}

                {/* Deployment History */}
                <h3 style="margin-top: 2rem; margin-bottom: 1rem;">
                  Deployment History
                </h3>
                <Table
                  columns={deploymentColumns}
                  data={deployments}
                  onRowClick={(d) => route(`/deployments/${d.deployment_id}`)}
                  keyField="deployment_id"
                />
              </>
            ) : (
              <p>No deployments found for this cluster.</p>
            )}
          </>
        )}

        {activeTab === "pods" &&
          cluster.status === "active" && (
            <>
              {pods.length > 0 ? (
                <Table
                  columns={podColumns}
                  data={pods}
                  onRowClick={handlePodClick}
                  keyField="name"
                />
              ) : (
                <p>No pods found or cluster not yet provisioned.</p>
              )}
            </>
          )}

        {activeTab === "events" &&
          cluster.status === "active" && (
            <>
              {events.length > 0 ? (
                <Table columns={eventColumns} data={events} keyField="name" />
              ) : (
                <p>No events found for this cluster.</p>
              )}
            </>
          )}

        {activeTab === "history" && (
          <>
            {/* Restore History */}
            {restoreHistory.length > 0 && (
              <Card title="Snapshot Restores" style="margin-bottom: 1.5rem;">
                <Table
                  columns={[
                    {
                      key: "snapshot_name",
                      label: "Snapshot",
                      render: (name, row) =>
                        row.snapshot_id ? (
                          <span style="display: flex; align-items: center; gap: 0.5rem;">
                            <span style="font-weight: 500;">
                              {name || "Unnamed"}
                            </span>
                            <Guid
                              value={row.snapshot_id}
                              linkTo={`/snapshots/${row.snapshot_id}`}
                            />
                          </span>
                        ) : (
                          "-"
                        ),
                    },
                    {
                      key: "snapshot_branch",
                      label: "Branch",
                      render: (branch) => branch || "-",
                    },
                    {
                      key: "status",
                      label: "Status",
                      render: (status) => <StatusBadge status={status} />,
                    },
                    {
                      key: "services_completed",
                      label: "Services",
                      render: (completed, row) =>
                        `${completed || 0}/${row.services_total || 0}`,
                    },
                    {
                      key: "initiated_by",
                      label: "Initiated By",
                    },
                    {
                      key: "started_at",
                      label: "When",
                      render: (ts) => formatDateTime(ts),
                    },
                  ]}
                  data={restoreHistory}
                  keyField="id"
                />
              </Card>
            )}

            {/* State Audit History */}
            <Card title="State History">
              {auditHistory.length > 0 ? (
                <Table
                  columns={auditColumns}
                  data={auditHistory}
                  keyField="id"
                />
              ) : (
                <p style="padding: 1rem; color: var(--text-light);">
                  No state history available for this cluster.
                </p>
              )}
            </Card>
          </>
        )}
      </div>

      {showSnapshotModal && (
        <CreateSnapshotModal
          onClose={() => setShowSnapshotModal(false)}
          preselectedClusterId={clusterId}
        />
      )}

      {showDestroyModal && cluster && (
        <DestroyClusterModal
          cluster={cluster}
          onClose={() => setShowDestroyModal(false)}
          onDestroyed={() => route("/clusters")}
        />
      )}

      {showRehabilitateConfirm && (
        <ConfirmModal
          title="Rehabilitate Cluster"
          message="This will attempt to restore the cluster to active status. The cluster infrastructure must still be running and reachable."
          confirmLabel="Rehabilitate"
          confirmClass="btn-primary"
          onConfirm={handleRehabilitate}
          onCancel={() => setShowRehabilitateConfirm(false)}
          loading={rehabilitating}
        />
      )}

      {showExtendTTLModal && (
        <Modal
          title="Extend Cluster TTL"
          onClose={() => {
            setShowExtendTTLModal(false);
            setActionError(null);
          }}
        >
          <div className="form-group">
            <label>Hours to extend</label>
            <input
              type="number"
              min="1"
              max="168"
              value={extendTTLHours}
              onChange={(e) => setExtendTTLHours(e.target.value)}
              placeholder="2"
            />
            <small style="color: var(--color-text-muted);">
              Add 1-168 hours to the cluster's current expiration time
            </small>
          </div>

          {actionError && <div className="modal-error">{actionError}</div>}

          <div className="modal-actions">
            <button
              className="btn-secondary"
              onClick={() => {
                setShowExtendTTLModal(false);
                setActionError(null);
              }}
              disabled={extending}
            >
              Cancel
            </button>
            <button
              className="btn-primary"
              onClick={handleExtend}
              disabled={extending}
            >
              {extending ? "Extending..." : "Extend TTL"}
            </button>
          </div>
        </Modal>
      )}
    </div>
  );
}
