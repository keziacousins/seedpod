import { useEffect, useState, useCallback, useRef } from "preact/hooks";
import { route } from "preact-router";
import { Card } from "../components/Card";
import { Table } from "../components/Table";
import { StatusBadge } from "../components/StatusBadge";
import { Breadcrumb } from "../components/Breadcrumb";
import { InfoGrid, InfoGridRow } from "../components/InfoGrid";
import { Guid } from "../components/Guid";
import { ConfirmModal } from "../components/ConfirmModal";
import { apiClient } from "../lib/api-client";
import { sseClient } from "../lib/sse-client";
import { formatDateTime } from "../lib/time-utils";

export function DeploymentDetail({ deploymentId }) {
  const [deployment, setDeployment] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [redeploying, setRedeploying] = useState(false);
  const [retriggering, setRetriggering] = useState(false);
  const [cancelling, setCancelling] = useState(false);
  const [showRedeployConfirm, setShowRedeployConfirm] = useState(false);
  const [showRetriggerConfirm, setShowRetriggerConfirm] = useState(false);
  const [showCancelConfirm, setShowCancelConfirm] = useState(false);

  console.log("DeploymentDetail mounted with deploymentId:", deploymentId);

  const loadDeploymentDetails = useCallback(async () => {
    if (!deploymentId) {
      setError("No deployment ID provided");
      setLoading(false);
      return;
    }

    try {
      setLoading(true);
      const data = await apiClient.get(`/api/deployments/${deploymentId}`);
      setDeployment(data);
      setError(null);
    } catch (err) {
      const errorMessage =
        err?.message || err?.toString() || "Unknown error occurred";
      setError(errorMessage);
      console.error("Failed to load deployment details:", {
        error: err,
        message: err?.message,
        deploymentId: deploymentId,
      });
    } finally {
      setLoading(false);
    }
  }, [deploymentId]);

  // Store load function and error state in refs so SSE handler always calls latest version
  const loadDeploymentDetailsRef = useRef(loadDeploymentDetails);
  const errorRef = useRef(error);
  useEffect(() => {
    loadDeploymentDetailsRef.current = loadDeploymentDetails;
    errorRef.current = error;
  }, [loadDeploymentDetails, error]);

  // Load data when deploymentId changes
  useEffect(() => {
    loadDeploymentDetails();
  }, [deploymentId, loadDeploymentDetails]);

  // Setup SSE listeners once on mount - handler is stable
  useEffect(() => {
    const handleDeploymentStatusChange = (event) => {
      // SSE events have structure: { type, data: {...}, timestamp }
      const eventData = event.data || event;
      console.log(
        "[DeploymentDetail] Deployment status change event received:",
        eventData,
      );

      if (eventData.deployment_id === deploymentId) {
        console.log(
          "[DeploymentDetail] This deployment changed - reloading data",
        );
        loadDeploymentDetailsRef.current();
      } else {
        console.log(
          "[DeploymentDetail] Different deployment changed:",
          eventData.deployment_id,
          "vs",
          deploymentId,
        );
      }
    };

    const handleReconnected = () => {
      loadDeploymentDetailsRef.current();
    };

    const handleConnected = () => {
      if (errorRef.current) {
        loadDeploymentDetailsRef.current();
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
  }, [deploymentId]); // Include deploymentId so handler gets latest value

  const handleRedeploy = async () => {
    try {
      setRedeploying(true);
      const result = await apiClient.post(
        `/api/deployments/${deploymentId}/redeploy`,
        {},
      );

      console.log("Redeploy initiated:", result);

      // Brief delay for visual feedback before navigating
      await new Promise((resolve) => setTimeout(resolve, 500));

      // Close modal and navigate to the new deployment
      setShowRedeployConfirm(false);
      route(`/deployments/${result.deployment_id}`);
    } catch (err) {
      setRedeploying(false);
      throw err; // Re-throw to let ConfirmModal handle it
    }
  };

  const handleRetrigger = async () => {
    try {
      setRetriggering(true);
      const result = await apiClient.post(
        `/api/deployments/${deploymentId}/retrigger`,
        {},
      );

      console.log("Retrigger initiated:", result);

      // Brief delay for visual feedback before navigating
      await new Promise((resolve) => setTimeout(resolve, 500));

      // Close modal and navigate to the new deployment
      setShowRetriggerConfirm(false);
      route(`/deployments/${result.new_deployment_id}`);
    } catch (err) {
      setRetriggering(false);
      throw err; // Re-throw to let ConfirmModal handle it
    }
  };

  const handleCancel = async () => {
    try {
      setCancelling(true);
      await apiClient.post(`/api/deployments/${deploymentId}/cancel`, {});

      console.log("Deployment cancelled");
      setShowCancelConfirm(false);
      // Reload to show updated status
      loadDeploymentDetails();
    } catch (err) {
      setCancelling(false);
      throw err; // Re-throw to let ConfirmModal handle it
    } finally {
      setCancelling(false);
    }
  };

  if (loading)
    return <div className="loading">Loading deployment details...</div>;
  if (error) return <div className="error">Error: {error}</div>;
  if (!deployment) return <div className="error">Deployment not found</div>;

  const breadcrumb = [
    { label: "Deployments", href: "/deployments" },
    { label: `Deployment ${deployment.deployment_id}` },
  ];

  const auditColumns = [
    { key: "triggering_repo", label: "Repository" },
    { key: "triggering_branch", label: "Branch" },
    {
      key: "commit_sha",
      label: "Commit",
      render: (sha) => (sha ? sha.substring(0, 7) : "-"),
    },
    {
      key: "deployment_profile_name",
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
    {
      key: "resolution_strategy",
      label: "Strategy",
      render: (strategy) =>
        strategy ? (
          <span
            onClick={() => route(`/config/strategies/${strategy}`)}
            className="config-pill"
            style="cursor: pointer; transition: background 0.2s;"
            onMouseEnter={(e) => (e.target.style.background = "var(--base01)")}
            onMouseLeave={(e) => (e.target.style.background = "var(--base02)")}
          >
            {strategy}
          </span>
        ) : (
          "-"
        ),
    },
    {
      key: "created_at",
      label: "Created At",
      render: (date) => formatDateTime(date),
    },
  ];

  return (
    <div className="page">
      <Breadcrumb path={breadcrumb} />

      <Card
        title="Deployment Status"
        style={`border: 2px solid ${
          deployment.status === "active"
            ? "var(--green)"
            : deployment.status === "failed"
              ? "var(--red)"
              : deployment.status === "deploying"
                ? "var(--blue)"
                : "var(--base01)"
        };`}
        actions={
          <div style={{ display: "flex", gap: "0.5rem" }}>
            {/* Show Cancel button only for new/pending/deploying deployments */}
            {(deployment.status === "new" ||
              deployment.status === "pending" ||
              deployment.status === "deploying") && (
              <button
                onClick={() => setShowCancelConfirm(true)}
                disabled={cancelling}
                style={{
                  padding: "0.5rem 1rem",
                  backgroundColor: "var(--orange)",
                  color: "var(--base3)",
                  border: "none",
                  borderRadius: "4px",
                  cursor: cancelling ? "not-allowed" : "pointer",
                  opacity: cancelling ? 0.6 : 1,
                  fontSize: "0.875rem",
                  fontWeight: 500,
                }}
                title="Cancel this deployment (supersedes it; does not change cluster state)"
              >
                {cancelling ? "Cancelling..." : "Cancel & Undo"}
              </button>
            )}
            <button
              onClick={() => setShowRetriggerConfirm(true)}
              disabled={retriggering}
              style={{
                padding: "0.5rem 1rem",
                backgroundColor: "var(--green)",
                color: "var(--base3)",
                border: "none",
                borderRadius: "4px",
                cursor: retriggering ? "not-allowed" : "pointer",
                opacity: retriggering ? 0.6 : 1,
                fontSize: "0.875rem",
                fontWeight: 500,
              }}
              title="Re-run full deployment workflow (manifest resolution + cluster provisioning if needed)"
            >
              {retriggering ? "Retriggering..." : "Retrigger"}
            </button>
            <button
              onClick={() => setShowRedeployConfirm(true)}
              disabled={redeploying}
              style={{
                padding: "0.5rem 1rem",
                backgroundColor: "var(--blue)",
                color: "var(--base3)",
                border: "none",
                borderRadius: "4px",
                cursor: redeploying ? "not-allowed" : "pointer",
                opacity: redeploying ? 0.6 : 1,
                fontSize: "0.875rem",
                fontWeight: 500,
              }}
              title="Redeploy using exact same manifests on same cluster (must be active)"
            >
              {redeploying ? "Redeploying..." : "Redeploy"}
            </button>
          </div>
        }
      >
        <InfoGrid>
          <InfoGridRow label="Deployment ID">
            <Guid value={deployment.deployment_id} />
          </InfoGridRow>
          <InfoGridRow label="Status">
            <StatusBadge status={deployment.status} />
          </InfoGridRow>
          <InfoGridRow label="Cluster ID">
            <Guid
              value={deployment.cluster_id}
              linkTo={`/clusters/${deployment.cluster_id}`}
            />
          </InfoGridRow>
          <InfoGridRow label="Profile">
            <span
              onClick={() =>
                route(`/config/profiles/${deployment.manifest_version}`)
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
              {deployment.manifest_version}
            </span>
          </InfoGridRow>
          <InfoGridRow label="Deployed At">
            <span>
              {deployment.deployed_at
                ? formatDateTime(deployment.deployed_at)
                : "-"}
            </span>
          </InfoGridRow>
          <InfoGridRow label="Deployed By">
            <span>{deployment.deployed_by || "-"}</span>
          </InfoGridRow>
          {deployment.superseded_by && (
            <InfoGridRow label="Superseded By">
              <Guid
                value={deployment.superseded_by}
                linkTo={`/deployments/${deployment.superseded_by}`}
              />
            </InfoGridRow>
          )}
          {deployment.spec_ref && (
            <InfoGridRow label="Spec Ref">
              <span style="font-family: monospace; font-size: 0.875rem;">
                {deployment.spec_ref}
              </span>
            </InfoGridRow>
          )}
          {deployment.failure_reason && (
            <InfoGridRow label="Error" fullWidth>
              <span className="error-text">{deployment.failure_reason}</span>
            </InfoGridRow>
          )}
          {deployment.status === "deploying" && (
            <InfoGridRow label="" fullWidth>
              <p className="warning-text" style="margin: 0.5rem 0 0 0;">
                ⏳ Deployment in progress... Waiting for rollout to complete.
              </p>
            </InfoGridRow>
          )}
        </InfoGrid>

        {deployment.resolved_images && Object.keys(deployment.resolved_images).length > 0 && (
          <div style="margin-top: 1.5rem;">
            <h3>Services</h3>
            <table style="width: 100%; border-collapse: collapse;">
              <thead>
                <tr style="border-bottom: 1px solid var(--base01);">
                  <th style="text-align: left; padding: 0.5rem; width: 25%;">
                    Service
                  </th>
                  <th style="text-align: left; padding: 0.5rem; width: 75%;">
                    Image
                  </th>
                </tr>
              </thead>
              <tbody>
                {Object.entries(deployment.resolved_images).map(([service, image]) => (
                  <tr
                    key={service}
                    style="border-bottom: 1px solid var(--base02);"
                  >
                    <td style="padding: 0.5rem; width: 25%;">{service}</td>
                    <td style="padding: 0.5rem; font-family: monospace; font-size: 0.875rem; width: 75%;">
                      {image}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Card>

      <h3 style="margin-top: 2rem;">Audit History</h3>
      {deployment.audit_history && deployment.audit_history.length > 0 ? (
        <Table
          columns={auditColumns}
          data={deployment.audit_history}
          keyField="id"
        />
      ) : (
        <p>No audit history available for this deployment.</p>
      )}

      {/* Redeploy Confirmation Modal */}
      {showRedeployConfirm && (
        <ConfirmModal
          title="Redeploy"
          message="Redeploy this deployment using the exact same manifests? This will trigger a fresh rollout on the cluster."
          confirmLabel="Redeploy"
          confirmClass="btn-primary"
          onConfirm={handleRedeploy}
          onCancel={() => setShowRedeployConfirm(false)}
          loading={redeploying}
        />
      )}

      {/* Retrigger Confirmation Modal */}
      {showRetriggerConfirm && (
        <ConfirmModal
          title="Retrigger Deployment"
          message="Re-trigger the full deployment workflow? This will re-run manifest resolution with latest images, create a new cluster if the original is destroyed, or deploy to existing cluster if still running."
          confirmLabel="Retrigger"
          confirmClass="btn-primary"
          onConfirm={handleRetrigger}
          onCancel={() => setShowRetriggerConfirm(false)}
          loading={retriggering}
        />
      )}

      {/* Cancel Confirmation Modal */}
      {showCancelConfirm && (
        <ConfirmModal
          title="Cancel Deployment"
          message="Cancel and undo this deployment? This supersedes the deployment and runs its rollback (kubectl rollout undo). The cluster's own state is unaffected."
          confirmLabel="Cancel & Undo"
          onConfirm={handleCancel}
          onCancel={() => setShowCancelConfirm(false)}
          loading={cancelling}
        />
      )}
    </div>
  );
}
