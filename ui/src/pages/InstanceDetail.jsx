import { useEffect, useRef, useState } from "preact/hooks";
import { Card } from "../components/Card";
import { Table } from "../components/Table";
import { StatusBadge } from "../components/StatusBadge";
import { Breadcrumb } from "../components/Breadcrumb";
import { InfoGrid, InfoGridRow } from "../components/InfoGrid";
import { Guid } from "../components/Guid";
import { CopyableText } from "../components/CopyableText";
import { ConfirmModal } from "../components/ConfirmModal";
import { apiClient } from "../lib/api-client";
import { sseClient } from "../lib/sse-client";
import { formatDateTime, formatDuration, formatRemaining } from "../lib/time-utils";

const auditColumns = [
  { key: "from_state", label: "From", render: (s) => (s ? <StatusBadge status={s} /> : "-") },
  { key: "to_state", label: "To", render: (s) => <StatusBadge status={s} /> },
  { key: "event", label: "Event" },
  { key: "actor", label: "Actor" },
  { key: "reason", label: "Reason" },
  { key: "created_at", label: "Timestamp", render: (d) => formatDateTime(d) },
];

export function InstanceDetail({ instanceId }) {
  const [instance, setInstance] = useState(null);
  const [audit, setAudit] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [actionError, setActionError] = useState(null);
  const [notice, setNotice] = useState(null);
  const [renewing, setRenewing] = useState(false);
  const [showDestroy, setShowDestroy] = useState(false);
  const [destroying, setDestroying] = useState(false);
  const [now, setNow] = useState(Date.now());
  const idRef = useRef(null); // the row's id: the route may name the instance by slug
  idRef.current = instance?.id;

  const load = async (showLoader = true) => {
    try {
      if (showLoader) setLoading(true);
      setInstance(await apiClient.get(`/api/instances/${instanceId}`));
      setError(null);
      const history = await apiClient.get(`/api/instances/${instanceId}/audit`);
      setAudit(history.audit.map((row, n) => ({ ...row, n })));
    } catch (err) {
      setError(err.message);
    } finally {
      if (showLoader) setLoading(false);
    }
  };

  useEffect(() => {
    load();
    const refetch = (event) => {
      const data = event.data || event;
      if (data.instance_id === instanceId || data.instance_id === idRef.current) load(false);
    };
    const reload = () => load(false);
    sseClient.on("instance_state_changed", refetch);
    sseClient.on("reconnected", reload);
    const tick = setInterval(() => setNow(Date.now()), 1000);
    return () => {
      sseClient.off("instance_state_changed", refetch);
      sseClient.off("reconnected", reload);
      clearInterval(tick);
    };
  }, [instanceId]);

  const handleRenew = async () => {
    try {
      setActionError(null);
      setRenewing(true);
      const renewed = await apiClient.post(`/api/instances/${instanceId}/renew`);
      setInstance(renewed);
      setNotice(`Renewed. New expiry: ${formatDateTime(renewed.expires_at)}`);
      load(false);
    } catch (err) {
      setNotice(null);
      setActionError(err.message);
    } finally {
      setRenewing(false);
    }
  };

  const handleDestroy = async () => {
    setDestroying(true); // one DELETE, even on a double click
    try {
      await apiClient.delete(`/api/instances/${instanceId}`); // ConfirmModal shows a failure
    } finally {
      setDestroying(false);
    }
    setShowDestroy(false);
    setNotice(null);
    load(false);
  };

  if (loading) return <div className="loading">Loading instance...</div>;
  if (error && !instance) return <div className="error">Error: {error}</div>;

  const live = instance.status === "provisioning" || instance.status === "active";
  const actions = (
    <>
      {instance.status === "active" && (
        <button className="btn-secondary" onClick={handleRenew} disabled={renewing}>
          {renewing ? "Renewing..." : "Renew"}
        </button>
      )}
      {instance.status !== "destroying" && instance.status !== "destroyed" && (
        <button className="btn-danger" onClick={() => setShowDestroy(true)}>
          Destroy
        </button>
      )}
    </>
  );

  return (
    <div className="page">
      <Breadcrumb path={[{ label: "Instances", href: "/instances" }, { label: instance.slug }]} />
      {error && <div className="error-banner">{error}</div>}
      {actionError && <div className="modal-error" style="margin-bottom: 1rem;">{actionError}</div>}
      {notice && <div style="margin-bottom: 1rem; color: var(--green);">{notice}</div>}

      <Card title={`Instance: ${instance.slug}`} actions={actions}>
        <InfoGrid>
          <InfoGridRow label="Status"><StatusBadge status={instance.status} /></InfoGridRow>
          <InfoGridRow label="ID"><Guid value={instance.id} /></InfoGridRow>
          <InfoGridRow label="Address"><CopyableText value={instance.address} /></InfoGridRow>
          <InfoGridRow label="Provider">{instance.provider}</InfoGridRow>
          <InfoGridRow label="Environment">{instance.environment}</InfoGridRow>
          <InfoGridRow label="Template">{instance.template}</InfoGridRow>
          <InfoGridRow label="Size">
            {instance.size} ({instance.vcpu} vCPU / {instance.memory_mb} MB)
          </InfoGridRow>
          <InfoGridRow label="Price">${instance.hourly_price}/h</InfoGridRow>
          <InfoGridRow label="Owner key">{instance.owner_key_id}</InfoGridRow>
          <InfoGridRow label="Lease">{formatDuration(instance.lease_seconds)}</InfoGridRow>
          <InfoGridRow label="Expires">
            {formatDateTime(instance.expires_at)}
            {live && ` (${formatRemaining(instance.expires_at, now)})`}
          </InfoGridRow>
          <InfoGridRow label="Hard limit">{formatDateTime(instance.hard_expires_at)}</InfoGridRow>
          <InfoGridRow label="Created">{formatDateTime(instance.created_at)}</InfoGridRow>
          <InfoGridRow label="Updated">{formatDateTime(instance.updated_at)}</InfoGridRow>
          {instance.destroyed_at && (
            <InfoGridRow label="Destroyed">{formatDateTime(instance.destroyed_at)}</InfoGridRow>
          )}
          {instance.failure_reason && (
            <InfoGridRow label="Failure" fullWidth>
              <span className="error-text">{instance.failure_reason}</span>
            </InfoGridRow>
          )}
        </InfoGrid>
      </Card>

      <Card title="State history">
        <Table columns={auditColumns} data={audit} keyField="n" />
      </Card>

      {showDestroy && (
        <ConfirmModal
          title="Destroy Instance"
          message={
            <p>
              Destroy instance <strong>{instance.slug}</strong>? The VM and anything unsaved on it
              are lost.
            </p>
          }
          confirmLabel="Destroy"
          onConfirm={handleDestroy}
          onCancel={() => setShowDestroy(false)}
          loading={destroying}
        />
      )}
    </div>
  );
}
