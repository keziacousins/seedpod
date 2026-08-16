import { useState, useEffect } from "preact/hooks";
import { route } from "preact-router";
import { Modal } from "./Modal";
import { Guid } from "./Guid";
import { apiClient } from "../lib/api-client";

function formatBytes(bytes) {
  if (!bytes || bytes === 0) return "0 B";
  const k = 1024;
  const sizes = ["B", "KB", "MB", "GB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + " " + sizes[i];
}

export function PresetDeployModal({ preset, profile, providers, onClose }) {
  const [deployBranch, setDeployBranch] = useState("");
  const [deployProvider, setDeployProvider] = useState("");
  const [deployTtl, setDeployTtl] = useState("");
  const [clusterName, setClusterName] = useState("");
  const [deploying, setDeploying] = useState(false);
  const [error, setError] = useState(null);

  // Data initialization state
  const [enableDataInit, setEnableDataInit] = useState(false);
  const [dataInitMode, setDataInitMode] = useState("specific"); // "specific" or "latest"
  const [snapshots, setSnapshots] = useState([]);
  const [loadingSnapshots, setLoadingSnapshots] = useState(false);
  const [selectedSnapshotId, setSelectedSnapshotId] = useState("");
  const [selectedSnapshot, setSelectedSnapshot] = useState(null);
  const [latestBranch, setLatestBranch] = useState("");
  const [latestMaxAgeDays, setLatestMaxAgeDays] = useState("7");
  const [selectedServices, setSelectedServices] = useState({});

  // Initialize form values when preset/profile are loaded
  useEffect(() => {
    if (preset) {
      setDeployBranch(preset.default_branch || "main");
      setDeployTtl(preset.default_ttl_hours?.toString() || "24");
    }
  }, [preset, profile]);

  // Load snapshots when data init is enabled
  useEffect(() => {
    if (enableDataInit && snapshots.length === 0) {
      loadSnapshots();
    }
  }, [enableDataInit]);

  // Update selected snapshot details when selection changes
  useEffect(() => {
    if (selectedSnapshotId) {
      const snap = snapshots.find((s) => s.id === selectedSnapshotId);
      setSelectedSnapshot(snap);
      // Initialize all services as selected
      if (snap?.services) {
        const initial = {};
        snap.services.forEach((s) => {
          initial[s.service_name] = true;
        });
        setSelectedServices(initial);
      }
    } else {
      setSelectedSnapshot(null);
      setSelectedServices({});
    }
  }, [selectedSnapshotId, snapshots]);

  const loadSnapshots = async () => {
    try {
      setLoadingSnapshots(true);
      const data = await apiClient.get("/api/snapshots");
      setSnapshots(data.snapshots);
    } catch (err) {
      console.error("Failed to load snapshots:", err);
    } finally {
      setLoadingSnapshots(false);
    }
  };

  const handleServiceToggle = (serviceName) => {
    setSelectedServices((prev) => ({
      ...prev,
      [serviceName]: !prev[serviceName],
    }));
  };

  const getSelectedServiceNames = () => {
    return Object.entries(selectedServices)
      .filter(([_, selected]) => selected)
      .map(([name, _]) => name);
  };

  const profileProviderUnknown = !profile?.provider || profile.provider === "unknown";

  const handleDeploy = async () => {
    try {
      setDeploying(true);
      setError(null);

      if (profileProviderUnknown && !deployProvider) {
        setError("Provider selection is required — this profile has no default provider");
        setDeploying(false);
        return;
      }

      const providerOverride = deployProvider || null;

      // Parse TTL, use null if empty or invalid to use server default
      const ttlHours = deployTtl ? parseInt(deployTtl, 10) : null;

      // Build data_initialization if enabled
      let dataInitialization = null;
      if (enableDataInit) {
        dataInitialization = {};

        if (dataInitMode === "specific" && selectedSnapshotId) {
          dataInitialization.restore_from_snapshot = selectedSnapshotId;
        } else if (dataInitMode === "latest") {
          dataInitialization.restore_from_latest = {
            branch: latestBranch || null,
            profile: null, // Could add profile filter if needed
            max_age_days: latestMaxAgeDays
              ? parseInt(latestMaxAgeDays, 10)
              : null,
          };
        }

        // Add services filter if not all services selected (only for specific snapshot mode)
        if (dataInitMode === "specific" && selectedSnapshot?.services) {
          const servicesToRestore = getSelectedServiceNames();
          if (servicesToRestore.length < selectedSnapshot.services.length) {
            dataInitialization.services = servicesToRestore;
          }
        }
      }

      const result = await apiClient.post(`/api/presets/${preset.id}/deploy`, {
        branch: deployBranch || null,
        provider_override: providerOverride,
        ttl_hours: ttlHours && !isNaN(ttlHours) ? ttlHours : null,
        data_initialization: dataInitialization,
        cluster_name: clusterName || null,
      });

      // Brief delay for visual feedback, then navigate
      await new Promise((resolve) => setTimeout(resolve, 500));

      // Navigate first, then close modal (navigation will unmount anyway)
      route(`/deployments/${result.deployment_id}`);
      onClose();
    } catch (err) {
      setError(err.message);
      setDeploying(false);
    }
  };

  return (
    <Modal onClose={onClose} title="Deploy from Preset">
      <div style="margin-bottom: 1rem; display: flex; flex-direction: column; gap: 0.5rem;">
        <div style="display: flex; align-items: center; gap: 0.5rem;">
          <span style="color: var(--color-text-muted);">Preset:</span>
          <span style="font-weight: 500;">{preset.name}</span>
          <Guid value={preset.id} />
        </div>
        <div style="display: flex; align-items: center; gap: 0.5rem;">
          <span style="color: var(--color-text-muted);">Profile:</span>
          <span
            onClick={() => route(`/config/profiles/${preset.profile_name}`)}
            className="config-pill"
            style="cursor: pointer;"
          >
            {preset.profile_name}
          </span>
        </div>
        {preset.description && (
          <p style="color: var(--color-text-muted); font-size: 0.875rem; margin: 0;">
            {preset.description}
          </p>
        )}
      </div>

      <div className="form-group">
        <label>Branch for Image Discovery</label>
        <input
          type="text"
          value={deployBranch}
          onChange={(e) => setDeployBranch(e.target.value)}
          placeholder="main"
        />
        <small style="color: var(--color-text-muted);">
          Images will be discovered from this branch (unless overridden per
          service)
        </small>
      </div>

      <div className="form-group">
        <label>Provider{profileProviderUnknown ? " *" : ""}</label>
        <select
          value={deployProvider}
          onChange={(e) => setDeployProvider(e.target.value)}
        >
          {profileProviderUnknown ? (
            <option value="">Select a provider...</option>
          ) : (
            <option value="">
              No override (profile default: {profile.provider})
            </option>
          )}
          {providers.map((p) => (
            <option key={p.name} value={p.name}>
              {p.display_name}
            </option>
          ))}
        </select>
        <small style="color: var(--color-text-muted);">
          {profileProviderUnknown
            ? "This profile has no default provider — you must select one"
            : "Override where the cluster will be deployed"}
        </small>
      </div>

      <div className="form-group">
        <label>TTL (hours)</label>
        <input
          type="number"
          min="1"
          max="168"
          value={deployTtl}
          onChange={(e) => setDeployTtl(e.target.value)}
          placeholder="24"
        />
        <small style="color: var(--color-text-muted);">
          How long before the cluster auto-expires (1-168 hours)
        </small>
      </div>

      <div className="form-group">
        <label>Cluster Name Override</label>
        <input
          type="text"
          value={clusterName}
          onChange={(e) => setClusterName(e.target.value)}
          placeholder={
            preset.naming_strategy?.type === "fixed"
              ? preset.naming_strategy.name || "Use preset default"
              : "Auto-generated"
          }
        />
        <small style="color: var(--color-text-muted);">
          {preset.naming_strategy?.type === "fixed" ? (
            <>
              Preset uses fixed naming:{" "}
              <code>{preset.naming_strategy.name}</code>. Override here if
              needed.
            </>
          ) : preset.naming_strategy?.type === "pattern" ? (
            <>
              Preset uses pattern: <code>{preset.naming_strategy.pattern}</code>
              . Override to use a specific name instead.
            </>
          ) : (
            "Leave empty for auto-generated name, or specify a custom cluster name"
          )}
        </small>
      </div>

      {preset.service_overrides &&
        Object.keys(preset.service_overrides).length > 0 && (
          <div style="margin: 1rem 0; padding: 0.75rem; background: var(--color-bg-muted); border-radius: 4px;">
            <strong style="font-size: 0.875rem;">Service Overrides:</strong>
            <ul style="margin: 0.5rem 0 0 1.5rem; font-size: 0.875rem;">
              {Object.entries(preset.service_overrides).map(
                ([service, config]) => (
                  <li key={service}>
                    {service}: <code>{config.tag}</code>
                  </li>
                ),
              )}
            </ul>
          </div>
        )}

      {/* Data Initialization Section */}
      <div style="margin: 1rem 0; border: 1px solid var(--color-border); border-radius: 4px;">
        <label style="display: flex; align-items: center; padding: 0.75rem; cursor: pointer; background: var(--color-bg-muted);">
          <input
            type="checkbox"
            checked={enableDataInit}
            onChange={(e) => setEnableDataInit(e.target.checked)}
            style="margin-right: 0.5rem;"
          />
          <span style="font-weight: 500;">
            Initialize with Data from Snapshot
          </span>
        </label>

        {enableDataInit && (
          <div style="padding: 0.75rem; border-top: 1px solid var(--color-border);">
            {loadingSnapshots ? (
              <p style="color: var(--color-text-muted);">
                Loading snapshots...
              </p>
            ) : snapshots.length === 0 ? (
              <p style="color: var(--color-text-muted);">
                No snapshots available.
              </p>
            ) : (
              <>
                <div style="margin-bottom: 0.75rem;">
                  <label style="display: flex; align-items: center; margin-bottom: 0.5rem; cursor: pointer;">
                    <input
                      type="radio"
                      name="dataInitMode"
                      checked={dataInitMode === "specific"}
                      onChange={() => setDataInitMode("specific")}
                      style="margin-right: 0.5rem;"
                    />
                    <span>Specific snapshot</span>
                  </label>
                  {dataInitMode === "specific" && (
                    <select
                      value={selectedSnapshotId}
                      onChange={(e) => setSelectedSnapshotId(e.target.value)}
                      style="width: 100%; margin-top: 0.25rem;"
                    >
                      <option value="">Select a snapshot...</option>
                      {snapshots.map((snap) => (
                        <option key={snap.id} value={snap.id}>
                          {snap.name} ({snap.branch || "no branch"}) -{" "}
                          {formatBytes(snap.total_size_bytes)}
                        </option>
                      ))}
                    </select>
                  )}
                </div>

                <div style="margin-bottom: 0.75rem;">
                  <label style="display: flex; align-items: center; margin-bottom: 0.5rem; cursor: pointer;">
                    <input
                      type="radio"
                      name="dataInitMode"
                      checked={dataInitMode === "latest"}
                      onChange={() => setDataInitMode("latest")}
                      style="margin-right: 0.5rem;"
                    />
                    <span>Latest matching snapshot</span>
                  </label>
                  {dataInitMode === "latest" && (
                    <div style="display: flex; gap: 0.5rem; margin-top: 0.25rem;">
                      <input
                        type="text"
                        placeholder="Branch filter (optional)"
                        value={latestBranch}
                        onChange={(e) => setLatestBranch(e.target.value)}
                        style="flex: 1;"
                      />
                      <input
                        type="number"
                        placeholder="Max age"
                        value={latestMaxAgeDays}
                        onChange={(e) => setLatestMaxAgeDays(e.target.value)}
                        style="width: 80px;"
                        min="1"
                      />
                      <span style="align-self: center; color: var(--color-text-muted);">
                        days
                      </span>
                    </div>
                  )}
                </div>

                {/* Service selection for specific snapshot */}
                {dataInitMode === "specific" && selectedSnapshot?.services && (
                  <div style="margin-top: 0.75rem; padding-top: 0.75rem; border-top: 1px solid var(--color-border);">
                    <label style="font-size: 0.875rem; font-weight: 500; margin-bottom: 0.5rem; display: block;">
                      Services to Restore:
                    </label>
                    <div style="max-height: 120px; overflow-y: auto; border: 1px solid var(--color-border); border-radius: 4px;">
                      {selectedSnapshot.services.map((service) => (
                        <label
                          key={service.service_name}
                          style="display: flex; align-items: center; padding: 0.35rem 0.5rem; cursor: pointer; font-size: 0.875rem;"
                        >
                          <input
                            type="checkbox"
                            checked={
                              selectedServices[service.service_name] || false
                            }
                            onChange={() =>
                              handleServiceToggle(service.service_name)
                            }
                            style="margin-right: 0.5rem;"
                          />
                          <span style="flex: 1;">{service.service_name}</span>
                          <span style="color: var(--color-text-muted);">
                            {formatBytes(service.size_bytes)}
                          </span>
                        </label>
                      ))}
                    </div>
                  </div>
                )}
              </>
            )}
          </div>
        )}
      </div>

      {error && <div className="modal-error">{error}</div>}

      <div className="modal-actions">
        <button
          className="btn-secondary"
          onClick={onClose}
          disabled={deploying}
        >
          Cancel
        </button>
        <button
          className="btn-primary"
          onClick={handleDeploy}
          disabled={deploying}
        >
          {deploying ? "Deploying..." : "Deploy"}
        </button>
      </div>
    </Modal>
  );
}
