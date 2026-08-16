import { useEffect, useState, useCallback } from "preact/hooks";
import { route } from "preact-router";
import { Breadcrumb } from "../components/Breadcrumb";
import { InfoGrid, InfoGridRow } from "../components/InfoGrid";
import { Card } from "../components/Card";
import { Guid } from "../components/Guid";
import { PresetEditModal } from "../components/PresetEditModal";
import { PresetDeployModal } from "../components/PresetDeployModal";
import { ConfirmModal } from "../components/ConfirmModal";
import { apiClient } from "../lib/api-client";
import { formatDateTime } from "../lib/time-utils";

export function PresetDetail({ presetId }) {
  const [preset, setPreset] = useState(null);
  const [profile, setProfile] = useState(null);
  const [profiles, setProfiles] = useState([]);
  const [providers, setProviders] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [showDeployModal, setShowDeployModal] = useState(false);
  const [showEditModal, setShowEditModal] = useState(false);
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false);
  const [deleting, setDeleting] = useState(false);

  const loadPreset = useCallback(async () => {
    try {
      setLoading(true);
      const data = await apiClient.get(`/api/presets/${presetId}`);
      setPreset(data);
      setError(null);

      // Load profile details
      try {
        const profileData = await apiClient.get(
          `/api/registry/profiles/${data.profile_name}`,
        );
        setProfile(profileData);
      } catch (err) {
        console.error("Failed to load profile:", err);
      }

      // Load providers
      try {
        const providersData = await apiClient.get("/api/registry/providers");
        setProviders(providersData.providers || []);
      } catch (err) {
        console.error("Failed to load providers:", err);
      }

      // Load all profiles (for edit modal)
      try {
        const profilesData = await apiClient.get("/api/registry/profiles");
        setProfiles(profilesData.profiles || []);
      } catch (err) {
        console.error("Failed to load profiles:", err);
      }
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }, [presetId]);

  useEffect(() => {
    loadPreset();
  }, [loadPreset]);

  const handleDelete = async () => {
    try {
      setDeleting(true);
      await apiClient.delete(`/api/presets/${presetId}`);
      route("/presets");
    } catch (err) {
      setDeleting(false);
      throw err; // Re-throw to let ConfirmModal handle it
    }
  };

  if (loading) return <div className="loading">Loading preset...</div>;
  if (error) return <div className="error-banner">Error: {error}</div>;
  if (!preset) return <div className="error-banner">Preset not found</div>;

  return (
    <div className="page">
      <Breadcrumb
        path={[{ label: "Presets", href: "/presets" }, { label: preset.name }]}
      />

      <div className="page-header">
        <h2>{preset.name}</h2>
        <div style="display: flex; gap: 0.5rem;">
          <button
            className="btn-primary"
            onClick={() => setShowDeployModal(true)}
          >
            Deploy
          </button>
          <button
            className="btn-primary"
            onClick={() => setShowEditModal(true)}
          >
            <svg
              width="14"
              height="14"
              viewBox="0 0 16 16"
              fill="none"
              xmlns="http://www.w3.org/2000/svg"
              style="margin-right: 0.4rem; vertical-align: -2px;"
            >
              <path
                d="M11.013 1.427a1.75 1.75 0 012.474 0l1.086 1.086a1.75 1.75 0 010 2.474l-8.61 8.61c-.21.21-.47.364-.756.445l-3.251.93a.75.75 0 01-.927-.928l.929-3.25a1.75 1.75 0 01.445-.758l8.61-8.61zm1.414 1.06a.25.25 0 00-.354 0L10.811 3.75l1.439 1.44 1.263-1.263a.25.25 0 000-.354l-1.086-1.086zM11.189 6.25L9.75 4.81l-6.286 6.287a.25.25 0 00-.064.108l-.558 1.953 1.953-.558a.249.249 0 00.108-.064l6.286-6.286z"
                fill="currentColor"
              />
            </svg>
            Edit
          </button>
          <button
            className="btn-danger"
            onClick={() => setShowDeleteConfirm(true)}
            disabled={deleting}
          >
            {deleting ? "Deleting..." : "Delete"}
          </button>
        </div>
      </div>

      {preset.description && (
        <p style="color: var(--color-text-muted); margin-bottom: 1.5rem;">
          {preset.description}
        </p>
      )}

      <div className="detail-grid">
        <Card title="Preset Details">
          <InfoGrid>
            <InfoGridRow label="ID">
              <Guid value={preset.id} />
            </InfoGridRow>
            <InfoGridRow label="Name">{preset.name}</InfoGridRow>
            <InfoGridRow label="Profile">
              <span
                onClick={() => route(`/config/profiles/${preset.profile_name}`)}
                className="config-pill"
                style="cursor: pointer;"
              >
                {preset.profile_name}
              </span>
            </InfoGridRow>
            <InfoGridRow label="Environment">
              <span className="status-badge status-muted">
                {preset.environment}
              </span>
            </InfoGridRow>
            <InfoGridRow label="Default Branch">
              {preset.default_branch || (
                <span style="color: var(--color-text-muted);">Not set</span>
              )}
            </InfoGridRow>
            <InfoGridRow label="Default TTL">
              {preset.default_ttl_hours ? (
                `${preset.default_ttl_hours} hours`
              ) : (
                <span style="color: var(--color-text-muted);">
                  24 hours (default)
                </span>
              )}
            </InfoGridRow>
            <InfoGridRow label="Naming Strategy">
              {preset.naming_strategy ? (
                <span>
                  <span className="status-badge status-muted">
                    {preset.naming_strategy.type}
                  </span>
                  {preset.naming_strategy.type === "fixed" &&
                    preset.naming_strategy.name && (
                      <code style="margin-left: 0.5rem;">
                        {preset.naming_strategy.name}
                      </code>
                    )}
                  {preset.naming_strategy.type === "pattern" &&
                    preset.naming_strategy.pattern && (
                      <code style="margin-left: 0.5rem;">
                        {preset.naming_strategy.pattern}
                      </code>
                    )}
                </span>
              ) : (
                <span className="status-badge status-muted">generated</span>
              )}
            </InfoGridRow>
            <InfoGridRow label="Created By">{preset.created_by}</InfoGridRow>
            <InfoGridRow label="Created">
              {formatDateTime(preset.created_at)}
            </InfoGridRow>
          </InfoGrid>
        </Card>

        <Card title="Usage Statistics">
          <InfoGrid>
            <InfoGridRow label="Total Deployments">
              {preset.use_count || 0}
            </InfoGridRow>
            <InfoGridRow label="Last Used">
              {preset.last_used_at ? (
                formatDateTime(preset.last_used_at)
              ) : (
                <span style="color: var(--color-text-muted);">Never</span>
              )}
            </InfoGridRow>
          </InfoGrid>
        </Card>

        {preset.service_overrides &&
          Object.keys(preset.service_overrides).length > 0 && (
            <Card title="Service Overrides">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Service</th>
                    <th>Tag</th>
                  </tr>
                </thead>
                <tbody>
                  {Object.entries(preset.service_overrides).map(
                    ([service, config]) => (
                      <tr key={service}>
                        <td style="font-family: monospace;">{service}</td>
                        <td>
                          <code>{config.tag}</code>
                        </td>
                      </tr>
                    ),
                  )}
                </tbody>
              </table>
            </Card>
          )}

        {profile && (
          <Card title="Profile Services">
            <p style="color: var(--color-text-muted); margin-bottom: 1rem; font-size: 0.875rem;">
              Services defined in the {profile.name} deployment profile:
            </p>
            <table className="data-table">
              <thead>
                <tr>
                  <th>Service</th>
                  <th>Repository</th>
                  <th>Port</th>
                </tr>
              </thead>
              <tbody>
                {profile.services.map((svc) => (
                  <tr key={svc.name}>
                    <td style="font-weight: 500;">{svc.name}</td>
                    <td style="font-family: monospace; font-size: 0.875rem;">
                      {svc.repository}
                    </td>
                    <td>{svc.port}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </Card>
        )}
      </div>

      {/* Deploy Modal */}
      {showDeployModal && (
        <PresetDeployModal
          preset={preset}
          profile={profile}
          providers={providers}
          onClose={() => setShowDeployModal(false)}
        />
      )}

      {/* Edit Modal */}
      {showEditModal && (
        <PresetEditModal
          preset={preset}
          profiles={profiles}
          onClose={() => setShowEditModal(false)}
          onSaved={() => {
            setShowEditModal(false);
            loadPreset();
          }}
        />
      )}

      {/* Delete Confirmation Modal */}
      {showDeleteConfirm && (
        <ConfirmModal
          title="Delete Preset"
          message={`Are you sure you want to delete preset "${preset.name}"?`}
          confirmLabel="Delete"
          onConfirm={handleDelete}
          onCancel={() => setShowDeleteConfirm(false)}
          loading={deleting}
        />
      )}
    </div>
  );
}
