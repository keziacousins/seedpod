import { useState, useEffect } from "preact/hooks";
import { route } from "preact-router";
import { Card } from "../Card";
import { apiClient } from "../../lib/api-client";

export function ConfigOverview() {
  const [overview, setOverview] = useState(null);
  const [loading, setLoading] = useState(true);
  const [reloadLoading, setReloadLoading] = useState(false);
  const [error, setError] = useState(null);
  const [success, setSuccess] = useState(null);
  const [rulesResult, setRulesResult] = useState(null);
  const [profilesResult, setProfilesResult] = useState(null);

  const loadOverview = async () => {
    try {
      setLoading(true);
      const data = await apiClient.get("/api/config/overview");
      setOverview(data);
      setError(null);
    } catch (err) {
      setError(`Failed to load config overview: ${err.message}`);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadOverview();
  }, []);

  const handleReloadRules = async () => {
    try {
      setReloadLoading(true);
      setError(null);
      setSuccess(null);
      const data = await apiClient.post("/api/rules/reload");
      setRulesResult(data);
      setSuccess("Deployment rules reloaded successfully!");
      // Refresh overview
      await loadOverview();
    } catch (err) {
      setError(`Failed to reload deployment rules: ${err.message}`);
      setRulesResult(null);
    } finally {
      setReloadLoading(false);
    }
  };

  const handleReloadProfiles = async () => {
    try {
      setReloadLoading(true);
      setError(null);
      setSuccess(null);
      const data = await apiClient.post("/api/deployment-profiles/reload");
      setProfilesResult(data);
      setSuccess("Deployment profiles reloaded successfully!");
      // Refresh overview
      await loadOverview();
    } catch (err) {
      setError(`Failed to reload deployment profiles: ${err.message}`);
      setProfilesResult(null);
    } finally {
      setReloadLoading(false);
    }
  };

  const handleReloadAll = async () => {
    try {
      setReloadLoading(true);
      setError(null);
      setSuccess(null);
      setRulesResult(null);
      setProfilesResult(null);

      // Reload both in parallel
      const [rulesData, profilesData] = await Promise.all([
        apiClient.post("/api/rules/reload"),
        apiClient.post("/api/deployment-profiles/reload"),
      ]);

      setRulesResult(rulesData);
      setProfilesResult(profilesData);
      setSuccess("All configuration reloaded successfully!");
      // Refresh overview
      await loadOverview();
    } catch (err) {
      setError(`Failed to reload configuration: ${err.message}`);
    } finally {
      setReloadLoading(false);
    }
  };

  if (loading) {
    return (
      <div style="padding: 2rem; text-align: center; color: var(--text-light);">
        Loading configuration overview...
      </div>
    );
  }

  const defaultStrategy = overview?.resolution_strategies?.default;

  return (
    <div>
      {error && (
        <div style="padding: 1rem; background: var(--red); color: var(--base03); border-radius: 4px; margin-bottom: 1rem;">
          {error}
        </div>
      )}

      {success && (
        <div style="padding: 1rem; background: var(--green); color: var(--base03); border-radius: 4px; margin-bottom: 1rem;">
          {success}
        </div>
      )}

      {/* Stats Cards */}
      <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 1rem; margin-bottom: 1.5rem;">
        <Card title="Deployment Rules">
          <div className="info-row">
            <span className="label">Version:</span>
            <span>{overview?.rules?.version || "unknown"}</span>
          </div>
          <div className="info-row">
            <span className="label">Total Rules:</span>
            <span>{overview?.rules?.total || 0}</span>
          </div>
          <div className="info-row">
            <span className="label">Enabled:</span>
            <span style="color: var(--green);">
              {overview?.rules?.enabled || 0}
            </span>
          </div>
          <div className="info-row">
            <span className="label">Disabled:</span>
            <span style="color: var(--base01);">
              {overview?.rules?.disabled || 0}
            </span>
          </div>
          <div className="info-row">
            <span className="label">Global Ephemeral:</span>
            <span
              style={`color: ${overview?.rules?.global_ephemeral_enabled ? "var(--green)" : "var(--red)"}`}
            >
              {overview?.rules?.global_ephemeral_enabled
                ? "Enabled"
                : "Disabled"}
            </span>
          </div>
          {overview?.rules?.enabled_rules &&
            overview.rules.enabled_rules.length > 0 && (
              <div style="margin-top: 0.5rem;">
                <div style="display: flex; flex-wrap: wrap; gap: 0.5rem;">
                  {overview.rules.enabled_rules.map((rule) => (
                    <span
                      key={rule}
                      onClick={() => route(`/config/rules/${rule}`)}
                      style="background: var(--base02); padding: 0.25rem 0.5rem; border-radius: 4px; font-size: 0.875rem; cursor: pointer; transition: background 0.2s;"
                      onMouseEnter={(e) =>
                        (e.target.style.background = "var(--base01)")
                      }
                      onMouseLeave={(e) =>
                        (e.target.style.background = "var(--base02)")
                      }
                    >
                      {rule}
                    </span>
                  ))}
                  {overview.rules.disabled_rules &&
                    overview.rules.disabled_rules.length > 0 &&
                    overview.rules.disabled_rules.map((rule) => (
                      <span
                        key={rule}
                        onClick={() => route(`/config/rules/${rule}`)}
                        style="background: var(--base02); padding: 0.25rem 0.5rem; border-radius: 4px; font-size: 0.875rem; cursor: pointer; transition: background 0.2s; opacity: 0.5;"
                        onMouseEnter={(e) => {
                          e.target.style.background = "var(--base01)";
                          e.target.style.opacity = "0.7";
                        }}
                        onMouseLeave={(e) => {
                          e.target.style.background = "var(--base02)";
                          e.target.style.opacity = "0.5";
                        }}
                      >
                        {rule}
                      </span>
                    ))}
                </div>
              </div>
            )}
        </Card>

        <Card title="Deployment Profiles">
          <div className="info-row">
            <span className="label">Total Profiles:</span>
            <span>{overview?.deployment_profiles?.total || 0}</span>
          </div>
          {overview?.deployment_profiles?.profiles &&
            overview.deployment_profiles.profiles.length > 0 && (
              <div style="margin-top: 0.5rem;">
                <div style="display: flex; flex-wrap: wrap; gap: 0.5rem;">
                  {overview.deployment_profiles.profiles.map((profile) => (
                    <span
                      key={profile}
                      onClick={() => route(`/config/profiles/${profile}`)}
                      style="background: var(--base02); padding: 0.25rem 0.5rem; border-radius: 4px; font-size: 0.875rem; cursor: pointer; transition: background 0.2s;"
                      onMouseEnter={(e) =>
                        (e.target.style.background = "var(--base01)")
                      }
                      onMouseLeave={(e) =>
                        (e.target.style.background = "var(--base02)")
                      }
                    >
                      {profile}
                    </span>
                  ))}
                </div>
              </div>
            )}
        </Card>

        <Card title="Resolution Strategies">
          <div className="info-row">
            <span className="label">Total Strategies:</span>
            <span>{overview?.resolution_strategies?.total || 0}</span>
          </div>
          {overview?.resolution_strategies?.strategies &&
            overview.resolution_strategies.strategies.length > 0 && (
              <div style="margin-top: 0.5rem;">
                <div style="display: flex; flex-wrap: wrap; gap: 0.5rem;">
                  {overview.resolution_strategies.strategies.map((strategy) => {
                    const isDefault = strategy === defaultStrategy;
                    return (
                      <span
                        key={strategy}
                        onClick={() => route(`/config/strategies/${strategy}`)}
                        style={`background: ${isDefault ? "var(--green)" : "var(--base02)"}; color: ${isDefault ? "var(--base03)" : "var(--text)"}; padding: 0.25rem 0.5rem; border-radius: 4px; font-size: 0.875rem; cursor: pointer; transition: all 0.2s; font-weight: ${isDefault ? "500" : "400"};`}
                        onMouseEnter={(e) => {
                          if (!isDefault) {
                            e.target.style.background = "var(--base01)";
                          }
                        }}
                        onMouseLeave={(e) => {
                          if (!isDefault) {
                            e.target.style.background = "var(--base02)";
                          }
                        }}
                      >
                        {strategy}
                        {isDefault && " ★"}
                      </span>
                    );
                  })}
                </div>
              </div>
            )}
        </Card>
      </div>

      {/* Reload Actions */}
      <Card title="Reload Configuration">
        <p style="margin-bottom: 1rem; color: var(--base0);">
          Reload configuration files from disk without restarting the server.
          This is useful when you've updated deployment rules or profiles.
        </p>

        <div style="display: flex; gap: 1rem; flex-wrap: wrap;">
          <button
            onClick={handleReloadRules}
            className="btn-primary"
            disabled={reloadLoading}
          >
            {reloadLoading ? "Reloading..." : "Reload Deployment Rules"}
          </button>

          <button
            onClick={handleReloadProfiles}
            className="btn-primary"
            disabled={reloadLoading}
          >
            {reloadLoading ? "Reloading..." : "Reload Deployment Profiles"}
          </button>

          <button
            onClick={handleReloadAll}
            className="btn-secondary"
            disabled={reloadLoading}
          >
            {reloadLoading ? "Reloading..." : "Reload All Configuration"}
          </button>
        </div>
      </Card>

      {/* Reload Results */}
      {rulesResult && (
        <Card title="Deployment Rules Reload Result">
          <div className="info-row">
            <span className="label">Status:</span>
            <span style={{ color: "var(--green)" }}>{rulesResult.status}</span>
          </div>
          {rulesResult.summary && (
            <>
              <div className="info-row">
                <span className="label">Total Rules:</span>
                <span>{rulesResult.summary.total_rules}</span>
              </div>
              <div className="info-row">
                <span className="label">Enabled:</span>
                <span>
                  {Array.isArray(rulesResult.summary.enabled_rules)
                    ? rulesResult.summary.enabled_rules.length
                    : 0}
                </span>
              </div>
              <div className="info-row">
                <span className="label">Disabled:</span>
                <span>
                  {Array.isArray(rulesResult.summary.disabled_rules)
                    ? rulesResult.summary.disabled_rules.length
                    : 0}
                </span>
              </div>
            </>
          )}
        </Card>
      )}

      {profilesResult && (
        <Card title="Deployment Profiles Reload Result">
          <div className="info-row">
            <span className="label">Status:</span>
            <span style={{ color: "var(--green)" }}>
              {profilesResult.status}
            </span>
          </div>
          <div className="info-row">
            <span className="label">Profiles Loaded:</span>
            <span>{profilesResult.deployment_profiles_count}</span>
          </div>
        </Card>
      )}
    </div>
  );
}
