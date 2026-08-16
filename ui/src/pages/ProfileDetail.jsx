import { useState, useEffect } from "preact/hooks";
import { route } from "preact-router";
import { Card } from "../components/Card";
import { Breadcrumb } from "../components/Breadcrumb";
import { InfoGrid, InfoGridRow } from "../components/InfoGrid";
import { apiClient } from "../lib/api-client";

export function ProfileDetail({ profileName }) {
  const [profile, setProfile] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  const decodedProfileName = decodeURIComponent(profileName);

  const loadProfile = async () => {
    try {
      setLoading(true);
      const data = await apiClient.get(
        `/api/config/deployment-profiles/${encodeURIComponent(decodedProfileName)}`,
      );

      if (data.status === "success") {
        setProfile(data.config);
      } else {
        setError("Failed to load deployment profile");
      }
    } catch (err) {
      if (err.message.includes("404")) {
        setError(`Profile "${decodedProfileName}" not found`);
      } else {
        setError(`Failed to load profile: ${err.message}`);
      }
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadProfile();
  }, [decodedProfileName]);

  const formatYAML = (obj, indent = 0) => {
    const spaces = "  ".repeat(indent);
    let result = "";

    for (const [key, value] of Object.entries(obj)) {
      if (value === null || value === undefined) {
        result += `${spaces}${key}: null\n`;
      } else if (Array.isArray(value)) {
        result += `${spaces}${key}:\n`;
        value.forEach((item) => {
          if (typeof item === "object") {
            result += `${spaces}  -\n`;
            result += formatYAML(item, indent + 2)
              .split("\n")
              .map((line) => (line ? `  ${line}` : ""))
              .join("\n");
          } else {
            result += `${spaces}  - ${item}\n`;
          }
        });
      } else if (typeof value === "object") {
        result += `${spaces}${key}:\n`;
        result += formatYAML(value, indent + 1);
      } else {
        result += `${spaces}${key}: ${value}\n`;
      }
    }

    return result;
  };

  if (loading) {
    return (
      <div className="page">
        <div style="padding: 2rem; text-align: center; color: var(--text-light);">
          Loading profile details...
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="page">
        <Breadcrumb
          path={[
            { label: "Config", href: "/config" },
            { label: "Deployment Profile" },
            { label: decodedProfileName },
          ]}
        />
        <div style="padding: 1rem; background: var(--red); color: var(--base03); border-radius: 4px;">
          {error}
        </div>
      </div>
    );
  }

  return (
    <div className="page">
      <Breadcrumb
        path={[
          { label: "Config", href: "/config" },
          { label: "Deployment Profile" },
          { label: decodedProfileName },
        ]}
      />

      <h2>{decodedProfileName}</h2>

      <Card title="Profile Metadata">
        <InfoGrid>
          <InfoGridRow label="Profile Name">
            <span style="font-weight: 500;">{decodedProfileName}</span>
          </InfoGridRow>

          <InfoGridRow label="Version">
            <span style="font-family: monospace;">
              {profile.version || "1.0"}
            </span>
          </InfoGridRow>

          <InfoGridRow label="Environment Type">
            <span style="text-transform: capitalize;">
              {profile.environment_type || "-"}
            </span>
          </InfoGridRow>

          <InfoGridRow label="Resolution Strategy">
            <span
              onClick={() =>
                route(
                  `/config/strategies/${profile.resolution_strategy || "branch_discovery_with_fallback"}`,
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
              {profile.resolution_strategy || "branch_discovery_with_fallback"}
            </span>
          </InfoGridRow>

          {profile.services && Array.isArray(profile.services) && (
            <InfoGridRow label="Services">
              <div style="display: flex; flex-wrap: wrap; gap: 0.25rem;">
                {profile.services.map((service) => (
                  <span key={service.name} className="config-pill">
                    {service.name}
                  </span>
                ))}
              </div>
            </InfoGridRow>
          )}

          {profile.cluster && (
            <>
              <InfoGridRow label="Cluster CPU">
                <span style="font-family: monospace;">
                  {profile.cluster.cpu || "-"}
                </span>
              </InfoGridRow>

              <InfoGridRow label="Cluster Memory">
                <span style="font-family: monospace;">
                  {profile.cluster.memory || "-"}
                </span>
              </InfoGridRow>

              {profile.cluster.region && (
                <InfoGridRow label="Region">
                  <span>{profile.cluster.region}</span>
                </InfoGridRow>
              )}
            </>
          )}
        </InfoGrid>
      </Card>

      {profile.services &&
        Array.isArray(profile.services) &&
        profile.services.length > 0 && (
          <Card title="Services Configuration">
            {profile.services.map((service) => (
              <div
                key={service.name}
                style="margin-bottom: 1rem; padding: 1rem; background: var(--base02); border-radius: 4px;"
              >
                <h4 style="margin-bottom: 0.5rem;">{service.name}</h4>
                <InfoGrid>
                  <InfoGridRow label="Repository">
                    <span style="font-family: monospace; font-size: 0.875rem;">
                      {service.repository}
                    </span>
                  </InfoGridRow>

                  {service.port && (
                    <InfoGridRow label="Port">
                      <span>{service.port}</span>
                    </InfoGridRow>
                  )}

                  {service.replicas !== undefined && (
                    <InfoGridRow label="Replicas">
                      <span>{service.replicas}</span>
                    </InfoGridRow>
                  )}

                  {service.image && (
                    <InfoGridRow label="Image Override">
                      <span style="font-family: monospace; font-size: 0.875rem;">
                        {service.image}
                      </span>
                    </InfoGridRow>
                  )}
                </InfoGrid>
              </div>
            ))}
          </Card>
        )}

      <Card title="Full Configuration (YAML)">
        <pre style="background: var(--base02); padding: 1rem; border-radius: 4px; overflow-x: auto; font-family: monospace; font-size: 0.875rem; line-height: 1.5; max-height: 600px;">
          {formatYAML(profile)}
        </pre>
      </Card>
    </div>
  );
}
