import { useState, useEffect } from "preact/hooks";
import { route } from "preact-router";
import { Table } from "../Table";
import { apiClient } from "../../lib/api-client";

export function DeploymentProfilesList() {
  const [profiles, setProfiles] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  const loadProfiles = async () => {
    try {
      setLoading(true);
      const data = await apiClient.get("/api/config/deployment-profiles");

      if (data.status === "success") {
        // Convert the deployment_profiles object to an array
        const profilesArray = Object.entries(
          data.deployment_profiles || {},
        ).map(([name, config]) => ({
          name,
          ...config,
        }));
        setProfiles(profilesArray);
      } else {
        setError("Failed to load deployment profiles");
      }
    } catch (err) {
      setError(`Failed to load deployment profiles: ${err.message}`);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadProfiles();
  }, []);

  const handleRowClick = (profile) => {
    route(`/config/profiles/${encodeURIComponent(profile.name)}`);
  };

  const columns = [
    {
      key: "name",
      label: "Profile Name",
      render: (value) => <span style="font-weight: 500;">{value}</span>,
    },
    {
      key: "version",
      label: "Version",
      render: (value) => (
        <span style="font-family: monospace; font-size: 0.875rem;">
          {value || "1.0"}
        </span>
      ),
    },
    {
      key: "environment_type",
      label: "Environment",
      render: (value) => (
        <span style="text-transform: capitalize;">{value || "-"}</span>
      ),
    },
    {
      key: "services",
      label: "Services",
      render: (value) => {
        if (!value || !Array.isArray(value) || value.length === 0) {
          return <span style="color: var(--text-light);">-</span>;
        }

        return (
          <div style="display: flex; flex-wrap: wrap; gap: 0.25rem;">
            {value.map((service, idx) => (
              <span key={idx} className="config-pill">
                {service}
              </span>
            ))}
          </div>
        );
      },
    },
    {
      key: "resolution_strategy",
      label: "Resolution Strategy",
      render: (value) => (
        <span style="font-family: monospace; font-size: 0.875rem; color: var(--cyan);">
          {value || "branch_discovery_with_fallback"}
        </span>
      ),
    },
  ];

  if (loading) {
    return (
      <div style="padding: 2rem; text-align: center; color: var(--text-light);">
        Loading deployment profiles...
      </div>
    );
  }

  if (error) {
    return (
      <div style="padding: 1rem; background: var(--red); color: var(--base03); border-radius: 4px;">
        {error}
      </div>
    );
  }

  return (
    <div>
      <div style="margin-bottom: 1rem; padding: 1rem; background: var(--base02); border-radius: 4px;">
        <span style="color: var(--text-light); font-size: 0.875rem;">
          Total Profiles:{" "}
        </span>
        <span style="font-weight: 500;">{profiles.length}</span>
      </div>

      <Table
        columns={columns}
        data={profiles}
        onRowClick={handleRowClick}
        emptyMessage="No deployment profiles configured"
      />
    </div>
  );
}
