import { useState, useEffect } from "preact/hooks";
import { route } from "preact-router";
import { Table } from "../Table";
import { StatusBadge } from "../StatusBadge";
import { apiClient } from "../../lib/api-client";

export function DeploymentRulesList() {
  const [rules, setRules] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [metadata, setMetadata] = useState(null);

  const loadRules = async () => {
    try {
      setLoading(true);
      const data = await apiClient.get("/api/config/rules");

      if (data.status === "loaded") {
        setRules(data.rules || []);
        setMetadata({
          version: data.version,
          global_ephemeral_enabled: data.global_ephemeral_enabled,
          default_ttl_hours: data.default_ttl_hours,
        });
      } else {
        setError(data.error || "Rules not loaded");
      }
    } catch (err) {
      setError(`Failed to load deployment rules: ${err.message}`);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadRules();
  }, []);

  const formatPattern = (rule) => {
    // Show repo patterns first, then branch/tag patterns
    const patterns = [];

    if (rule.repo_patterns && rule.repo_patterns.length > 0) {
      patterns.push(...rule.repo_patterns.map((p) => `repo:${p}`));
    }

    if (rule.branch_patterns && rule.branch_patterns.length > 0) {
      patterns.push(...rule.branch_patterns.map((p) => `branch:${p}`));
    }

    if (rule.tag_pattern) {
      patterns.push(`tag:${rule.tag_pattern}`);
    }

    return patterns.length > 0 ? patterns.join(", ") : "-";
  };

  const formatConfigPills = (config) => {
    if (!config) return <span style="color: var(--text-light);">-</span>;

    const pills = [];

    // Common config fields to display
    if (config.ttl_hours !== undefined) {
      pills.push({ key: "ttl", value: `${config.ttl_hours}h` });
    }
    if (config.cluster_size) {
      pills.push({ key: "size", value: config.cluster_size });
    }
    if (config.environment) {
      pills.push({ key: "env", value: config.environment });
    }
    if (config.deployment_profile) {
      pills.push({ key: "profile", value: config.deployment_profile });
    }
    if (config.require_manual_approval !== undefined) {
      pills.push({
        key: "manual",
        value: config.require_manual_approval ? "yes" : "no",
      });
    }

    if (pills.length === 0)
      return <span style="color: var(--text-light);">-</span>;

    return (
      <div style="display: flex; flex-wrap: wrap; gap: 0.25rem;">
        {pills.map((pill, idx) => (
          <span key={idx} className="config-pill">
            {pill.key}: {pill.value}
          </span>
        ))}
      </div>
    );
  };

  const handleRowClick = (rule) => {
    // Navigate to rule detail page
    // Use rule name as identifier (URL-encoded)
    const ruleName = encodeURIComponent(rule.name);
    route(`/config/rules/${ruleName}`);
  };

  const columns = [
    {
      key: "name",
      label: "Name",
      render: (value, rule) => (
        <div>
          <div style="font-weight: 500;">{value}</div>
          {rule.description && (
            <div style="font-size: 0.875rem; color: var(--text-light); margin-top: 0.25rem;">
              {rule.description}
            </div>
          )}
        </div>
      ),
    },
    {
      key: "repo_patterns",
      label: "Repository",
      render: (value) => {
        if (!value || value.length === 0) {
          return (
            <span style="color: var(--text-light); font-size: 0.875rem;">
              Any
            </span>
          );
        }
        return (
          <div style="display: flex; flex-wrap: wrap; gap: 0.25rem;">
            {value.map((pattern, idx) => (
              <span key={idx} className="config-pill">
                {pattern}
              </span>
            ))}
          </div>
        );
      },
    },
    {
      key: "branch_patterns",
      label: "Branch",
      render: (value, rule) => {
        if (value && value.length > 0) {
          return (
            <div style="display: flex; flex-wrap: wrap; gap: 0.25rem;">
              {value.map((pattern, idx) => (
                <span key={idx} className="config-pill">
                  {pattern}
                </span>
              ))}
            </div>
          );
        }
        if (rule.tag_pattern) {
          return <span className="config-pill">tag:{rule.tag_pattern}</span>;
        }
        return (
          <span style="color: var(--text-light); font-size: 0.875rem;">
            Any
          </span>
        );
      },
    },
    {
      key: "action",
      label: "Action",
      render: (value) => (
        <span style="font-family: monospace; font-size: 0.875rem; color: var(--cyan);">
          {value}
        </span>
      ),
    },
    {
      key: "enabled",
      label: "Enabled",
      render: (value) => (
        <StatusBadge
          status={value ? "active" : "inactive"}
          label={value ? "Enabled" : "Disabled"}
        />
      ),
    },
    {
      key: "config",
      label: "Config",
      render: (value) => formatConfigPills(value),
    },
  ];

  if (loading) {
    return (
      <div style="padding: 2rem; text-align: center; color: var(--text-light);">
        Loading deployment rules...
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
      {metadata && (
        <div style="margin-bottom: 1rem; padding: 1rem; background: var(--base02); border-radius: 4px;">
          <div style="display: flex; gap: 2rem; flex-wrap: wrap;">
            <div>
              <span style="color: var(--text-light); font-size: 0.875rem;">
                Version:{" "}
              </span>
              <span style="font-weight: 500;">{metadata.version}</span>
            </div>
            <div>
              <span style="color: var(--text-light); font-size: 0.875rem;">
                Global Ephemeral:{" "}
              </span>
              <span
                style={`font-weight: 500; color: ${metadata.global_ephemeral_enabled ? "var(--green)" : "var(--red)"}`}
              >
                {metadata.global_ephemeral_enabled ? "Enabled" : "Disabled"}
              </span>
            </div>
            <div>
              <span style="color: var(--text-light); font-size: 0.875rem;">
                Default TTL:{" "}
              </span>
              <span style="font-weight: 500;">
                {metadata.default_ttl_hours}h
              </span>
            </div>
            <div>
              <span style="color: var(--text-light); font-size: 0.875rem;">
                Total Rules:{" "}
              </span>
              <span style="font-weight: 500;">{rules.length}</span>
            </div>
          </div>
        </div>
      )}

      <Table
        columns={columns}
        data={rules}
        onRowClick={handleRowClick}
        emptyMessage="No deployment rules configured"
      />
    </div>
  );
}
