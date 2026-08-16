import { useState, useEffect } from "preact/hooks";
import { route } from "preact-router";
import { Card } from "../components/Card";
import { Breadcrumb } from "../components/Breadcrumb";
import { InfoGrid, InfoGridRow } from "../components/InfoGrid";
import { StatusBadge } from "../components/StatusBadge";
import { apiClient } from "../lib/api-client";

export function RuleDetail({ ruleName }) {
  const [rule, setRule] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  const decodedRuleName = decodeURIComponent(ruleName);

  const loadRule = async () => {
    try {
      setLoading(true);

      // Fetch all rules and find the matching one
      const data = await apiClient.get("/api/config/rules");

      if (data.status === "loaded") {
        const matchedRule = data.rules.find((r) => r.name === decodedRuleName);

        if (matchedRule) {
          setRule(matchedRule);
        } else {
          setError(`Rule "${decodedRuleName}" not found`);
        }
      } else {
        setError("Failed to load deployment rules");
      }
    } catch (err) {
      setError(`Failed to load rule: ${err.message}`);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadRule();
  }, [decodedRuleName]);

  const formatPatterns = (rule) => {
    const patterns = [];

    if (rule.branch_patterns && rule.branch_patterns.length > 0) {
      patterns.push(...rule.branch_patterns.map((p) => `branch: ${p}`));
    }

    if (rule.tag_pattern) {
      patterns.push(`tag: ${rule.tag_pattern}`);
    }

    if (rule.repo_patterns && rule.repo_patterns.length > 0) {
      patterns.push(...rule.repo_patterns.map((p) => `repo: ${p}`));
    }

    return patterns;
  };

  if (loading) {
    return (
      <div className="page">
        <div style="padding: 2rem; text-align: center; color: var(--text-light);">
          Loading rule details...
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
            { label: "Deployment Rule" },
            { label: decodedRuleName },
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
          { label: "Deployment Rule" },
          { label: decodedRuleName },
        ]}
      />

      <div style="display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 1.5rem;">
        <div>
          <h2 style="margin-bottom: 0.25rem;">{rule.name}</h2>
          {rule.description && (
            <p style="color: var(--text-light); margin: 0; font-size: 0.95rem;">
              {rule.description}
            </p>
          )}
        </div>
        <StatusBadge
          status={rule.enabled ? "active" : "inactive"}
          label={rule.enabled ? "Enabled" : "Disabled"}
        />
      </div>

      <Card title="Rule Configuration">
        <InfoGrid>
          <InfoGridRow label="Name">
            <span style="font-weight: 500;">{rule.name}</span>
          </InfoGridRow>

          <InfoGridRow label="Action">
            <span style="font-family: monospace; color: var(--cyan);">
              {rule.action}
            </span>
          </InfoGridRow>

          <InfoGridRow label="Repository Patterns">
            <div style="display: flex; flex-direction: column; gap: 0.25rem;">
              {rule.repo_patterns && rule.repo_patterns.length > 0 ? (
                rule.repo_patterns.map((pattern, idx) => (
                  <span
                    key={idx}
                    style="font-family: monospace; background: var(--base02); padding: 0.25rem 0.5rem; border-radius: 4px; display: inline-block; width: fit-content;"
                  >
                    {pattern}
                  </span>
                ))
              ) : (
                <span style="color: var(--text-light);">Any repository</span>
              )}
            </div>
          </InfoGridRow>

          <InfoGridRow label="Branch Patterns">
            <div style="display: flex; flex-direction: column; gap: 0.25rem;">
              {rule.branch_patterns && rule.branch_patterns.length > 0 ? (
                rule.branch_patterns.map((pattern, idx) => (
                  <span
                    key={idx}
                    style="font-family: monospace; background: var(--base02); padding: 0.25rem 0.5rem; border-radius: 4px; display: inline-block; width: fit-content;"
                  >
                    {pattern}
                  </span>
                ))
              ) : rule.tag_pattern ? (
                <span style="font-family: monospace; background: var(--base02); padding: 0.25rem 0.5rem; border-radius: 4px; display: inline-block; width: fit-content;">
                  tag:{rule.tag_pattern}
                </span>
              ) : (
                <span style="color: var(--text-light);">Any branch</span>
              )}
            </div>
          </InfoGridRow>
        </InfoGrid>
      </Card>

      {rule.config && Object.keys(rule.config).length > 0 && (
        <Card title="Action Configuration">
          <InfoGrid>
            {Object.entries(rule.config).map(([key, value]) => (
              <InfoGridRow key={key} label={key}>
                <span style="font-family: monospace;">
                  {typeof value === "object"
                    ? JSON.stringify(value, null, 2)
                    : String(value)}
                </span>
              </InfoGridRow>
            ))}
          </InfoGrid>
        </Card>
      )}

      <Card title="Full Configuration (YAML)">
        <pre style="background: var(--base02); padding: 1rem; border-radius: 4px; overflow-x: auto; font-family: monospace; font-size: 0.875rem; line-height: 1.5;">
          {`name: ${rule.name}
description: ${rule.description || ""}
enabled: ${rule.enabled}
action: ${rule.action}
${
  rule.branch_patterns && rule.branch_patterns.length > 0
    ? `branch_patterns:
${rule.branch_patterns.map((p) => `  - ${p}`).join("\n")}`
    : ""
}
${rule.tag_pattern ? `tag_pattern: ${rule.tag_pattern}` : ""}
${
  rule.repo_patterns && rule.repo_patterns.length > 0
    ? `repo_patterns:
${rule.repo_patterns.map((p) => `  - ${p}`).join("\n")}`
    : ""
}
${
  rule.config && Object.keys(rule.config).length > 0
    ? `config:
${Object.entries(rule.config)
  .map(([k, v]) => `  ${k}: ${typeof v === "object" ? JSON.stringify(v) : v}`)
  .join("\n")}`
    : ""
}`}
        </pre>
      </Card>
    </div>
  );
}
