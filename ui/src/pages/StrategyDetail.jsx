import { useState, useEffect } from "preact/hooks";
import { Card } from "../components/Card";
import { Breadcrumb } from "../components/Breadcrumb";
import { InfoGrid, InfoGridRow } from "../components/InfoGrid";
import { apiClient } from "../lib/api-client";

export function StrategyDetail({ strategyName }) {
  const [strategy, setStrategy] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  const decodedStrategyName = decodeURIComponent(strategyName);

  const loadStrategy = async () => {
    try {
      setLoading(true);
      const data = await apiClient.get(
        `/api/config/resolution-strategies/${encodeURIComponent(decodedStrategyName)}`,
      );

      if (data.status === "success") {
        setStrategy(data.strategy);
      } else {
        setError("Failed to load resolution strategy");
      }
    } catch (err) {
      setError(`Failed to load strategy: ${err.message}`);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadStrategy();
  }, [decodedStrategyName]);

  if (loading) {
    return (
      <div className="page">
        <div style="padding: 2rem; text-align: center; color: var(--text-light);">
          Loading strategy details...
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
            { label: "Resolution Strategy" },
            { label: decodedStrategyName },
          ]}
        />
        <div style="padding: 1rem; background: var(--red); color: var(--base03); border-radius: 4px; margin-top: 1rem;">
          {error}
        </div>
      </div>
    );
  }

  if (!strategy) {
    return (
      <div className="page">
        <Breadcrumb
          path={[
            { label: "Config", href: "/config" },
            { label: "Resolution Strategy" },
            { label: decodedStrategyName },
          ]}
        />
        <div style="padding: 2rem; text-align: center; color: var(--text-light);">
          Strategy not found
        </div>
      </div>
    );
  }

  const breadcrumb = [
    { label: "Config", href: "/config" },
    { label: "Resolution Strategy" },
    { label: strategy.name },
  ];

  return (
    <div className="page">
      <Breadcrumb path={breadcrumb} />

      <div style="margin-bottom: 1.5rem;">
        <h2 style="margin-bottom: 0.25rem;">{strategy.name}</h2>
        {strategy.description && (
          <p style="color: var(--text-light); margin: 0; font-size: 0.95rem;">
            {strategy.description}
          </p>
        )}
      </div>

      <Card title="Strategy Configuration">
        <InfoGrid>
          <InfoGridRow label="Fallback Branches">
            {strategy.fallback_branches &&
            strategy.fallback_branches.length > 0 ? (
              <div style="display: flex; flex-direction: column; gap: 0.25rem;">
                {strategy.fallback_branches.map((branch, idx) => (
                  <span
                    key={idx}
                    style="font-family: monospace; background: var(--base02); padding: 0.25rem 0.5rem; border-radius: 4px; display: inline-block; width: fit-content;"
                  >
                    {branch}
                  </span>
                ))}
              </div>
            ) : (
              <span style="color: var(--text-light);">
                No fallback branches configured
              </span>
            )}
          </InfoGridRow>

          <InfoGridRow label="Require Triggering Repo">
            <span
              style={`color: ${strategy.require_triggering_repo ? "var(--green)" : "var(--base01)"}`}
            >
              {strategy.require_triggering_repo ? "Yes" : "No"}
            </span>
          </InfoGridRow>

          <InfoGridRow label="Allow External Fallback">
            <span
              style={`color: ${strategy.allow_external_fallback ? "var(--green)" : "var(--base01)"}`}
            >
              {strategy.allow_external_fallback ? "Yes" : "No"}
            </span>
          </InfoGridRow>
        </InfoGrid>
      </Card>

      {strategy.explanation && (
        <Card title="How This Strategy Works" style="margin-top: 1.5rem;">
          <div style="color: var(--base0); line-height: 1.6;">
            <p style="margin: 0;">{strategy.explanation}</p>
          </div>
        </Card>
      )}
    </div>
  );
}
