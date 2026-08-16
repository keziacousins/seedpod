import { useState, useEffect } from "preact/hooks";
import { route } from "preact-router";
import { Table } from "../Table";
import { apiClient } from "../../lib/api-client";

export function ResolutionStrategiesList() {
  const [strategies, setStrategies] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  const loadStrategies = async () => {
    try {
      setLoading(true);
      const data = await apiClient.get("/api/config/resolution-strategies");

      if (data.status === "success") {
        // Convert strategies object to array
        const strategiesArray = Object.entries(data.strategies || {}).map(
          ([key, strategy]) => ({
            key,
            ...strategy,
          }),
        );
        setStrategies(strategiesArray);
      } else {
        setError("Failed to load resolution strategies");
      }
    } catch (err) {
      setError(`Failed to load resolution strategies: ${err.message}`);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadStrategies();
  }, []);

  const handleRowClick = (strategy) => {
    route(`/config/strategies/${strategy.name}`);
  };

  const columns = [
    {
      key: "name",
      label: "Strategy Name",
      render: (value, strategy) => (
        <div>
          <div style="font-weight: 500;">{value}</div>
          {strategy.description && (
            <div style="font-size: 0.875rem; color: var(--text-light); margin-top: 0.25rem;">
              {strategy.description}
            </div>
          )}
        </div>
      ),
    },
    {
      key: "fallback_branches",
      label: "Fallback Branches",
      render: (value) => {
        if (!value || !Array.isArray(value) || value.length === 0) {
          return <span style="color: var(--text-light);">None</span>;
        }

        return (
          <div style="display: flex; flex-wrap: wrap; gap: 0.25rem;">
            {value.map((branch) => (
              <span key={branch} className="config-pill">
                {branch}
              </span>
            ))}
          </div>
        );
      },
    },
    {
      key: "require_triggering_repo",
      label: "Require Triggering Repo",
      render: (value) => (
        <span style={`color: ${value ? "var(--green)" : "var(--text-light)"}`}>
          {value ? "Yes" : "No"}
        </span>
      ),
    },
    {
      key: "allow_external_fallback",
      label: "Allow External Fallback",
      render: (value) => (
        <span style={`color: ${value ? "var(--green)" : "var(--text-light)"}`}>
          {value ? "Yes" : "No"}
        </span>
      ),
    },
  ];

  if (loading) {
    return (
      <div style="padding: 2rem; text-align: center; color: var(--text-light);">
        Loading resolution strategies...
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
    <Table
      columns={columns}
      data={strategies}
      onRowClick={handleRowClick}
      keyField="name"
      emptyMessage="No resolution strategies configured"
    />
  );
}
