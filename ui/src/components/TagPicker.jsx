import { useState, useEffect, useCallback, useRef } from "preact/hooks";
import { apiClient } from "../lib/api-client";

/**
 * TagPicker - Component for selecting container image tags per service
 *
 * Props:
 * - services: Array of { name, repository, external } objects from the deployment profile
 * - value: Object of { serviceName: { tag: "tag-value" } }
 * - onChange: Callback when overrides change
 * - disabled: Whether the picker is disabled
 */
export function TagPicker({ services = [], value = {}, onChange, disabled }) {
  const [expandedService, setExpandedService] = useState(null);
  const [tagCache, setTagCache] = useState({}); // { repository: tags[] }
  const [loadingTags, setLoadingTags] = useState({});
  const [tagErrors, setTagErrors] = useState({});

  // Load tags for a repository
  const loadTags = useCallback(async (repository) => {
    if (tagCache[repository]) return;
    if (loadingTags[repository]) return;

    setLoadingTags((prev) => ({ ...prev, [repository]: true }));
    setTagErrors((prev) => ({ ...prev, [repository]: null }));

    try {
      const data = await apiClient.get(
        `/api/registry/tags/${encodeURIComponent(repository)}?limit=50`,
      );
      setTagCache((prev) => ({ ...prev, [repository]: data.tags || [] }));
    } catch (err) {
      console.error(`Failed to load tags for ${repository}:`, err);
      setTagErrors((prev) => ({
        ...prev,
        [repository]: err.message || "Failed to load tags",
      }));
    } finally {
      setLoadingTags((prev) => ({ ...prev, [repository]: false }));
    }
  }, [tagCache, loadingTags]);

  // Handle expanding a service row
  const handleToggle = useCallback(
    (service) => {
      if (service.external) return; // External images are not on GHCR
      if (expandedService === service.name) {
        setExpandedService(null);
      } else {
        setExpandedService(service.name);
        loadTags(service.repository);
      }
    },
    [expandedService, loadTags],
  );

  // Handle tag selection
  const handleSelectTag = useCallback(
    (serviceName, tag) => {
      const newValue = { ...value };
      if (tag === null) {
        // Clear override
        delete newValue[serviceName];
      } else {
        newValue[serviceName] = { tag };
      }
      onChange?.(newValue);
    },
    [value, onChange],
  );

  // Handle manual tag input
  const handleManualTag = useCallback(
    (serviceName, tag) => {
      if (tag.trim()) {
        handleSelectTag(serviceName, tag.trim());
      }
    },
    [handleSelectTag],
  );

  if (!services || services.length === 0) {
    return (
      <div style="color: var(--color-text-muted); font-size: 0.875rem; padding: 0.5rem 0;">
        No services defined in the selected profile.
      </div>
    );
  }

  return (
    <div className="tag-picker">
      <div className="tag-picker-header">
        <span>Service</span>
        <span>Tag Override</span>
      </div>
      {services.map((service) => {
        const currentValue = value[service.name];
        const isExpanded = expandedService === service.name;
        const tags = tagCache[service.repository] || [];
        const isLoading = loadingTags[service.repository];
        const error = tagErrors[service.repository];

        return (
          <div key={service.name} className="tag-picker-row">
            <div
              className="tag-picker-service"
              onClick={() => !disabled && handleToggle(service)}
              style={disabled || service.external ? "cursor: default;" : "cursor: pointer;"}
            >
              {!service.external && (
                <span className="tag-picker-expand">
                  {isExpanded ? "▼" : "▶"}
                </span>
              )}
              <span className="tag-picker-name">{service.name}</span>
              {service.external ? (
                <span className="tag-picker-auto">(external image)</span>
              ) : currentValue?.tag ? (
                <span className="tag-picker-selected">
                  <code>{currentValue.tag}</code>
                  {!disabled && (
                    <button
                      className="tag-picker-clear"
                      onClick={(e) => {
                        e.stopPropagation();
                        handleSelectTag(service.name, null);
                      }}
                      title="Clear override"
                    >
                      ×
                    </button>
                  )}
                </span>
              ) : (
                <span className="tag-picker-auto">(auto-discover)</span>
              )}
            </div>

            {isExpanded && !disabled && !service.external && (
              <div className="tag-picker-dropdown">
                {/* Manual input */}
                <div className="tag-picker-manual">
                  <input
                    type="text"
                    placeholder="Enter tag manually..."
                    onKeyDown={(e) => {
                      if (e.key === "Enter") {
                        handleManualTag(service.name, e.target.value);
                        e.target.value = "";
                        setExpandedService(null);
                      }
                    }}
                  />
                </div>

                {/* Tag list */}
                {isLoading && (
                  <div className="tag-picker-loading">Loading tags...</div>
                )}

                {error && <div className="tag-picker-error">{error}</div>}

                {!isLoading && !error && tags.length === 0 && (
                  <div className="tag-picker-empty">
                    No tags found. Enter a tag manually above.
                  </div>
                )}

                {!isLoading && tags.length > 0 && (
                  <div className="tag-picker-tags">
                    {tags.map((t) => (
                      <div
                        key={t.tag}
                        className={`tag-picker-tag ${currentValue?.tag === t.tag ? "selected" : ""}`}
                        onClick={() => {
                          handleSelectTag(service.name, t.tag);
                          setExpandedService(null);
                        }}
                      >
                        <span className="tag-picker-tag-name">{t.tag}</span>
                        <span className="tag-picker-tag-meta">
                          {formatSize(t.size_bytes)} · {formatDate(t.pushed_at)}
                        </span>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}

// Helper: format file size
function formatSize(bytes) {
  if (!bytes) return "";
  const units = ["B", "KB", "MB", "GB"];
  let size = bytes;
  let unitIndex = 0;
  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024;
    unitIndex++;
  }
  return `${size.toFixed(1)}${units[unitIndex]}`;
}

// Helper: format date
function formatDate(dateStr) {
  if (!dateStr) return "";
  const date = new Date(dateStr);
  const now = new Date();
  const diffMs = now - date;
  const diffDays = Math.floor(diffMs / (1000 * 60 * 60 * 24));

  if (diffDays === 0) return "today";
  if (diffDays === 1) return "yesterday";
  if (diffDays < 7) return `${diffDays}d ago`;
  if (diffDays < 30) return `${Math.floor(diffDays / 7)}w ago`;
  return `${Math.floor(diffDays / 30)}mo ago`;
}
