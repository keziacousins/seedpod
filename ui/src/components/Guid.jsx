import { useState } from "preact/hooks";
import { route } from "preact-router";

/**
 * Guid - Display component for GUIDs with copy-to-clipboard functionality
 *
 * Features:
 * - Shows truncated GUID (default: 8 chars)
 * - Hover reveals full GUID in tooltip
 * - Copy icon for clipboard functionality
 * - Optional link navigation (prioritizes navigation over copy in click area)
 * - Visual feedback on copy (checkmark replaces copy icon briefly)
 *
 * Usage:
 *   <Guid value="a1b2c3d4-5e6f-7g8h-9i0j-1k2l3m4n5o6p" />
 *   <Guid value={clusterId} linkTo={`/clusters/${clusterId}`} />
 *   <Guid value={id} length={6} />
 */

export function Guid({ value, linkTo, length = 8 }) {
  const [copied, setCopied] = useState(false);

  if (!value) {
    return <span className="guid-missing">-</span>;
  }

  // Remove hyphens for display
  const cleanGuid = value.replace(/-/g, "");
  const shortGuid = cleanGuid.substring(0, length);

  const handleCopy = async (e) => {
    e.preventDefault();
    e.stopPropagation();

    try {
      await navigator.clipboard.writeText(value);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch (err) {
      console.error("Failed to copy GUID:", err);
      // Fallback: select text
      const el = document.createElement("textarea");
      el.value = value;
      document.body.appendChild(el);
      el.select();
      document.execCommand("copy");
      document.body.removeChild(el);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    }
  };

  const handleClick = (e) => {
    if (linkTo) {
      e.preventDefault();
      e.stopPropagation();
      route(linkTo);
    }
  };

  return (
    <span className="guid-container">
      <span
        className={`guid-display ${linkTo ? "guid-display-link" : ""}`}
        title={value}
        onClick={handleClick}
      >
        <span className="guid-text">{shortGuid}</span>
        {linkTo && <span className="guid-arrow">→</span>}
      </span>
      <button
        className="guid-copy-btn"
        onClick={handleCopy}
        title={copied ? "Copied!" : "Copy to clipboard"}
        aria-label="Copy GUID to clipboard"
      >
        {copied ? (
          <svg
            className="guid-copy-icon guid-copy-success"
            width="14"
            height="14"
            viewBox="0 0 16 16"
            fill="none"
            xmlns="http://www.w3.org/2000/svg"
          >
            <path
              d="M13.78 4.22a.75.75 0 010 1.06l-7.25 7.25a.75.75 0 01-1.06 0L2.22 9.28a.75.75 0 011.06-1.06L6 10.94l6.72-6.72a.75.75 0 011.06 0z"
              fill="currentColor"
            />
          </svg>
        ) : (
          <svg
            className="guid-copy-icon"
            width="14"
            height="14"
            viewBox="0 0 16 16"
            fill="none"
            xmlns="http://www.w3.org/2000/svg"
          >
            <path
              d="M0 6.75C0 5.784.784 5 1.75 5h1.5a.75.75 0 010 1.5h-1.5a.25.25 0 00-.25.25v7.5c0 .138.112.25.25.25h7.5a.25.25 0 00.25-.25v-1.5a.75.75 0 011.5 0v1.5A1.75 1.75 0 019.25 16h-7.5A1.75 1.75 0 010 14.25v-7.5z"
              fill="currentColor"
            />
            <path
              d="M5 1.75C5 .784 5.784 0 6.75 0h7.5C15.216 0 16 .784 16 1.75v7.5A1.75 1.75 0 0114.25 11h-7.5A1.75 1.75 0 015 9.25v-7.5zm1.75-.25a.25.25 0 00-.25.25v7.5c0 .138.112.25.25.25h7.5a.25.25 0 00.25-.25v-7.5a.25.25 0 00-.25-.25h-7.5z"
              fill="currentColor"
            />
          </svg>
        )}
      </button>
    </span>
  );
}
