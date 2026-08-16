import { useState } from "preact/hooks";

/**
 * HiddenSecret - Display component for secrets with in-place reveal
 *
 * Features:
 * - Shows "hidden" with lock icon by default
 * - Click to reveal the secret value in place
 * - Copy icon for clipboard functionality
 * - Auto-hides after 60 seconds
 * - Visual feedback on copy (checkmark replaces copy icon briefly)
 *
 * Usage:
 *   <HiddenSecret
 *     environment="ephemeral"
 *     keyName="DATABASE_URL"
 *     onReveal={async () => { ... return secretValue; }}
 *   />
 */

export function HiddenSecret({ environment, keyName, onReveal }) {
  const [revealed, setRevealed] = useState(false);
  const [secretValue, setSecretValue] = useState(null);
  const [loading, setLoading] = useState(false);
  const [copied, setCopied] = useState(false);
  const [error, setError] = useState(null);

  const handleReveal = async (e) => {
    e.preventDefault();
    e.stopPropagation();

    if (revealed) return; // Already revealed

    try {
      setLoading(true);
      setError(null);
      const value = await onReveal();
      setSecretValue(value);
      setRevealed(true);

      // Auto-hide after 5 seconds
      setTimeout(() => {
        setRevealed(false);
        setSecretValue(null);
      }, 5000);
    } catch (err) {
      setError(err.message);
      console.error("Failed to reveal secret:", err);
    } finally {
      setLoading(false);
    }
  };

  const handleCopy = async (e) => {
    e.preventDefault();
    e.stopPropagation();

    try {
      await navigator.clipboard.writeText(secretValue);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch (err) {
      console.error("Failed to copy secret:", err);
      // Fallback: select text
      const el = document.createElement("textarea");
      el.value = secretValue;
      document.body.appendChild(el);
      el.select();
      document.execCommand("copy");
      document.body.removeChild(el);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    }
  };

  if (loading) {
    return (
      <span className="hidden-secret-container">
        <span className="hidden-secret-loading">Loading...</span>
      </span>
    );
  }

  if (error) {
    return (
      <span className="hidden-secret-container">
        <span className="hidden-secret-error" title={error}>
          ⚠️ Error
        </span>
      </span>
    );
  }

  if (!revealed) {
    return (
      <span className="hidden-secret-container">
        <button
          className="hidden-secret-reveal-btn"
          onClick={handleReveal}
          title="Click to reveal secret"
        >
          <svg
            className="hidden-secret-lock-icon"
            width="12"
            height="12"
            viewBox="0 0 16 16"
            fill="none"
            xmlns="http://www.w3.org/2000/svg"
          >
            <path
              d="M4 4v2h-.25A1.75 1.75 0 002 7.75v5.5c0 .966.784 1.75 1.75 1.75h8.5A1.75 1.75 0 0014 13.25v-5.5A1.75 1.75 0 0012.25 6H12V4a4 4 0 10-8 0zm6.5 2V4a2.5 2.5 0 00-5 0v2h5z"
              fill="currentColor"
            />
          </svg>
          <span className="hidden-secret-text">hidden</span>
        </button>
      </span>
    );
  }

  return (
    <span className="hidden-secret-container">
      <span className="hidden-secret-revealed" title={secretValue}>
        <span className="hidden-secret-value">{secretValue}</span>
      </span>
      <button
        className="hidden-secret-copy-btn"
        onClick={handleCopy}
        title={copied ? "Copied!" : "Copy to clipboard"}
        aria-label="Copy secret to clipboard"
      >
        {copied ? (
          <svg
            className="hidden-secret-copy-icon hidden-secret-copy-success"
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
            className="hidden-secret-copy-icon"
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
