import { useState } from "preact/hooks";
import { route } from "preact-router";
import { apiClient } from "../lib/api-client";

export function Login({ onLogin }) {
  const [token, setToken] = useState("");
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(false);

  const handleSubmit = async (e) => {
    e.preventDefault();

    if (!token.trim()) {
      setError("Please enter an API token");
      return;
    }

    setLoading(true);
    setError(null);

    try {
      // Set token and try to fetch clusters to validate
      apiClient.setToken(token);
      const clusters = await apiClient.get("/api/clusters");

      // Token is valid - notify parent
      onLogin(token);
      route("/clusters");
    } catch (err) {
      setError(err.message);
      apiClient.setToken(null);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="login-container">
      <div className="login-box">
        <div className="login-header">
          <div className="login-logo">
            <picture>
              <source
                srcset="/logo-dark.svg"
                media="(prefers-color-scheme: dark)"
              />
              <img src="/logo-light.svg" alt="Seedpod" />
            </picture>
          </div>
          <h1>Seedpod</h1>
        </div>
        <p>Enter your API token to continue</p>

        <form onSubmit={handleSubmit}>
          <input
            type="password"
            placeholder="API Token"
            value={token}
            onInput={(e) => setToken(e.target.value)}
            className="token-input"
            disabled={loading}
          />

          {error && <div className="error-message">{error}</div>}

          <button type="submit" className="btn-primary" disabled={loading}>
            {loading ? "Validating..." : "Login"}
          </button>
        </form>

        {/* v1's command (`uv run python cli.py bootstrap username`) stood here until
            2026-08-14. v2 has no cli.py: DR-0021 split the entry points by trust
            model, and creating a key is the OFFLINE, direct-DB one. */}
        <p className="help-text">
          Generate a token with: <code>seedpod-bootstrap create-admin &lt;username&gt;</code>
        </p>
      </div>
    </div>
  );
}
