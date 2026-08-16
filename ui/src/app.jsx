import { useState, useEffect } from "preact/hooks";
import Router from "preact-router";
import { TopNav } from "./components/TopNav";
import { TabNav } from "./components/TabNav";
import { MiniEventHud } from "./components/MiniEventHud";
import { Login } from "./pages/Login";
import { ClusterList } from "./pages/ClusterList";
import { ClusterDetail } from "./pages/ClusterDetail";
import { PodDetail } from "./pages/PodDetail";
import { ContainerDetail } from "./pages/ContainerDetail";
import { DeploymentList } from "./pages/DeploymentList";
import { DeploymentDetail } from "./pages/DeploymentDetail";
import { SecretList } from "./pages/SecretList";
import { ApiKeyList } from "./pages/ApiKeyList";
import { CreateApiKey } from "./pages/CreateApiKey";
import { ApiKeyDetail } from "./pages/ApiKeyDetail";
import { Workflows } from "./pages/Workflows";
import { Health } from "./pages/Health";
import { Config } from "./pages/Config";
import { RuleDetail } from "./pages/RuleDetail";
import { ProfileDetail } from "./pages/ProfileDetail";
import { StrategyDetail } from "./pages/StrategyDetail";
import { PresetList } from "./pages/PresetList";
import { PresetDetail } from "./pages/PresetDetail";
import { SnapshotList } from "./pages/SnapshotList";
import { SnapshotDetail } from "./pages/SnapshotDetail";
import { apiClient } from "./lib/api-client";
import { sseClient } from "./lib/sse-client";
import { eventStore } from "./lib/event-store";
import { useEventHistory } from "./hooks/useEventHistory";
import "./styles/app.css";

const NAV_ITEMS = [
  { path: "/clusters", label: "Clusters" },
  { path: "/deployments", label: "Deployments" },
  { path: "/presets", label: "Presets" },
  { path: "/snapshots", label: "Snapshots" },
  { path: "/secrets", label: "Secrets" },
  { path: "/keys", label: "API Keys" },
  { path: "/workflows", label: "Workflows" },
  { path: "/config", label: "Config" },
  { path: "/health", label: "Health" },
];

export function App() {
  const [isAuthenticated, setIsAuthenticated] = useState(false);
  const [user, setUser] = useState(null);
  const [hudVisible, setHudVisible] = useState(false);

  useEffect(() => {
    console.log("[App] Initializing application...");

    // Initialize event store as an SSE consumer (like any other listener)
    // This must happen BEFORE connecting SSE
    eventStore.initialize(sseClient);
    console.log("[App] Event store initialized");

    // Check if we have a token on mount
    const token = apiClient.getToken();
    if (token) {
      console.log("[App] Token found, authenticating and connecting SSE...");
      setIsAuthenticated(true);
      setUser({ username: "User" }); // Could fetch actual user info
      sseClient.connect(token);
    } else {
      console.log("[App] No token found, showing login");
    }

    // Cleanup on unmount
    return () => {
      console.log("[App] Cleaning up, disconnecting SSE...");
      sseClient.disconnect();
    };
  }, []);

  const handleLogin = (token) => {
    setIsAuthenticated(true);
    setUser({ username: "User" });
    sseClient.connect(token);
  };

  const handleLogout = () => {
    apiClient.setToken(null);
    sseClient.disconnect();
    setIsAuthenticated(false);
    setUser(null);
  };

  const toggleHud = () => {
    console.log(`[App] ${hudVisible ? "Closing" : "Opening"} HUD`);
    setHudVisible(!hudVisible);
  };

  if (!isAuthenticated) {
    return <Login onLogin={handleLogin} />;
  }

  return (
    <div className={`app ${hudVisible ? "hud-visible" : ""}`}>
      <TopNav
        user={user}
        onLogout={handleLogout}
        onToggleHud={toggleHud}
        hudVisible={hudVisible}
      />
      <div
        className={`mini-event-hud-container ${hudVisible ? "visible" : ""}`}
      >
        <MiniEventHud />
      </div>
      <TabNav items={NAV_ITEMS} />

      <div className="content">
        <Router>
          <ClusterList path="/clusters" />
          <ClusterDetail path="/clusters/:clusterId" />
          <PodDetail path="/clusters/:clusterId/pods/:namespace/:podName" />
          <ContainerDetail path="/clusters/:clusterId/pods/:namespace/:podName/containers/:containerName" />
          <DeploymentList path="/deployments" />
          <DeploymentDetail path="/deployments/:deploymentId" />
          <PresetList path="/presets" />
          <PresetDetail path="/presets/:presetId" />
          <SnapshotList path="/snapshots" />
          <SnapshotDetail path="/snapshots/:snapshotId" />
          <SecretList path="/secrets" />
          <ApiKeyList path="/keys" />
          <CreateApiKey path="/keys/create" />
          <ApiKeyDetail path="/keys/:keyId" />
          <Workflows path="/workflows" />
          <Health path="/health" />
          <Config path="/config" />
          <RuleDetail path="/config/rules/:ruleName" />
          <ProfileDetail path="/config/profiles/:profileName" />
          <StrategyDetail path="/config/strategies/:strategyName" />
          <ClusterList default />
        </Router>
      </div>
    </div>
  );
}
