import { useState, useEffect } from "preact/hooks";
import { TabNav } from "../components/TabNav";
import { ConfigOverview } from "../components/config/ConfigOverview";
import { DeploymentRulesList } from "../components/config/DeploymentRulesList";
import { DeploymentProfilesList } from "../components/config/DeploymentProfilesList";
import { ResolutionStrategiesList } from "../components/config/ResolutionStrategiesList";

const CONFIG_TABS = [
  { id: "overview", label: "Overview" },
  { id: "rules", label: "Deployment Rules" },
  { id: "profiles", label: "Deployment Profiles" },
  { id: "strategies", label: "Resolution Strategies" },
];

export function Config() {
  const [activeTab, setActiveTab] = useState("overview");

  const renderTabContent = () => {
    switch (activeTab) {
      case "overview":
        return <ConfigOverview />;
      case "rules":
        return <DeploymentRulesList />;
      case "profiles":
        return <DeploymentProfilesList />;
      case "strategies":
        return <ResolutionStrategiesList />;
      default:
        return <ConfigOverview />;
    }
  };

  return (
    <div className="page">
      <h2>Configuration</h2>

      <TabNav
        tabs={CONFIG_TABS}
        activeTab={activeTab}
        onTabChange={setActiveTab}
      />

      <div>{renderTabContent()}</div>
    </div>
  );
}
