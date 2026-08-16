import { Link } from "preact-router/match";

/**
 * TabNav - Reusable tab navigation component
 *
 * Supports two modes:
 * 1. Routing tabs (top-level nav) - uses Link with routing
 * 2. Controlled tabs (page-level) - uses buttons with callbacks
 *
 * Usage:
 *   // Routing tabs (app-level navigation)
 *   <TabNav items={[
 *     { path: "/clusters", label: "Clusters" },
 *     { path: "/deployments", label: "Deployments" }
 *   ]} />
 *
 *   // Controlled tabs (page-level state)
 *   <TabNav
 *     tabs={[
 *       { id: "active", label: "Active Jobs", count: 5 },
 *       { id: "history", label: "History", count: 10 }
 *     ]}
 *     activeTab={activeTab}
 *     onTabChange={setActiveTab}
 *   />
 */
export function TabNav({ items, tabs, activeTab, onTabChange }) {
  // Page-level controlled tabs
  if (tabs && activeTab !== undefined && onTabChange) {
    return (
      <div className="tab-container">
        {tabs.map((tab) => (
          <button
            key={tab.id}
            className={activeTab === tab.id ? "tab-active" : "tab"}
            onClick={() => onTabChange(tab.id)}
          >
            {tab.label}
            {tab.count !== undefined && ` (${tab.count})`}
          </button>
        ))}
      </div>
    );
  }

  // App-level routing tabs
  return (
    <div className="tab-nav">
      {items.map((item) => (
        <Link
          key={item.path}
          href={item.path}
          className="tab-link"
          activeClassName="active"
        >
          {item.label}
        </Link>
      ))}
    </div>
  );
}
