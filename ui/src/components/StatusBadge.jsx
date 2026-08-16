// Three-tier color system (v2 status set — ui-contract §3, DR-0002):
// - bright: active / in-progress states (new, provisioning, deploying, active, blocked, compensating…)
// - red:    failed / intervention needed (failed, destroy-failed, cancelled)
// - muted:  history / terminal states (superseded, rejected, destroyed, zombie, unmanaged)
//
// Keys are UPPER_SNAKE. Lookup normalizes hyphens → underscores so the hyphenated v2
// wire values ("destroy-scheduled", "destroy-failed") match (fixes the v1 bug where
// "destroy-scheduled".toUpperCase() === "DESTROY-SCHEDULED" never matched).

const STATUS_COLORS = {
  // Bright - active / in-progress
  NEW: "bright-blue", // deployment/workflow pre-persistence (v2)
  PENDING: "bright-yellow",
  PROVISIONING: "bright-blue", // cluster (absorbs v1 CREATING)
  DEPLOYING: "bright-yellow", // deployment-only now (cluster never deploys)
  ACTIVE: "bright-green",
  RUNNING: "bright-green", // pod status & workflow-run status
  SUCCEEDED: "bright-green", // workflow-run status (v1 SUCCESS)
  BLOCKED: "bright-orange", // workflow parked on unreachable infra (v2)
  COMPENSATING: "bright-orange", // workflow rolling back (v2)
  DESTROYING: "bright-orange",
  DESTROY_SCHEDULED: "bright-orange",

  // Red - failed / needs intervention
  FAILED: "red",
  DESTROY_FAILED: "red",
  CANCELLED: "red", // deployment was cancelled

  // Muted - history / terminal
  SUPERSEDED: "muted",
  REJECTED: "muted", // deployment superseded before running (v2)
  DESTROYED: "muted",
  ZOMBIE: "muted",
  UNMANAGED: "muted",

  // Boolean status indicators
  TRUE: "bright-green",
  FALSE: "red",
};

export function StatusBadge({ status }) {
  const key = status?.toUpperCase().replace(/-/g, "_") || "UNKNOWN";
  const color = STATUS_COLORS[key] || "muted";

  return <span className={`status-badge status-${color}`}>{status}</span>;
}
