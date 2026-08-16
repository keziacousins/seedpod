/**
 * `transition` — the HUD's status-change formatter (backlog #21, #20).
 *
 * This is the third defect of one class the HUD has produced, which is why the
 * logic is now an exported pure function instead of another inline `||` chain:
 *
 *   1. `data.status` on `deployment_status_changed` — a field v2 never sends, so
 *      the `|| "updated"` fallback fired on EVERY event (fixed, smoke 6).
 *   2. `data.status` on `pod_status_changed` — a field neither v1 nor v2 ever
 *      sent (v1's payload had `phase`), so it rendered `?` even against v1
 *      (removed with the topic, DR-0035).
 *   3. `data.old_status || "?"` rendering a DELIBERATELY empty birth value
 *      identically to a missing one (#21, fixed here).
 *
 * The common shape: a falsy-but-meaningful value treated as absent. These tests
 * pin the distinction rather than the phrasing.
 */

import { describe, expect, it } from "vitest";

import { transition } from "./MiniEventHud";

describe("transition", () => {
  it("renders a birth as an arrow with no left-hand side", () => {
    // `core/machine.py` sends old_status: "" at all four birth sites, preserving
    // v1's broadcast shape. That is the server stating "this did not exist", not
    // failing to tell us.
    expect(transition({ old_status: "", new_status: "pending" })).toBe("→ pending");
  });

  it("renders a normal transition with both sides", () => {
    expect(transition({ old_status: "pending", new_status: "deploying" })).toBe("pending → deploying");
  });

  it("distinguishes a deliberately empty old_status from a missing one", () => {
    // The whole point of #21: these two must not render identically.
    expect(transition({ old_status: "", new_status: "active" })).not.toBe(
      transition({ new_status: "active" }),
    );
    expect(transition({ new_status: "active" })).toBe("? → active");
  });

  it("falls back to `status` only when new_status is absent", () => {
    // Obligation 1 says v2 sends old_status/new_status. The `status` fallback is
    // kept for defensiveness but must never win over a real new_status.
    expect(transition({ old_status: "a", new_status: "b", status: "c" })).toBe("a → b");
    expect(transition({ old_status: "a", status: "c" })).toBe("a → c");
  });

  it("never throws on a malformed or absent payload", () => {
    // The HUD renders whatever arrives on the wire; a bad frame must not blank
    // the panel.
    expect(transition(undefined)).toBe("? → ?");
    expect(transition({})).toBe("? → ?");
  });
});
