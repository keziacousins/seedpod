/**
 * The first SPA tests (backlog #20).
 *
 * `time-utils` is the right place to start: it is pure, needs no DOM, and it is
 * where smoke 6's worst UI defect lived. Five call sites did `new Date(s + "Z")`
 * on timestamps v2 already serializes with `+00:00`, producing `...+00:00Z` — an
 * Invalid Date — and rendering `NaNh NaNm` across the UI. `parseUTC` always
 * handled both forms; it simply was not being used, and nothing would have caught
 * the regression if it were reintroduced. Now something does.
 */

import { describe, expect, it } from "vitest";

import { formatDate, formatDateTime, formatTime, parseUTC } from "./time-utils";

describe("parseUTC", () => {
  it("parses the +00:00 offset v2 actually sends", () => {
    // The exact shape v2 serializes (aware datetimes; naive are banned in core/).
    const d = parseUTC("2026-08-10T17:53:33.754426+00:00");
    expect(d).toBeInstanceOf(Date);
    expect(Number.isNaN(d.getTime())).toBe(false);
    expect(d.toISOString()).toBe("2026-08-10T17:53:33.754Z");
  });

  it("parses a bare Z suffix", () => {
    expect(parseUTC("2026-08-10T17:53:33Z").toISOString()).toBe("2026-08-10T17:53:33.000Z");
  });

  it("treats a naive timestamp as UTC rather than local time", () => {
    // v1 emitted these bare. Reading one as local time would silently shift every
    // rendered timestamp by the viewer's offset.
    expect(parseUTC("2026-08-10T17:53:33").toISOString()).toBe("2026-08-10T17:53:33.000Z");
  });

  it("never produces an Invalid Date for a v2 timestamp -- smoke 6's regression", () => {
    // The defect was `new Date(isoString + "Z")` applied unconditionally, which
    // yields "...+00:00Z". Pinned directly so reintroducing it fails here.
    const naive = new Date("2026-08-10T17:53:33.754426+00:00" + "Z");
    expect(Number.isNaN(naive.getTime())).toBe(true); // what the bug produced
    expect(Number.isNaN(parseUTC("2026-08-10T17:53:33.754426+00:00").getTime())).toBe(false);
  });

  it("returns null for empty input rather than an Invalid Date", () => {
    expect(parseUTC("")).toBeNull();
    expect(parseUTC(null)).toBeNull();
    expect(parseUTC(undefined)).toBeNull();
  });
});

describe("formatters", () => {
  it("render a placeholder instead of NaN for missing input", () => {
    // "NaNh NaNm" reaching the UI is the symptom smoke 6 found; every formatter
    // must degrade to a dash.
    for (const fn of [formatDateTime, formatTime, formatDate]) {
      expect(fn(null)).toBe("-");
      expect(fn("")).toBe("-");
    }
  });

  it("render something non-empty for a real v2 timestamp", () => {
    const ts = "2026-08-10T17:53:33.754426+00:00";
    for (const fn of [formatDateTime, formatTime, formatDate]) {
      const out = fn(ts);
      expect(out).not.toBe("-");
      expect(out).not.toMatch(/NaN/);
    }
  });
});
