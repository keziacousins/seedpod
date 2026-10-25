/**
 * `selectedPermissions` — what the key form sends as `permissions`.
 *
 * Found when the form was needed for a real key: it sent `{ "clusters:read": true }`,
 * and `POST /api/keys` takes a list, so every create was a 422.
 */

import { describe, expect, it } from "vitest";

import { selectedPermissions } from "./permissions";

describe("selectedPermissions", () => {
  it("sends a list of the checked permissions", () => {
    expect(selectedPermissions({ "clusters:read": true, "kubeconfig:read": true })).toEqual([
      "clusters:read",
      "kubeconfig:read",
    ]);
  });

  it("leaves out a permission that was checked and then cleared", () => {
    expect(selectedPermissions({ "clusters:read": true, "secrets:read": false })).toEqual(["clusters:read"]);
  });

  it("is empty when nothing is checked", () => {
    expect(selectedPermissions({})).toEqual([]);
  });
});
