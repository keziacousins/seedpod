/**
 * `describeEnvVar` — what the container page says about one env var.
 *
 * Found on the first full-stack deploy after DR-0051: every secret showed as
 * `POSTGRES_PASSWORD: (not set)`, on a pod that was running with its password.
 * The page printed `value || "(not set)"`, and a secret reference has no `value`.
 */

import { describe, expect, it } from "vitest";

import { describeEnvVar } from "./env-var";

describe("describeEnvVar", () => {
  it("shows a literal value as it is", () => {
    expect(describeEnvVar({ name: "LOG_LEVEL", value: "info" })).toEqual({ text: "info", source: "literal" });
  });

  it("names the Secret and the key for a secret reference, not '(not set)'", () => {
    // The exact shape the cluster API sends for a DR-0051 secret.
    const envVar = {
      name: "POSTGRES_PASSWORD",
      value: "",
      valueFrom: { secretKeyRef: { name: "audit-postgres-secrets", key: "audit_postgres_password-53b0a859" } },
    };
    expect(describeEnvVar(envVar)).toEqual({
      text: "from secret audit-postgres-secrets / audit_postgres_password-53b0a859",
      source: "secret",
    });
  });

  it("names the other kinds of reference", () => {
    expect(describeEnvVar({ name: "A", valueFrom: { configMapKeyRef: { name: "cfg", key: "a" } } }).text).toBe(
      "from configmap cfg / a",
    );
    expect(describeEnvVar({ name: "B", valueFrom: { fieldRef: { fieldPath: "status.podIP" } } }).text).toBe(
      "from field status.podIP",
    );
    expect(describeEnvVar({ name: "C", valueFrom: { resourceFieldRef: { resource: "limits.cpu" } } }).text).toBe(
      "from resource limits.cpu",
    );
  });

  it("says a variable with no value and no reference is empty", () => {
    expect(describeEnvVar({ name: "X", value: "" })).toEqual({ text: "(empty)", source: "empty" });
    expect(describeEnvVar({ name: "X" })).toEqual({ text: "(empty)", source: "empty" });
  });

  it("keeps a literal that is falsy but not empty", () => {
    expect(describeEnvVar({ name: "REPLICAS", value: "0" }).text).toBe("0");
    expect(describeEnvVar({ name: "FLAG", value: "false" }).text).toBe("false");
  });
});
