/**
 * How a container env var gets its value, as text for the container page.
 *
 * A pod's env var is either a literal (`value`) or a reference (`valueFrom`).
 * Since DR-0051 every secret is a reference, so "no `value`" no longer means
 * "not set": the variable is set, from a Secret, and the pod spec does not hold
 * the value. This says which, and never shows a value that the API did not send.
 *
 * Returns `{ text, source }`. `source` is `"literal"`, `"secret"`, `"configmap"`,
 * `"field"`, `"resource"` or `"empty"`; the page styles a reference differently
 * from a literal.
 */
export function describeEnvVar(envVar) {
  const from = envVar?.valueFrom;
  if (from?.secretKeyRef) {
    const { name, key } = from.secretKeyRef;
    return { text: `from secret ${name} / ${key}`, source: "secret" };
  }
  if (from?.configMapKeyRef) {
    const { name, key } = from.configMapKeyRef;
    return { text: `from configmap ${name} / ${key}`, source: "configmap" };
  }
  if (from?.fieldRef) {
    return { text: `from field ${from.fieldRef.fieldPath}`, source: "field" };
  }
  if (from?.resourceFieldRef) {
    return { text: `from resource ${from.resourceFieldRef.resource}`, source: "resource" };
  }
  const value = envVar?.value;
  if (value === undefined || value === null || value === "") {
    return { text: "(empty)", source: "empty" };
  }
  return { text: String(value), source: "literal" };
}
