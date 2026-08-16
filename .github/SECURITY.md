# Security policy

Seedpod is a control plane. It holds encrypted kubeconfigs, deployment secrets and
provider API tokens, and it runs commands against real infrastructure. A defect
here can destroy a cluster or disclose a credential, so please report suspected
vulnerabilities privately rather than in a public issue.

## Reporting a vulnerability

Use GitHub's private vulnerability reporting:
**[Report a vulnerability](https://github.com/keziacousins/seedpod/security/advisories/new)**
(Security tab → Report a vulnerability). That opens a channel visible only to the
maintainers.

Please include what you need to make it reproducible: affected version or commit,
the provider involved if any, and the smallest set of steps that shows the
problem. If you have a patch, attach it to the advisory rather than opening a pull
request, so the fix and the disclosure land together.

This is a personal project with no bug bounty and no paid support. Expect a first
response within about two weeks. Please give a fix a reasonable window before
disclosing publicly; if the report goes unanswered, disclose.

## Scope

**In scope** — anything that breaks a boundary the system claims to hold:

- Recovering plaintext from a stored secret, kubeconfig or manifest without the
  Fernet key, or a flaw in how those keys are derived, scoped or applied.
- Bypassing API authentication or the permission model (`seedpod/api/auth.py`,
  `seedpod/api/permissions.py`) — reaching an endpoint or an environment the
  presented key is not entitled to.
- Command or template injection through user-supplied values: a cluster name,
  branch, preset or profile field reaching a subprocess or a rendered manifest
  unescaped.
- Anything letting a request act on a cluster in an environment outside its
  entitlement, particularly a destroy.
- Credential disclosure through logs, API responses, SSE events or error text.

**Not vulnerabilities** — deliberate design, documented where it lives:

- **The example configuration is an example.** `config/` ships an `exampleco`
  tree with placeholder values, and `config/providers/digitalocean.yml` opens SSH
  and the K3s API to `0.0.0.0/0` with an inline warning to change it. Those
  defaults are illustrative, not recommended; replace them before deploying
  anything real.
- **`seedpod-bootstrap` trusts local filesystem access.** It is never exposed over
  HTTP, and its trust boundary is the machine it runs on (DR-0021).
- **Losing a Fernet key is unrecoverable.** That is the encryption working. See
  `docs/guides/operations.md` §1 for the custody requirement.
- Findings that require an already-compromised host, or that depend on a `.env`
  the attacker can already read — a copy of `.env` is a copy of everything, by
  design.

## Supported versions

Seedpod is pre-1.0 (`2.0.0a0`) and has no release branches. Only the current
`main` receives fixes. Requires Python 3.11 or newer.

## What this project already does

So you can tell a real finding from a known and accepted position:

- Secrets are encrypted at rest with two scoped Fernet keys (DEV and PROD), and
  each ciphertext records which key class wrote it.
- No credential is committed. `.env`, `db/`, `/data/`, `logs/` and
  `admin-api-key.txt` are gitignored, and the tree was scanned before publication.
- CI holds no secrets and reaches no provider. It runs on `pull_request`, never
  `pull_request_target`, so a fork's code never runs with this repository's
  credentials.
- Every GitHub Action is pinned to a full commit SHA, and Dependabot proposes the
  bumps. CodeQL and dependency review run on every pull request.
