# Seedpod UI

The web UI for clusters, deployments, snapshots, secrets, and config. Preact + Vite, with
Server-Sent Events for live updates.

In a release, seedpod serves this bundle itself, same-origin (DR-0041 decision 3). The vite
dev server below is a development workflow only.

## Development

You need Node 24 (the version CI builds with) and a running seedpod API, by default at
`http://localhost:8000`.

```bash
npm install
cp .env.example .env    # VITE_API_URL=http://localhost:8000
npm run dev             # http://localhost:5173
```

The dev server calls the API cross-origin. `SEEDPOD_CORS_ORIGINS` defaults to `*`, so this
works with no server-side change.

### Getting a token

The UI authenticates with an API key pasted into the login form. Mint the first one with:

```bash
seedpod-bootstrap create-admin <username>
```

It prints the key once and never again. There is no key file to read it back from.

### Tests

```bash
npm test          # vitest, single run
npm run test:watch
```

## Build

```bash
npm run build     # -> dist/
npm run preview
```

`dist/` ships in the release artifact, so the appliance needs no Node at runtime. `vite build`
runs in production mode and loads `.env.production`, which sets `VITE_API_URL` to empty —
`api-client.js` and `sse-client.js` both read `import.meta.env.VITE_API_URL || ''`, and an
empty base means same-origin. That file exists to override a developer's gitignored `.env`:
a release once shipped a bundle that sent every request to `localhost:8000`, which worked on
the server host and failed from every other machine.

## Routes

Routing is `preact-router` over real paths, so the server needs an SPA fallback for deep
links. `seedpod/api/spa.py` provides one, mounted from `seedpod/__main__.py` when
`SEEDPOD_UI_DIR` is set.

| Path | Page |
|---|---|
| `/clusters`, `/clusters/:clusterId` | cluster list and detail |
| `/clusters/:clusterId/pods/:namespace/:podName` | pod detail and logs |
| `…/containers/:containerName` | container detail |
| `/deployments`, `/deployments/:deploymentId` | deployment history and detail |
| `/presets`, `/presets/:presetId` | deploy presets |
| `/snapshots`, `/snapshots/:snapshotId` | snapshots |
| `/secrets` | secret management |
| `/keys`, `/keys/create`, `/keys/:keyId` | API keys |
| `/workflows` | workflow definitions |
| `/config`, `/config/rules/:ruleName`, `/config/profiles/:profileName`, `/config/strategies/:strategyName` | the on-disk config, browsable |
| `/health` | health |

## Layout

```
src/
├── app.jsx, main.jsx    routing and mount
├── components/          TopNav, TabNav, Table, Card, StatusBadge, Breadcrumb,
│   │                    modals (deploy, destroy, snapshot, restore, confirm),
│   │                    MiniEventHud, ConnectionStatus, HiddenSecret, …
│   └── config/          config browser views
├── pages/               one per route above
├── hooks/               useSSE, useEventHistory
├── lib/                 api-client, sse-client, event-store, time-utils
└── styles/
```

## Live updates

The UI subscribes to `/api/events/stream` and updates without polling: cluster state changes
(`PENDING` → `PROVISIONING` → `ACTIVE` …), deployment progress, and pod status. `event-store.js`
keeps the recent history that `MiniEventHud` shows.

## Environment variables

- `VITE_API_URL` — API base. `http://localhost:8000` for the dev server; empty in production
  builds, meaning same-origin.
