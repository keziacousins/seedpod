# Seedpod UI

Web UI for managing K3s clusters, deployments, and infrastructure.

## Tech Stack

- **Preact** - Lightweight React alternative
- **Vite** - Fast build tool and dev server
- **Preact Router** - Client-side routing
- **Server-Sent Events (SSE)** - Real-time updates from backend

## Development

### Prerequisites

- Node.js 18+
- Running `seedpod` API server (default: http://localhost:8000)

### Setup

```bash
# Install dependencies
npm install

# Copy environment config
cp .env.example .env

# Start dev server
npm run dev
```

The UI will be available at http://localhost:5173

### Authentication

1. Generate an API token from the seedpod CLI:
   ```bash
   cd ..
   uv run seedpod bootstrap myuser --expires-days 365
   ```

2. Copy the token from `admin-api-key.txt` or CLI output

3. Open the UI and paste the token in the login form

### Project Structure

```
src/
├── components/       # Reusable UI components
│   ├── TopNav.jsx
│   ├── TabNav.jsx
│   ├── Table.jsx
│   ├── Card.jsx
│   ├── StatusBadge.jsx
│   └── Breadcrumb.jsx
├── pages/           # Route pages
│   ├── Login.jsx
│   ├── ClusterList.jsx
│   ├── ClusterDetail.jsx
│   ├── PodDetail.jsx
│   ├── DeploymentList.jsx
│   ├── SecretList.jsx
│   └── ApiKeyList.jsx
├── lib/            # Utilities
│   ├── api-client.js
│   └── sse-client.js
├── hooks/          # Custom hooks
│   └── useSSE.js
└── app.jsx        # Main app with routing
```

### Routes

- `/clusters` - List all clusters
- `/clusters/:id` - Cluster details with pod list
- `/clusters/:id/pods/:name` - Pod details and logs
- `/deployments` - Deployment history
- `/secrets` - Secret management
- `/keys` - API key management

### Real-time Updates

The UI connects to the seedpod SSE endpoint (`/api/events/stream`) to receive real-time updates for:

- Cluster state changes (PENDING → PROVISIONING → ACTIVE, etc.)
- Deployment progress
- Pod status updates

Events are automatically handled and update the UI without polling.

## Build

```bash
# Production build
npm run build

# Preview production build
npm run preview
```

The build output will be in `dist/`.

## Environment Variables

- `VITE_API_URL` - API endpoint (default: http://localhost:8000)

