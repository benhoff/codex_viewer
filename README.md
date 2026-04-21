# Codex Session Viewer

FastAPI viewer for Codex rollout sessions.

It is optimized for the shortest path to useful output:

- local import is the default
- `~/.codex/sessions` is the default source
- no `.env` file is required for first run
- no token or agent daemon is required for first run

Design notes:

- [Action queue scoring](docs/action-queue-scoring.md)
- [Self-hosted launch priorities](docs/selfhosted-launch-priorities.md)

## Fastest Path To Value

If you want to see your own sessions in the UI as quickly as possible, start here.

```bash
./scripts/bootstrap-local.sh
./scripts/start-server.sh
```

Then open `http://127.0.0.1:8000`.

`bootstrap-local.sh` installs Python dependencies into `.deps` and only invokes `npm` if the built CSS asset is missing.

What this does by default:

- runs in `local` sync mode
- imports from `~/.codex/sessions`
- stores SQLite data in `./data`
- starts the web UI on `127.0.0.1:8000`
- skips auth unless you explicitly enable it

If the dashboard is empty:

1. Make sure Codex has written at least one session on this machine.
2. If your sessions live somewhere else, set `CODEX_SESSION_ROOTS`.
3. Run a manual import:

```bash
PYTHONPATH=.deps python3 -m codex_session_viewer sync
```

Do not start by copying `.env.server.example` unless you are intentionally setting up a central server for other machines.

## Remote Server Mode

Use `remote` mode only when you want one server to collect uploads from other machines.

Server setup:

```bash
cp .env.server.example .env
docker compose up --build -d
```

Then open the viewer, finish `/setup`, create a sync token, and connect an agent host.

Agent host setup:

```bash
cp .env.agent.example .env
./scripts/bootstrap-local.sh --skip-css
./scripts/start-agent-daemon.sh
```

Set these values in the agent `.env` file:

- `CODEX_VIEWER_SERVER_URL`
- `CODEX_VIEWER_SYNC_API_TOKEN`

The agent wrapper already forces `CODEX_VIEWER_SYNC_MODE=remote`.

## Commands

Start the web app:

```bash
./scripts/start-server.sh
```

Override the bind target without editing `.env`:

```bash
./scripts/start-server.sh --host 127.0.0.1 --port 8001
```

Run a one-shot local import:

```bash
PYTHONPATH=.deps python3 -m codex_session_viewer sync
```

Force a full rebuild:

```bash
PYTHONPATH=.deps python3 -m codex_session_viewer sync --rebuild
```

Run the remote sync daemon:

```bash
./scripts/start-agent-daemon.sh
```

Export one session:

```bash
PYTHONPATH=.deps python3 -m codex_session_viewer export SESSION_ID --format markdown
```

Create a whole-instance backup archive:

```bash
PYTHONPATH=.deps python3 -m codex_session_viewer backup create --output ./codex-viewer-backup.zip
```

Verify a backup archive:

```bash
PYTHONPATH=.deps python3 -m codex_session_viewer backup verify ./codex-viewer-backup.zip
```

Restore a backup archive into a fresh data directory:

```bash
PYTHONPATH=.deps python3 -m codex_session_viewer backup restore ./codex-viewer-backup.zip --data-dir ./restore-data
```

## Configuration

You usually only need to care about these variables:

- `CODEX_VIEWER_SYNC_MODE`: `local` by default, `remote` for central-server deployments
- `CODEX_SESSION_ROOTS`: comma-separated local import roots, default `~/.codex/sessions`
- `CODEX_VIEWER_SERVER_URL`: required for remote agents uploading to a server
- `CODEX_VIEWER_SYNC_API_TOKEN`: required for remote agents
- `CODEX_VIEWER_REMOTE_BATCH_SIZE`: remote daemon upload batch size, default `25`
- `CODEX_VIEWER_AUTH_MODE`: `none`, `password`, `proxy`, or `password_or_proxy`

Env files are loaded in this order:

- `.env`
- `.env.<CODEX_VIEWER_ENV>`
- `.env.local`
- `.env.<CODEX_VIEWER_ENV>.local`

Example env files:

- `.env.server.example`: central server using remote uploads
- `.env.agent.example`: remote daemon host
- `.env.development.example`: repo development with reload enabled

## Auth

Browser auth is optional.

Built-in password auth:

```env
CODEX_VIEWER_AUTH_MODE=password
```

Reverse-proxy header auth:

```env
CODEX_VIEWER_AUTH_MODE=proxy
CODEX_VIEWER_AUTH_PROXY_USER_HEADER=X-Forwarded-User
CODEX_VIEWER_AUTH_PROXY_NAME_HEADER=X-Forwarded-Name
CODEX_VIEWER_AUTH_PROXY_EMAIL_HEADER=X-Forwarded-Email
```

If auth is enabled and no admin exists yet, the first visit will route through `/setup` so the initial admin can be created or claimed.

## Docker

`docker compose up --build -d` is intentionally configured for remote-server mode.

Defaults in [compose.yml](compose.yml):

- `CODEX_VIEWER_SYNC_MODE=remote`
- SQLite persisted in the `viewer-data` Docker volume
- viewer served on port `8000`

The Docker build now compiles Tailwind inside the image, so the host does not need Node for the container path.

If you want host-visible SQLite files instead of a named volume, replace:

```yaml
volumes:
  - viewer-data:/app/data
```

with:

```yaml
volumes:
  - ./data:/app/data
```

If you want local import inside Docker instead of remote uploads, override the sync mode and mount your session directory:

```yaml
services:
  viewer:
    environment:
      CODEX_VIEWER_SYNC_MODE: local
      CODEX_SESSION_ROOTS: /sessions
    volumes:
      - viewer-data:/app/data
      - /home/you/.codex/sessions:/sessions:ro
```

## Systemd

Systemd examples live in [deploy/systemd](deploy/systemd).

Wrapper scripts:

- [scripts/start-server.sh](scripts/start-server.sh)
- [scripts/start-agent-daemon.sh](scripts/start-agent-daemon.sh)
- [scripts/bootstrap-local.sh](scripts/bootstrap-local.sh)

## Testing

The project uses browser-level end-to-end tests for product flows rather than unit tests.

Install the browser runner:

```bash
npm install
npm run test:e2e:install
```

Run the suite:

```bash
npm run test:e2e
```

The E2E harness starts the real FastAPI app against a temporary SQLite data directory for each test, then drives onboarding, login, dashboard, project, session, queue, and machines flows through Playwright.

## Backup And Restore

The supported lightweight backup boundary is:

- `CODEX_VIEWER_DATA_DIR`
- the SQLite database file
- raw session artifacts stored under `data/session_artifacts`
- the generated browser session secret in `data/.session-secret`

What this does not try to do yet:

- project-level export/import
- selective restore
- archive retention policies
- in-app project archive lifecycle

Recommended workflow:

1. Create a backup archive with `backup create`.
2. Verify it with `backup verify`.
3. Restore it into a fresh directory with `backup restore --data-dir ...`.
4. Start the viewer against the restored directory.

`backup restore` is intentionally offline and conservative. It restores into a new or empty target directory only; it does not merge into an existing install.

If you use the default layout, the restored instance can usually be started by pointing `CODEX_VIEWER_DATA_DIR` at the restored directory. If you run SQLite outside the data directory with `CODEX_VIEWER_DB`, restore it with `--database-path` and reuse that setting when you start the restored server.
