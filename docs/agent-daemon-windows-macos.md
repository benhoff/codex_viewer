# Agent Daemon On macOS And Windows

This guide is for remote-agent hosts that upload sessions to a central viewer.
If you are running the viewer in `local` mode on the same machine, you do not
need the daemon.

## What The Daemon Needs

- A checkout of this repo on the agent machine
- Python 3 on `PATH`
- A sync token created from the viewer server
- A reachable `CODEX_VIEWER_SERVER_URL`

The daemon reads settings from `.env`, `.env.local`, and the environment, just
like the server.

## Common Settings

The minimum `.env` for a remote host is:

```env
CODEX_VIEWER_SERVER_URL=http://viewer.example.com:8000
CODEX_VIEWER_SYNC_API_TOKEN=replace-with-token
CODEX_VIEWER_SYNC_MODE=remote
```

Optional but commonly useful:

```env
CODEX_VIEWER_SOURCE_HOST=my-laptop
CODEX_VIEWER_SYNC_INTERVAL=30
```

If the machine only has Codex sessions, you can usually leave
`CODEX_SESSION_ROOTS` unset and use the default:

- macOS: `~/.codex/sessions`
- Windows: `%USERPROFILE%\.codex\sessions`

If the machine has both Codex and Claude sessions:

```env
CODEX_SESSION_ROOTS=~/.codex/sessions,~/.claude/projects
```

On Windows `.env` files do not expand PowerShell variables. Use a literal path
if you set `CODEX_SESSION_ROOTS` explicitly, for example:

```env
CODEX_SESSION_ROOTS=C:/Users/YOU/.codex/sessions,C:/Users/YOU/.claude/projects
```

## macOS Quick Start

From the repo root:

```bash
cat > .env <<EOF
CODEX_VIEWER_SERVER_URL=http://viewer.example.com:8000
CODEX_VIEWER_SYNC_API_TOKEN=replace-with-token
CODEX_VIEWER_SYNC_MODE=remote
CODEX_VIEWER_SOURCE_HOST=$(hostname)
# Optional if this machine also stores Claude sessions:
# CODEX_SESSION_ROOTS=$HOME/.codex/sessions,$HOME/.claude/projects
EOF

./scripts/bootstrap-local.sh --skip-css
./scripts/start-agent-daemon.sh
```

The `--skip-css` flag is recommended on agent hosts because they do not serve
the web UI.

## macOS Auto-Start With launchd

Example `~/Library/LaunchAgents/dev.codex-viewer.agent.plist`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
  <dict>
    <key>Label</key>
    <string>dev.codex-viewer.agent</string>

    <key>WorkingDirectory</key>
    <string>/Users/YOU/src/codex_viewer</string>

    <key>ProgramArguments</key>
    <array>
      <string>/bin/bash</string>
      <string>/Users/YOU/src/codex_viewer/scripts/start-agent-daemon.sh</string>
    </array>

    <key>RunAtLoad</key>
    <true/>

    <key>KeepAlive</key>
    <true/>

    <key>StandardOutPath</key>
    <string>/Users/YOU/Library/Logs/codex-viewer-agent.log</string>

    <key>StandardErrorPath</key>
    <string>/Users/YOU/Library/Logs/codex-viewer-agent.log</string>
  </dict>
</plist>
```

Load it:

```bash
launchctl load ~/Library/LaunchAgents/dev.codex-viewer.agent.plist
launchctl start dev.codex-viewer.agent
```

Unload it:

```bash
launchctl unload ~/Library/LaunchAgents/dev.codex-viewer.agent.plist
```

## Windows PowerShell Quick Start

From the repo root in PowerShell:

```powershell
@"
CODEX_VIEWER_SERVER_URL=http://viewer.example.com:8000
CODEX_VIEWER_SYNC_API_TOKEN=replace-with-token
CODEX_VIEWER_SYNC_MODE=remote
CODEX_VIEWER_SOURCE_HOST=$env:COMPUTERNAME
# Optional if this machine also stores Claude sessions:
# CODEX_SESSION_ROOTS=C:/Users/YOU/.codex/sessions,C:/Users/YOU/.claude/projects
"@ | Set-Content -Encoding Ascii .env

.\scripts\bootstrap-local.ps1 -SkipCss
.\scripts\start-agent-daemon.ps1
```

If PowerShell blocks local scripts, allow them for the current shell only:

```powershell
Set-ExecutionPolicy -Scope Process Bypass
```

Then rerun the bootstrap and daemon commands.

## Windows Auto-Start With Task Scheduler

Create a per-user logon task:

```powershell
schtasks /Create `
  /SC ONLOGON `
  /TN "Codex Viewer Agent" `
  /TR "powershell.exe -NoProfile -ExecutionPolicy Bypass -File `"%USERPROFILE%\\src\\codex_viewer\\scripts\\start-agent-daemon.ps1`"" `
  /F
```

Run it immediately:

```powershell
schtasks /Run /TN "Codex Viewer Agent"
```

Delete it:

```powershell
schtasks /Delete /TN "Codex Viewer Agent" /F
```

## Troubleshooting

- If the daemon reaches the server but imports nothing, check
  `CODEX_SESSION_ROOTS` on that machine.
- If the viewer URL in `/setup` shows `127.0.0.1` or `localhost`, replace it
  with a network-reachable hostname before launching a remote machine.
- On Windows, prefer `C:/...` or `C:\...` literal paths in `.env`.
- On agent hosts, use the daemon wrappers instead of running `python -m ...`
  directly. The wrappers set `PYTHONPATH` and force `CODEX_VIEWER_SYNC_MODE=remote`.
