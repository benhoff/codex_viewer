# Self-Hosted Launch Priorities

This project is closest to something `r/selfhosted` would care about when it looks like infrastructure, not just an AI demo. The winning angle is:

> self-hosted observability and audit UI for coding agents, with simple deployment, local data ownership, and clear recovery paths

## Priority Order

1. Easy Docker Compose deployment
   - One command to start the server.
   - Persistent data volume for SQLite.
   - Clear upgrade path and documented env vars.
   - Optional bind-mount examples for people who want transparent host-side backups.

2. Backup and restore workflow
   - Document what to back up.
   - Document how to restore the SQLite database and config.
   - Add an export/import story for sessions or projects.
   - Treat "tested restore" as a feature, not a footnote.

3. Privacy-first, local-first story
   - Server owns all imported session data.
   - Remote agents push into the server; no SaaS dependency is required.
   - Keep the product pitch centered on visibility, auditing, and control.

4. Strong audit trail for agent activity
   - Clear rendering of prompts, final responses, commands, patches, and plan updates.
   - Host-aware session grouping and repo-aware views.
   - Timeline should help a user answer "what changed, where, and why?"

5. Lightweight auth, with optional SSO
   - Simple built-in auth or token-based access for small installs.
   - Optional OIDC/Auth proxy integration for larger homelabs.
   - Do not require a heavy identity product for basic use.

6. Agent health and notifications
   - Show stale remotes, failed syncs, version drift, and protocol mismatch clearly.
   - Add webhook/ntfy/Apprise-style notifications for broken agents and failing uploads.
   - Make the system feel "set it and forget it" once deployed.

7. Better dashboarding
   - Keep the index page useful as a control surface.
   - Highlight hottest repos, broken remotes, recent activity, and turn/session totals.
   - Lean into the fact that self-hosters like dashboards when they are operationally useful.

8. Git-aware safety signals
   - Show branch and commit context when available.
   - Flag risky commands, infra-file changes, or root-adjacent edits.
   - Make repo views answer "is this safe?" faster.

9. Export/archive features
   - Project-level export bundles.
   - Session archive/retention controls.
   - Import path for restores or migrations.

10. Optional local-model ecosystem hooks
   - Make it easy to use with locally run agents or local model workflows.
   - Keep this optional and secondary to deployment, privacy, and auditability.

## What To Avoid Leading With

- Autonomous "AI manages your server" marketing.
- Anything that requires a cloud account.
- Heavy enterprise workflow before deploy/backups/auth are solid.

## Good Near-Term Roadmap

1. Ship Docker Compose and backup docs.
2. Add basic notifications for stale or failing remotes.
3. Add lightweight auth and optional reverse-proxy/OIDC guidance.
4. Improve repo/session audit views and export bundles.
5. Add safety signals around commands and patches.

