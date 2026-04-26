# Action Queue Scoring

This note defines the homepage scoring rule for the action queue that replaces the older broad "attention sessions" feed.

## Why This Exists

The current homepage session-attention logic is anomaly-oriented:

- any usage pressure counts
- any non-zero command exits count
- any aborted turns count
- any import warning counts

That older rule is implemented through [build_error_sessions_panel](../agent_operations_viewer/web/routes/pages.py), [summarize_attention_status](../agent_operations_viewer/projects.py), and [usage_pressure_snapshot](../agent_operations_viewer/session_insights.py).

It is useful for surfacing odd sessions, but too noisy for a primary triage queue.

## Validation Summary

Validation used two sources:

1. The actual `../codex` repository workflow surface.
2. Analogous imported session data from this viewer's local SQLite database.

There are currently no imported sessions for `/home/wulfuser/codex` or `openai/codex` in the local database, so the validation against `../codex` is workflow-based rather than session-based.

There is still a second, stronger form of validation available from `../codex`: it is the codebase that produces the rollout/session artifacts the viewer ingests. That means we can validate not just "what counts as risky work in the repo", but also "what structured evidence Codex actually records in the session data".

That still provides a strong check because `../codex` has multiple clear verification and regeneration obligations:

- Rust crate changes are expected to run targeted tests such as `cargo test -p <crate>`, and sometimes a broader workspace test, per `../codex/AGENTS.md`.
- TUI changes require snapshot coverage and snapshot review.
- `ConfigToml` changes require `just write-config-schema`.
- app-server protocol changes require `just write-app-server-schema`.
- dependency changes require `just bazel-lock-update` and `just bazel-lock-check`.
- user-facing API or behavior changes are expected to update docs in `docs/`.

That repo is therefore a good stress test for whether a signal is genuinely actionable. In `../codex`, the meaningful failures are not "a command exited non-zero". The meaningful failures are:

- verification failed for a changed area
- required generated artifacts were not updated
- risky code changed without verification
- the environment is missing prerequisites needed to run the expected verification

Those are exactly the signals the action queue should prioritize.

The analogous imported sessions in this viewer's database support the same conclusion:

- a session with repeated `rg` no-match exits and an import warning is noisy
- a session with missing `uv`, missing Python packages, and failed startup commands is actionable
- a session with only rate-window pressure is telemetry, not a user task

## Additional Producer-Side Insights From `../codex`

Looking at the producer code in `../codex/codex-rs/protocol` and `../codex/codex-rs/rollout`, Codex already records much richer evidence than the homepage currently uses.

Important examples:

- command lifecycle events carry `turn_id`, `duration`, `status`, `cwd`, and `parsed_cmd`
- patch lifecycle events carry explicit structured file changes plus `status`
- approval requests carry reasons, proposed policy amendments, and additional requested permissions
- guardian reviews carry `risk_level`, `user_authorization`, `rationale`, and the exact reviewed action
- turn control events distinguish `TurnComplete` from `TurnAborted`, and `TurnAborted` includes an explicit reason
- context-management events exist explicitly as `ContextCompacted` and `ThreadRolledBack`
- session/turn context captures approval policy, sandbox policy, network allow/deny context, collaboration mode, model, truncation policy, and reasoning settings
- MCP startup completion records which servers failed and why
- model routing changes are explicit (`ModelReroute`)

This matters because it means the viewer does not need to infer everything from raw shell text.

### High-Value Signals The Viewer Is Not Fully Exploiting Yet

1. Parsed command semantics
   - Codex emits `ParsedCommand` with `Read`, `ListFiles`, `Search`, and `Unknown`.
   - This is better than regexing shell text.
   - It lets the queue suppress search/probe failures with much higher confidence.

2. Declined vs failed vs blocked
   - Commands and patch applications both have explicit statuses such as `Completed`, `Failed`, and `Declined`.
   - This lets the viewer distinguish:
     - true execution failures
     - user/policy declines
     - work that never ran because approval was denied

3. Approval friction
   - `ExecApprovalRequest`, `ApplyPatchApprovalRequest`, and `RequestPermissions` events expose exactly what extra access the agent needed.
   - This is a separate class of actionable issue from "the code is wrong".
   - A user can often resolve it by granting or adjusting policy, not by debugging the repo.

4. Guardian-assessed risk
   - `GuardianAssessmentEvent` already contains structured risk and authorization judgments.
   - This is a much stronger correctness/safety signal than plain command exits.

5. Explicit context management
   - Codex emits `ContextCompacted` and `ThreadRolledBack`.
   - That confirms usage pressure by itself should stay out of the action queue.
   - If anything, the action-worthy case is repeated compaction or rollback followed by incomplete work.

6. Abort semantics
   - `TurnAborted.reason` distinguishes `interrupted`, `replaced`, and `review_ended`.
   - These should not all score the same way.
   - `interrupted after patch` is actionable; `replaced` often is not.

7. MCP and tooling startup failures
   - `McpStartupComplete.failed` and related startup updates are explicit environment/setup issues.
   - These are actionable and currently underrepresented in homepage triage.

## Scoring Adjustments From Producer-Side Validation

Using the producer schema from `../codex`, the scoring rule should be tightened in a few places.

### New Or Stronger Positive Signals

- `+50` guardian review denied or timed out for a high/critical-risk action
- `+35` command or patch blocked by missing permissions or approval friction with a clear remediation path
- `+30` MCP startup failure for a tool/server the session attempted to use
- `+15` repeated context rollback/compaction followed by incomplete or aborted work

### Better Suppression Rules

- `-100` search/list/read command failures when `parsed_cmd` classifies them as exploratory and there are no later stronger signals
- `-30` command or patch `declined` by user/policy when no code was changed and the session recovered cleanly
- `-20` `TurnAborted(reason=replaced)` unless there were patches or failed verification in the same turn

### Better Root-Cause Fingerprints

The normalized signature should prefer producer-native structure when available:

- `parsed_cmd` category plus normalized target
- approval request type plus requested permission shape
- guardian assessment action type plus target
- patch status plus changed path set
- MCP server name for startup failures
- turn abort reason

Examples:

- `approval_blocked:filesystem:/outside/workspace`
- `guardian_denied:apply_patch:/etc/systemd/system`
- `mcp_startup_failed:github`
- `turn_aborted:interrupted_after_patch`
- `search_no_match:rg:/workspace/src`

## Design Goal

The homepage should show a short queue of unresolved, high-confidence, user-actionable items.

An item belongs in the queue only if all of these are true:

1. It is recent enough to matter now, or it is the latest unresolved issue for that repo and host.
2. It has a high-confidence signal that implies correctness risk, blocked progress, or incomplete work.
3. It is not already superseded by later successful verification.
4. The UI can tell the user what to do next.

## Existing Inputs To Reuse

The viewer already computes stronger audit signals than the current homepage queue uses:

- [verification_verdict](../agent_operations_viewer/session_view.py)
- [build_trust_signals](../agent_operations_viewer/session_view.py)
- [build_session_audit_summary](../agent_operations_viewer/session_view.py)

Those signals should become the backbone of the action queue.

The `../codex` producer validation adds an important refinement: when producer-native structured fields exist, they should take precedence over viewer-side heuristics.

## Queue Eligibility Gate

Before scoring, reject anything that fails the gate below.

### Include

- Sessions with failed verification after patches or file changes.
- Sessions with claim/evidence mismatches.
- Sessions with risky file changes and no verification.
- Sessions with repeated environment/setup failures that blocked expected verification.
- Sessions with aborted turns after patches or after a failed verification attempt.
- Sessions with approval or permission blockers that prevented intended work from running.
- Sessions with guardian-denied or guardian-timed-out risky actions.
- Sessions with MCP startup failures that blocked the requested workflow.

### Exclude Or Demote

- Import-warning-only sessions.
- Usage-pressure-only sessions.
- Sessions whose only failures are search or probe misses such as `rg`, `grep`, `find`, or similar exploratory commands.
- Sessions whose only failures are exploratory commands as classified by producer-native `parsed_cmd`.
- Old sessions that have a newer successful follow-up on the same repo and host.

## Root Cause Fingerprint

The homepage should deduplicate by root cause, not by session id.

Recommended fingerprint:

```text
(repo_key, host, issue_kind, normalized_signature)
```

Where `normalized_signature` is a stable summary such as:

- `verification_failed:cargo test -p codex-tui`
- `missing_binary:uv`
- `missing_module:fastapi`
- `generated_artifact_missing:config.schema.json`
- `snapshot_review_needed:codex-tui`

This lets repeated failures collapse into one queue item with a stronger score instead of flooding the homepage.

## Proposed Scoring Function

Only score sessions that pass the eligibility gate.

```text
score =
  severity_points
  + recency_points
  + repo_risk_points
  + repetition_points
  - noise_penalty

show_on_homepage = score >= 40
score is clamped to [0, 100]
```

### Severity Points

- `+45` verification failed after file changes or patches
- `+40` claim/evidence mismatch
- `+35` risky file changes with no verification
- `+25` repeated environment/setup blocker preventing expected verification
- `+20` aborted after patch or after failed verification
- `+35` approval or permissions blocker with a clear remediation path
- `+50` guardian denied/timed out on a high-risk action
- `+30` MCP startup failure for a needed server/tool
- `+10` verification passed with warnings on a risky scope
- `+0` raw command exits with no stronger signal

### Recency Points

- `+20` latest event within 24 hours
- `+10` latest event within 7 days
- `+5` older than 7 days but still the latest unresolved fingerprint for that repo and host
- `+0` otherwise

### Repo Risk Points

These should be derived from touched-file kinds, not from command text.

- `+20` shared or high-blast-radius surfaces
  - examples in `../codex`: `codex-rs/core/`, `codex-rs/protocol/`, `codex-rs/config/`, `codex-rs/app-server-protocol/`, `Cargo.toml`, `Cargo.lock`, `MODULE.bazel.lock`, sandbox/auth/security-sensitive code
- `+10` user-visible product surfaces
  - examples in `../codex`: `codex-rs/tui/`, `codex-rs/cli/`, SDK public API, app-server behavior
- `+5` localized implementation code with limited blast radius
- `+0` docs-only or comments-only changes

### Repetition Points

- `+5` for each additional matching fingerprint in the last 24 hours
- cap repetition bonus at `+15`

This upgrades a flaky or persistent blocker without letting it dominate forever.

### Noise Penalty

- `-100` import-warning-only
- `-100` usage-pressure-only
- `-20` only search/probe failures and no patches
- `-10` only aborted turns and no patches, no verification, and no risky files
- `-100` only exploratory `parsed_cmd` failures and no stronger signal

## Interpretation Bands

- `85-100`: critical, likely blocking or correctness-threatening
- `65-84`: high priority, should be triaged soon
- `40-64`: medium priority, valid queue item
- `0-39`: do not show on homepage; keep visible only in session detail or telemetry views

## Next-Action Mapping

Every action-queue item should carry a concrete next step.

- `verification_failed`
  - next action: open the failing verification step and rerun or fix it
- `claim_evidence_mismatch`
  - next action: review the turn response against recorded commands and verification
- `risky_changes_unverified`
  - next action: run targeted verification for the touched area
- `env_setup_blocker`
  - next action: install the missing dependency or bootstrap the environment, then rerun verification
- `aborted_after_patch`
  - next action: reopen the session near the last patched turn and complete verification

This is the main difference between an action queue and an anomaly feed.

## Why This Validates Well Against `../codex`

The `../codex` repo has explicit workflow requirements that make path-aware scoring valuable:

- touching TUI code without snapshot-aware verification is genuinely risky
- touching shared crates without targeted tests is risky
- touching protocol or config code without regenerating schema artifacts is risky
- touching dependency files without lockfile follow-through is risky
- touching docs alone is usually lower risk

So the implemented scoring rule lines up with real developer obligations in that repo:

- it promotes failures with a clear required follow-up
- it suppresses telemetry and exploratory command noise
- it becomes more valuable on complex repos because repo-specific verification obligations are clear

## What This Would Hide

These should not be homepage queue items:

- `rg` returned exit `1` because no match was found
- `ffmpeg | rg` returned exit `1` because no marker matched
- context pressure is high but there is no failed work
- the importer truncated search text but the session itself completed normally

Those are still useful signals, but they belong in telemetry or session detail views.

## Recommended Implementation Order

Implemented:

1. Add a root-cause fingerprint and dedupe queue items by fingerprint.
2. Replace homepage session attention with the eligibility gate above.
3. Score only surviving items with this function.
4. Add `resolve`, `snooze`, and `ignore fingerprint` controls.
5. Auto-clear verification-backed queue items when later successful verification is recorded for the same repo/host.

Remaining follow-up:

1. Move import warnings and usage pressure into a separate telemetry panel.
