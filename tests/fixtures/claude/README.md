Claude fixture data for importer tests.

Provenance:
- `projects/nested-skill/*` is adapted from inline test data in
  `https://github.com/delexw/claude-code-trace/blob/main/src-tauri/src/parser/subagent.rs`
- `projects/persisted-output/*` follows the persisted-output wrapper pattern
  exercised in `https://github.com/delexw/claude-code-trace/blob/main/src-tauri/src/parser/sanitize.rs`
- `projects/public-streaming/*` is a sanitized adaptation of Simon Willison's
  public Claude session gist:
  `https://gist.github.com/simonw/bfe117b6007b9d7dfc5a81e4b2fd3d9a`
- `projects/warmup-issue/*` is a sanitized adaptation of the warmup-sidechain
  repro in Anthropic issue `#9668`:
  `https://github.com/anthropics/claude-code/issues/9668`

These fixtures are intentionally small and sanitized. They preserve the
directory layout Claude uses under `~/.claude/projects/` so sync/import tests can
exercise path-based sidechain detection and persisted tool-result resolution.
