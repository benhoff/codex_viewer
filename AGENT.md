# Access Policy

This document describes the current access model for the Codex Session Viewer.

It reflects the behavior implemented in:

- `codex_session_viewer/web/auth.py`
- `codex_session_viewer/local_auth.py`
- `codex_session_viewer/projects.py`
- `codex_session_viewer/saved_turns.py`
- `codex_session_viewer/web/routes/pages.py`
- `codex_session_viewer/web/routes/projects.py`
- `codex_session_viewer/web/routes/sessions.py`

## Policy Goals

The app uses a two-layer access model:

1. Install-wide roles determine who can change shared system state.
2. Per-project ACLs determine who can read restricted projects.

The model is intentionally simple:

- global roles: `admin`, `viewer`
- project visibility: `authenticated`, `private`
- project membership roles: `viewer`, `editor`

Global admins always bypass project ACL checks.

## Authentication Sources

The app supports:

- local password auth
- trusted proxy / SSO auth
- no auth

### Local password users

Local users are persisted in `users` and have a stable `user_id`.

### Proxy users

Proxy users are identities supplied by a trusted reverse proxy via headers such as:

- `X-Forwarded-User`
- `X-Forwarded-Name`
- `X-Forwarded-Email`

Proxy users are auto-provisioned into `users` and assigned a stable `user_id`.
They default to role `viewer`.

The app does not grant admin from proxy headers alone. Admin elevation is an explicit app-side action.

### Auth disabled

When auth is disabled, the app behaves as a single local operator install. Shared state is effectively admin-accessible.

## Global Roles

### `admin`

Admins can:

- view all normal app surfaces
- manage onboarding and bootstrap
- manage server settings
- create and revoke sync API tokens
- manage users and roles
- manage project ACL and visibility
- perform machine-level admin actions
- edit or delete project metadata

Admins bypass project ACL visibility checks.

### `viewer`

Viewers can:

- view dashboards, projects, sessions, exports, streams, environment audits, and machines
- use their own review queue
- change their own local password if they are a local password user

Viewers cannot:

- change install-wide settings
- manage tokens
- manage users
- manage project ACL
- perform machine admin actions
- edit or delete project metadata

## Project Access Control

Per-project ACL is enforced using a stable `project_id`, not the mutable display or grouping key.

### Visibility

Each project has one visibility mode:

- `authenticated`
- `private`

#### `authenticated`

Any authenticated user can read the project.

If auth is disabled, the project is readable in the local single-operator mode.

#### `private`

Only these users can read the project:

- global admins
- users explicitly granted project membership

Private projects should not appear in filtered dashboard, search, session, queue, or machine/project summary surfaces for unauthorized users.

### Project membership roles

Project ACL entries store one of:

- `viewer`
- `editor`

#### Project `viewer`

Can read that private project and its derived surfaces.

#### Project `editor`

Currently stored for future project-scoped write access, but not broadly used yet.

Important v1 limitation:

- boundary-changing project operations remain admin-only

This includes operations that can merge, split, or redefine ACL domains, such as regrouping project sources or changing canonical project identity.

In practice today, `editor` is persisted and manageable, but shared project mutation still requires global admin.

## Authorization Rules

### Install-wide admin checks

Shared-state mutation requires global admin.

Examples:

- onboarding completion actions
- token management
- server settings changes
- machine actions
- project ACL changes
- project edit/delete actions

### Project read checks

The effective rule is:

1. if auth is disabled, allow
2. if the user is a global admin, allow
3. if the project visibility is not `private`, allow
4. otherwise require explicit membership in `project_acl`

### Project write checks

Current v1 rule:

- project-boundary writes are admin-only

Even if a user has project role `editor`, the app does not yet delegate regrouping or canonical identity changes to them.

## Personal State

The review queue is personal when auth is enabled.

Owner scope rules:

- when auth is enabled, queue items are scoped to the authenticated `user_id`
- when auth is disabled, queue items use the global scope `__global__`

The app should not fall back to shared global queue ownership when auth is enabled.

Users can only queue sessions they can read.

Project ACL filtering also applies when showing:

- queue counts
- queue lists
- project-scoped queue views

## Surfaces Filtered by Project Visibility

Project ACL is intended to affect both detail pages and global summary surfaces.

This includes:

- dashboard / index
- search
- project pages
- project stream
- project environment audit
- session detail pages
- session exports
- queue counts and queue lists
- machine summaries that include recent repo or session links

The goal is that unauthorized users should not learn private project names or session links from summary pages.

## Machines

The app still exposes install-level machine health and audit surfaces, but project-derived content shown there should respect project visibility filtering.

Global machine administration remains admin-only.

## User Management

User identities are persisted in `users`.

Relevant fields include:

- `role`
- `auth_source`
- `external_subject`
- `display_name`
- `email`
- `disabled_at`
- `last_seen_at`

Only admins can:

- promote or demote users
- disable or re-enable users
- manage project memberships

Proxy users are auto-created as `viewer`.

## Bootstrap / First Admin

Bootstrap is install-level, not project-level.

Rules:

- the system requires at least one admin when auth is enabled
- password installs can create the first admin directly
- proxy installs can claim the currently authenticated proxy identity as the first admin

Once bootstrap is complete, normal viewers should not gain admin powers through setup routes.

## Non-Goals and Current Limits

The current access model does not yet include:

- per-project admin roles
- per-project token ownership
- per-project machine ownership
- shared queue or annotation workflows
- fine-grained permissions beyond global `admin` / `viewer`
- broad editor write powers for project-regrouping operations

This is deliberate. The current policy favors predictable enforcement over partial delegation.

## Decision Summary

Use this mental model:

- `admin` controls the install
- `viewer` can read and manage only personal state
- `private` projects require explicit membership
- project `editor` exists, but admin still owns ACL-boundary-changing project operations in v1

If future work expands project-scoped writes, this file should be updated alongside the enforcement code.
