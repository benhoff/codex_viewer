const { test, expect } = require("../helpers/fixtures");
const { login } = require("../helpers/auth");
const { expectPageLoad } = require("../helpers/assertions");

function syncHeaders(rawToken, sourceHost) {
  return {
    authorization: `Bearer ${rawToken}`,
    "x-codex-viewer-host": sourceHost,
  };
}

test("sync api heartbeat and session ingestion surface in the UI", async ({ page, request, app, seed }) => {
  await seed.createAdmin({
    username: "admin",
    password: "Password123!",
  });
  const apiToken = await seed.createToken({
    label: "Sync ingest E2E token",
  });

  const sourceHost = "sync-builder-1";
  const projectKey = "acme/sync-app";
  const projectLabel = "acme/sync-app";
  const searchQuery = "synthetic turn 4";
  const sessionPayload = await seed.buildSessionPayload({
    sourceHost,
    projectKey,
    projectLabel,
    githubOrg: "acme",
    githubRepo: "sync-app",
    turns: 4,
    commandsPerTurn: 2,
    sessionIndex: 1,
  });
  const headers = syncHeaders(apiToken.token, sourceHost);

  const healthResponse = await request.get(app.url("/api/health"));
  expect(healthResponse.ok()).toBeTruthy();
  const health = await healthResponse.json();

  await test.step("sync manifest starts empty for the host", async () => {
    const manifestResponse = await request.get(
      app.url(`/api/sync/manifest?host=${encodeURIComponent(sourceHost)}`),
      { headers },
    );

    expect(manifestResponse.ok()).toBeTruthy();
    const manifest = await manifestResponse.json();
    expect(manifest.host).toBe(sourceHost);
    expect(manifest.sessions).toEqual([]);
    expect(manifest.server.sync_api_version).toBe(health.sync_api_version);
  });

  await test.step("heartbeat and session payloads are accepted", async () => {
    const heartbeatPayload = {
      source_host: sourceHost,
      agent_version: health.expected_agent_version,
      sync_api_version: health.sync_api_version,
      sync_mode: "remote",
      update_state: "current",
      server_version_seen: health.app_version,
      server_api_version_seen: health.sync_api_version,
      last_seen_at: sessionPayload.session.imported_at,
      last_sync_at: sessionPayload.session.imported_at,
      last_upload_count: 1,
      last_skip_count: 0,
      last_fail_count: 0,
    };
    const heartbeatResponse = await request.post(app.url("/api/sync/heartbeat"), {
      headers,
      data: heartbeatPayload,
    });
    expect(heartbeatResponse.ok()).toBeTruthy();
    const heartbeatResult = await heartbeatResponse.json();
    expect(heartbeatResult).toMatchObject({
      status: "ok",
      source_host: sourceHost,
    });

    const sessionResponse = await request.post(app.url("/api/sync/session"), {
      headers,
      data: sessionPayload,
    });
    expect(sessionResponse.ok()).toBeTruthy();
    const sessionResult = await sessionResponse.json();
    expect(sessionResult).toMatchObject({
      status: "ok",
      session_id: sessionPayload.session.id,
      source_host: sourceHost,
      event_count: sessionPayload.session.event_count,
      content_sha256: sessionPayload.session.content_sha256,
    });
  });

  await test.step("sync manifest reflects the imported session", async () => {
    const manifestResponse = await request.get(
      app.url(`/api/sync/manifest?host=${encodeURIComponent(sourceHost)}`),
      { headers },
    );

    expect(manifestResponse.ok()).toBeTruthy();
    const manifest = await manifestResponse.json();
    expect(manifest.sessions).toHaveLength(1);
    expect(manifest.sessions[0]).toMatchObject({
      id: sessionPayload.session.id,
      source_host: sourceHost,
      source_path: sessionPayload.session.source_path,
      source_root: sessionPayload.session.source_root,
      file_size: sessionPayload.session.file_size,
      file_mtime_ns: sessionPayload.session.file_mtime_ns,
      content_sha256: sessionPayload.session.content_sha256,
      event_count: sessionPayload.session.event_count,
      stored_event_count: sessionPayload.session.event_count,
      has_raw_artifact: 0,
    });
  });

  await login(page, app, {
    username: "admin",
    password: "Password123!",
  });

  await test.step("ingested data appears across the browser surfaces", async () => {
    await expectPageLoad(page, app.url("/"), {
      expectedPathname: "/",
      expectedText: "Active Repos",
    });
    await expect(page.locator("body")).toContainText(projectLabel);
    await expect(page.locator("body")).toContainText(sourceHost);

    await expectPageLoad(page, app.url("/machines"), {
      expectedPathname: "/machines",
      expectedText: "Machines",
    });
    await expect(page.locator("body")).toContainText(sourceHost);
    await expect(page.locator("body")).toContainText("1 up / 0 skip / 0 fail");

    await expectPageLoad(page, app.url(`/search?q=${encodeURIComponent(searchQuery)}`), {
      expectedPathname: "/search",
      expectedText: "Turn Hits",
    });
    await expect(page.getByRole("link", { name: projectLabel }).first()).toBeVisible();

    await expectPageLoad(page, app.url(`/${projectKey}`), {
      expectedPathname: `/${projectKey}`,
      expectedText: projectLabel,
    });
    await expect(page.locator("body")).toContainText(sourceHost);

    await expectPageLoad(page, app.url(`/sessions/${sessionPayload.session.id}`), {
      expectedPathname: `/sessions/${sessionPayload.session.id}`,
      expectedText: "Conversation",
    });
    await expect(page.locator("body")).toContainText("Investigate synthetic turn 1");
    await expect(page.locator("body")).toContainText("Completed synthetic turn 4");
  });
});
