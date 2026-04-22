const { test, expect } = require("../helpers/fixtures");
const { login } = require("../helpers/auth");
const { expectPageLoad } = require("../helpers/assertions");

function syncHeaders(rawToken, sourceHost) {
  return {
    authorization: `Bearer ${rawToken}`,
    "x-codex-viewer-host": sourceHost,
  };
}

function manifestBySessionId(manifest) {
  return Object.fromEntries(
    manifest.sessions.map((session) => [session.id, session]),
  );
}

test("raw sync batch uploads persist raw artifacts and avoid duplicate sessions on replay", async ({
  page,
  request,
  app,
  seed,
}) => {
  await seed.createAdmin({
    username: "admin",
    password: "Password123!",
  });
  const apiToken = await seed.createToken({
    label: "Raw sync batch E2E token",
  });

  const sourceHost = "raw-batch-builder-1";
  const projectLabel = "acme/raw-batch-app";
  const detailPath = "/acme/raw-batch-app";
  const rawSessionOne = await seed.buildRawSessionPayload({
    sourceHost,
    projectKey: projectLabel,
    projectLabel,
    githubOrg: "acme",
    githubRepo: "raw-batch-app",
    sessionIndex: 1,
    userMessage: "Investigate raw batch sync session one.",
    assistantMessage: "Completed raw batch sync session one.",
  });
  const rawSessionTwo = await seed.buildRawSessionPayload({
    sourceHost,
    projectKey: projectLabel,
    projectLabel,
    githubOrg: "acme",
    githubRepo: "raw-batch-app",
    sessionIndex: 2,
    userMessage: "Investigate raw batch sync session two.",
    assistantMessage: "Completed raw batch sync session two.",
  });
  const headers = syncHeaders(apiToken.token, sourceHost);

  const healthResponse = await request.get(app.url("/api/health"));
  expect(healthResponse.ok()).toBeTruthy();
  const health = await healthResponse.json();

  await test.step("raw sync manifest starts empty", async () => {
    const manifestResponse = await request.get(
      app.url(`/api/sync/manifest?host=${encodeURIComponent(sourceHost)}`),
      { headers },
    );
    expect(manifestResponse.ok()).toBeTruthy();

    const manifest = await manifestResponse.json();
    expect(manifest.host).toBe(sourceHost);
    expect(manifest.sessions).toEqual([]);
  });

  await test.step("heartbeat and raw batch upload succeed", async () => {
    const heartbeatResponse = await request.post(app.url("/api/sync/heartbeat"), {
      headers,
      data: {
        source_host: sourceHost,
        agent_version: health.expected_agent_version,
        sync_api_version: health.sync_api_version,
        sync_mode: "remote",
        update_state: "current",
        server_version_seen: health.app_version,
        server_api_version_seen: health.sync_api_version,
        last_upload_count: 0,
        last_skip_count: 0,
        last_fail_count: 0,
      },
    });
    expect(heartbeatResponse.ok()).toBeTruthy();

    const batchResponse = await request.post(app.url("/api/sync/sessions-raw"), {
      headers,
      data: {
        sessions: [
          rawSessionOne.payload,
          rawSessionTwo.payload,
        ],
      },
    });
    expect(batchResponse.ok()).toBeTruthy();

    const batchResult = await batchResponse.json();
    expect(batchResult).toMatchObject({
      status: "ok",
      mode: "raw_batch",
      processed_count: 2,
    });
    expect(batchResult.results).toHaveLength(2);
    expect(batchResult.results).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          status: "ok",
          session_id: rawSessionOne.expected.session_id,
          source_host: sourceHost,
          event_count: rawSessionOne.expected.event_count,
          content_sha256: rawSessionOne.expected.content_sha256,
        }),
        expect.objectContaining({
          status: "ok",
          session_id: rawSessionTwo.expected.session_id,
          source_host: sourceHost,
          event_count: rawSessionTwo.expected.event_count,
          content_sha256: rawSessionTwo.expected.content_sha256,
        }),
      ]),
    );

    const postSyncHeartbeatResponse = await request.post(app.url("/api/sync/heartbeat"), {
      headers,
      data: {
        source_host: sourceHost,
        agent_version: health.expected_agent_version,
        sync_api_version: health.sync_api_version,
        sync_mode: "remote",
        update_state: "current",
        server_version_seen: health.app_version,
        server_api_version_seen: health.sync_api_version,
        last_upload_count: 2,
        last_skip_count: 0,
        last_fail_count: 0,
      },
    });
    expect(postSyncHeartbeatResponse.ok()).toBeTruthy();
  });

  await test.step("manifest reports raw artifacts for both uploaded sessions", async () => {
    const manifestResponse = await request.get(
      app.url(`/api/sync/manifest?host=${encodeURIComponent(sourceHost)}`),
      { headers },
    );
    expect(manifestResponse.ok()).toBeTruthy();

    const manifest = await manifestResponse.json();
    expect(manifest.sessions).toHaveLength(2);
    const sessionsById = manifestBySessionId(manifest);

    expect(sessionsById[rawSessionOne.expected.session_id]).toMatchObject({
      id: rawSessionOne.expected.session_id,
      source_host: sourceHost,
      source_path: rawSessionOne.expected.source_path,
      source_root: rawSessionOne.expected.source_root,
      file_size: rawSessionOne.expected.file_size,
      file_mtime_ns: rawSessionOne.expected.file_mtime_ns,
      content_sha256: rawSessionOne.expected.content_sha256,
      event_count: rawSessionOne.expected.event_count,
      stored_event_count: rawSessionOne.expected.event_count,
      has_raw_artifact: 1,
    });
    expect(sessionsById[rawSessionTwo.expected.session_id]).toMatchObject({
      id: rawSessionTwo.expected.session_id,
      source_host: sourceHost,
      source_path: rawSessionTwo.expected.source_path,
      source_root: rawSessionTwo.expected.source_root,
      file_size: rawSessionTwo.expected.file_size,
      file_mtime_ns: rawSessionTwo.expected.file_mtime_ns,
      content_sha256: rawSessionTwo.expected.content_sha256,
      event_count: rawSessionTwo.expected.event_count,
      stored_event_count: rawSessionTwo.expected.event_count,
      has_raw_artifact: 1,
    });
  });

  await test.step("replaying the same batch keeps the manifest stable", async () => {
    const replayResponse = await request.post(app.url("/api/sync/sessions-raw"), {
      headers,
      data: {
        sessions: [
          rawSessionOne.payload,
          rawSessionTwo.payload,
        ],
      },
    });
    expect(replayResponse.ok()).toBeTruthy();

    const replayResult = await replayResponse.json();
    expect(replayResult).toMatchObject({
      status: "ok",
      mode: "raw_batch",
      processed_count: 2,
    });

    const manifestResponse = await request.get(
      app.url(`/api/sync/manifest?host=${encodeURIComponent(sourceHost)}`),
      { headers },
    );
    expect(manifestResponse.ok()).toBeTruthy();

    const manifest = await manifestResponse.json();
    expect(manifest.sessions).toHaveLength(2);
    expect(
      manifest.sessions.map((session) => session.id).sort(),
    ).toEqual([
      rawSessionOne.expected.session_id,
      rawSessionTwo.expected.session_id,
    ].sort());
  });

  await login(page, app, {
    username: "admin",
    password: "Password123!",
  });

  await test.step("raw-batch imported data appears across browser surfaces", async () => {
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
    await expect(page.locator("body")).toContainText("2 up / 0 skip / 0 fail");

    await expectPageLoad(page, app.url("/search?q=raw%20batch%20session%20two"), {
      expectedPathname: "/search",
      expectedText: "Turn Hits",
    });
    await expect(page.getByRole("link", { name: projectLabel }).first()).toBeVisible();

    await expectPageLoad(page, app.url(detailPath), {
      expectedPathname: detailPath,
      expectedText: projectLabel,
    });
    await expect(page.locator("body")).toContainText(sourceHost);

    await expectPageLoad(page, app.url(`/sessions/${rawSessionTwo.expected.session_id}`), {
      expectedPathname: `/sessions/${rawSessionTwo.expected.session_id}`,
      expectedText: "Conversation",
    });
    await expect(page.locator("body")).toContainText(rawSessionTwo.expected.user_message);
    await expect(page.locator("body")).toContainText(rawSessionTwo.expected.assistant_message);
  });
});
