const { test, expect } = require("../helpers/fixtures");
const { expectPageLoad } = require("../helpers/assertions");
const { fetchWithSession, loginAs, syncHeaders } = require("../helpers/browser");

test("session export endpoints return the expected formats and respect ACL gating", async ({
  page,
  request,
  app,
  seed,
}) => {
  await seed.createAdmin({
    username: "admin",
    password: "Password123!",
  });
  await seed.createUser({
    username: "viewer",
    password: "Password123!",
    role: "viewer",
  });
  const apiToken = await seed.createToken({
    label: "Export contract token",
  });

  const sourceHost = "export-builder";
  const projectKey = "acme/export-app";
  const sessionFixture = await seed.buildRawSessionPayload({
    sourceHost,
    projectKey,
    projectLabel: projectKey,
    githubOrg: "acme",
    githubRepo: "export-app",
    sessionIndex: 1,
    userMessage: "Export contract user prompt.",
    assistantMessage: "Export contract assistant response.",
  });
  const headers = syncHeaders(apiToken.token, sourceHost);

  const heartbeatResponse = await request.post(app.url("/api/sync/heartbeat"), {
    headers,
    data: {
      source_host: sourceHost,
      last_upload_count: 1,
      last_skip_count: 0,
      last_fail_count: 0,
    },
  });
  expect(heartbeatResponse.ok()).toBeTruthy();

  const uploadResponse = await request.post(app.url("/api/sync/session-raw"), {
    headers,
    data: sessionFixture.payload,
  });
  expect(uploadResponse.ok()).toBeTruthy();

  const sessionId = sessionFixture.expected.session_id;
  const exportUrls = {
    raw: app.url(`/sessions/${sessionId}/export/raw`),
    json: app.url(`/sessions/${sessionId}/export/json`),
    markdown: app.url(`/sessions/${sessionId}/export/markdown`),
    bundle: app.url(`/sessions/${sessionId}/export/bundle`),
  };

  await loginAs(page, app, {
    username: "admin",
    password: "Password123!",
  });

  await expectPageLoad(page, app.url(`/sessions/${sessionId}`), {
    expectedPathname: `/sessions/${sessionId}`,
    expectedText: "Conversation",
  });

  const rawExport = await fetchWithSession(page, exportUrls.raw, "text");
  expect(rawExport.status).toBe(200);
  expect(rawExport.headers.contentType).toContain("application/x-ndjson");
  expect(rawExport.headers.contentDisposition).toContain(`${sessionId}.jsonl`);
  expect(rawExport.text).toContain(sessionFixture.expected.user_message);

  const jsonExport = await fetchWithSession(page, exportUrls.json, "json");
  expect(jsonExport.status).toBe(200);
  expect(jsonExport.headers.contentType).toContain("application/json");
  expect(jsonExport.headers.contentDisposition).toContain(`${sessionId}.json`);
  expect(jsonExport.json.session.id).toBe(sessionId);
  expect(Array.isArray(jsonExport.json.events)).toBeTruthy();
  expect(jsonExport.json.events.length).toBeGreaterThan(0);
  expect(jsonExport.json.execution_context).toBeTruthy();

  const markdownExport = await fetchWithSession(page, exportUrls.markdown, "text");
  expect(markdownExport.status).toBe(200);
  expect(markdownExport.headers.contentType).toContain("text/markdown");
  expect(markdownExport.headers.contentDisposition).toContain(`${sessionId}.md`);
  expect(markdownExport.text).toContain(sessionFixture.expected.user_message);
  expect(markdownExport.text).toContain(sessionFixture.expected.assistant_message);

  const bundleExport = await fetchWithSession(page, exportUrls.bundle, "bytes");
  expect(bundleExport.status).toBe(200);
  expect(bundleExport.headers.contentType).toContain("application/zip");
  expect(bundleExport.headers.contentDisposition).toContain(`${sessionId}.zip`);
  expect(bundleExport.byteLength).toBeGreaterThan(200);
  expect(bundleExport.firstBytes).toEqual([80, 75, 3, 4]);

  await seed.setProjectVisibility({
    groupKey: sessionFixture.expected.project_key,
    visibility: "private",
  });

  await loginAs(page, app, {
    username: "viewer",
    password: "Password123!",
  });

  const viewerRaw = await fetchWithSession(page, exportUrls.raw, "text");
  const viewerJson = await fetchWithSession(page, exportUrls.json, "text");
  const viewerMarkdown = await fetchWithSession(page, exportUrls.markdown, "text");
  const viewerBundle = await fetchWithSession(page, exportUrls.bundle, "text");

  expect(viewerRaw.status).toBe(404);
  expect(viewerJson.status).toBe(404);
  expect(viewerMarkdown.status).toBe(404);
  expect(viewerBundle.status).toBe(404);
});
