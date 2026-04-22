const { test, expect } = require("../helpers/fixtures");
const { expectPageLoad } = require("../helpers/assertions");
const { expectPageStatus, loginAs, syncHeaders } = require("../helpers/browser");

async function seedSyncedProject(request, seed, app, options = {}) {
  const apiToken = await seed.createToken({
    label: options.tokenLabel || "Project action bootstrap token",
  });
  const sessionPayload = await seed.buildSessionPayload({
    sourceHost: options.sourceHost,
    projectKey: options.projectKey,
    projectLabel: options.projectLabel,
    githubOrg: options.githubOrg,
    githubRepo: options.githubRepo,
    turns: options.turns || 3,
    commandsPerTurn: options.commandsPerTurn || 1,
    sessionIndex: options.sessionIndex || 1,
  });
  const headers = syncHeaders(apiToken.token, options.sourceHost);

  const heartbeatResponse = await request.post(app.url("/api/sync/heartbeat"), {
    headers,
    data: {
      source_host: options.sourceHost,
      last_upload_count: 1,
      last_skip_count: 0,
      last_fail_count: 0,
    },
  });
  expect(heartbeatResponse.ok()).toBeTruthy();

  const sessionResponse = await request.post(app.url("/api/sync/session"), {
    headers,
    data: sessionPayload,
  });
  expect(sessionResponse.ok()).toBeTruthy();

  return {
    apiToken,
    headers,
    sessionPayload,
  };
}

async function saveSessionForReview(page, app, sessionId) {
  await expectPageLoad(page, app.url(`/sessions/${sessionId}`), {
    expectedPathname: `/sessions/${sessionId}`,
    expectedText: "Conversation",
  });
  await page.getByRole("button", { name: "Save for review" }).first().click();
  await expect(page.getByText("Saved to review queue")).toBeVisible();
}

async function triggerProjectAction(page, app, detailPath, actionLabel) {
  await expectPageLoad(page, app.url(detailPath), {
    expectedPathname: detailPath,
    expectedText: "Open stream",
  });
  await page.locator("details.group > summary").click();
  page.once("dialog", (dialog) => dialog.accept());
  await page.getByRole("button", { name: actionLabel }).click();
  await expect(page).toHaveURL(app.url("/"));
}

test("ignoring a project removes it from read surfaces and suppresses future sync imports", async ({
  page,
  request,
  app,
  seed,
}) => {
  await seed.createAdmin({
    username: "admin",
    password: "Password123!",
  });
  await seed.session({
    sourceHost: "bootstrap-host",
    projectKey: "acme/bootstrap-app",
    projectLabel: "acme/bootstrap-app",
    githubOrg: "acme",
    githubRepo: "bootstrap-app",
  });
  await seed.heartbeat({
    sourceHost: "bootstrap-host",
    status: "healthy",
    uploadCount: 1,
  });

  const sourceHost = "ignore-builder";
  const projectKey = "acme/ignore-me";
  const projectLabel = "acme/ignore-me";
  const detailPath = `/${projectKey}`;
  const searchQuery = "ignore-me";
  const { apiToken, headers, sessionPayload } = await seedSyncedProject(request, seed, app, {
    tokenLabel: "Ignore project token",
    sourceHost,
    projectKey,
    projectLabel,
    githubOrg: "acme",
    githubRepo: "ignore-me",
  });

  await loginAs(page, app, {
    username: "admin",
    password: "Password123!",
  });
  await saveSessionForReview(page, app, sessionPayload.session.id);

  await expectPageLoad(page, app.url("/queue"), {
    expectedPathname: "/queue",
    expectedText: "Review Queue",
  });
  await expect(page.locator("body")).toContainText(projectLabel);

  await triggerProjectAction(page, app, detailPath, "Ignore");

  await expect(page.locator("body")).not.toContainText(projectLabel);

  await expectPageLoad(page, app.url(`/search?q=${encodeURIComponent(searchQuery)}`), {
    expectedPathname: "/search",
    expectedText: "Turn Hits",
  });
  await expect(page.locator("body")).toContainText("No turns matched");
  await expect(page.getByRole("link", { name: projectLabel })).toHaveCount(0);

  await expectPageLoad(page, app.url("/queue"), {
    expectedPathname: "/queue",
    expectedText: "Review Queue",
  });
  await expect(page.locator("body")).toContainText("No saved turns yet.");
  await expect(page.locator("body")).not.toContainText(projectLabel);

  await expectPageStatus(page, app.url(detailPath), 404, "Project group not found");
  await expectPageStatus(page, app.url(`/sessions/${sessionPayload.session.id}`), 404, "Session not found");

  const manifestResponse = await request.get(
    app.url(`/api/sync/manifest?host=${encodeURIComponent(sourceHost)}`),
    { headers: syncHeaders(apiToken.token, sourceHost) },
  );
  expect(manifestResponse.ok()).toBeTruthy();
  const manifest = await manifestResponse.json();
  expect(manifest.sessions).toEqual([]);
  expect(manifest.ignored_project_keys).toContain(projectKey);

  const replayResponse = await request.post(app.url("/api/sync/session"), {
    headers,
    data: sessionPayload,
  });
  expect(replayResponse.ok()).toBeTruthy();
  const replayResult = await replayResponse.json();
  expect(replayResult).toMatchObject({
    status: "ignored",
    session_id: sessionPayload.session.id,
    source_host: sourceHost,
    inferred_project_key: projectKey,
  });
});

test("deleting a project removes it from dashboard, search, queue, and direct routes without ignoring future imports", async ({
  page,
  request,
  app,
  seed,
}) => {
  await seed.createAdmin({
    username: "admin",
    password: "Password123!",
  });
  await seed.session({
    sourceHost: "bootstrap-host",
    projectKey: "acme/bootstrap-app",
    projectLabel: "acme/bootstrap-app",
    githubOrg: "acme",
    githubRepo: "bootstrap-app",
  });
  await seed.heartbeat({
    sourceHost: "bootstrap-host",
    status: "healthy",
    uploadCount: 1,
  });

  const sourceHost = "delete-builder";
  const projectKey = "acme/delete-me";
  const projectLabel = "acme/delete-me";
  const detailPath = `/${projectKey}`;
  const searchQuery = "delete-me";
  const { apiToken, sessionPayload } = await seedSyncedProject(request, seed, app, {
    tokenLabel: "Delete project token",
    sourceHost,
    projectKey,
    projectLabel,
    githubOrg: "acme",
    githubRepo: "delete-me",
  });

  await loginAs(page, app, {
    username: "admin",
    password: "Password123!",
  });
  await saveSessionForReview(page, app, sessionPayload.session.id);

  await triggerProjectAction(page, app, detailPath, "Delete");

  await expect(page.locator("body")).not.toContainText(projectLabel);

  await expectPageLoad(page, app.url(`/search?q=${encodeURIComponent(searchQuery)}`), {
    expectedPathname: "/search",
    expectedText: "Turn Hits",
  });
  await expect(page.locator("body")).toContainText("No turns matched");
  await expect(page.getByRole("link", { name: projectLabel })).toHaveCount(0);

  await expectPageLoad(page, app.url("/queue"), {
    expectedPathname: "/queue",
    expectedText: "Review Queue",
  });
  await expect(page.locator("body")).toContainText("No saved turns yet.");
  await expect(page.locator("body")).not.toContainText(projectLabel);

  await expectPageStatus(page, app.url(detailPath), 404, "Project group not found");
  await expectPageStatus(page, app.url(`/sessions/${sessionPayload.session.id}`), 404, "Session not found");

  const manifestResponse = await request.get(
    app.url(`/api/sync/manifest?host=${encodeURIComponent(sourceHost)}`),
    { headers: syncHeaders(apiToken.token, sourceHost) },
  );
  expect(manifestResponse.ok()).toBeTruthy();
  const manifest = await manifestResponse.json();
  expect(manifest.sessions).toEqual([]);
  expect(manifest.ignored_project_keys).not.toContain(projectKey);
});
