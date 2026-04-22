const { test, expect } = require("../helpers/fixtures");
const { login } = require("../helpers/auth");
const { expectPageLoad } = require("../helpers/assertions");

async function loginAs(page, app, username) {
  await page.context().clearCookies();
  await login(page, app, {
    username,
    password: "Password123!",
  });
}

async function expectPageStatus(page, url, expectedStatus, expectedText) {
  const response = await page.goto(url, {
    waitUntil: "domcontentloaded",
  });

  expect(response, `Expected a navigation response for ${url}`).not.toBeNull();
  expect(response.status()).toBe(expectedStatus);

  if (expectedText) {
    await expect(page.locator("body")).toContainText(expectedText);
  }
}

test("private project data stays hidden until viewer access is granted", async ({ page, app, seed }) => {
  await seed.createAdmin({
    username: "admin",
    password: "Password123!",
  });
  await seed.createUser({
    username: "viewer",
    password: "Password123!",
    role: "viewer",
  });
  await seed.createToken({
    label: "ACL smoke token",
  });

  const publicSession = await seed.session({
    sourceHost: "public-host",
    projectKey: "acme/public-app",
    projectLabel: "acme/public-app",
    githubOrg: "acme",
    githubRepo: "public-app",
    sessionIndex: 1,
    turns: 3,
    commandsPerTurn: 1,
  });
  const privateSession = await seed.session({
    sourceHost: "secret-host",
    projectKey: "acme/secret-app",
    projectLabel: "acme/secret-app",
    githubOrg: "acme",
    githubRepo: "secret-app",
    sessionIndex: 1,
    turns: 3,
    commandsPerTurn: 1,
  });

  await seed.heartbeat({
    sourceHost: "public-host",
    status: "healthy",
    uploadCount: 1,
  });
  await seed.heartbeat({
    sourceHost: "secret-host",
    status: "healthy",
    uploadCount: 1,
  });
  await seed.setProjectVisibility({
    groupKey: "acme/secret-app",
    visibility: "private",
  });

  await test.step("viewer cannot see private repo before ACL grant", async () => {
    await loginAs(page, app, "viewer");

    await expectPageLoad(page, app.url("/"), {
      expectedPathname: "/",
      expectedText: "Active Repos",
    });
    await expect(page.locator("body")).toContainText("acme/public-app");
    await expect(page.locator("body")).not.toContainText("acme/secret-app");
    await expect(page.locator("body")).toContainText("public-host");
    await expect(page.locator("body")).not.toContainText("secret-host");

    await expectPageLoad(page, app.url("/machines"), {
      expectedPathname: "/machines",
      expectedText: "Machines",
    });
    await expect(page.locator("body")).toContainText("public-host");
    await expect(page.locator("body")).not.toContainText("secret-host");

    await expectPageLoad(page, app.url("/search?q=secret-app"), {
      expectedPathname: "/search",
      expectedText: "Turn Hits",
    });
    await expect(page.getByText(/No turns matched/i)).toBeVisible();
    await expect(page.getByRole("link", { name: "acme/secret-app" })).toHaveCount(0);

    await expectPageStatus(page, app.url("/acme/secret-app"), 404, "Project group not found");
    await expectPageStatus(page, app.url(`/sessions/${privateSession.session_id}`), 404, "Session not found");
    await expectPageStatus(page, app.url("/machines/secret-host/audit"), 404, "Remote host not found");
    await expectPageStatus(page, app.url("/acme/secret-app/queue"), 404, "Project group not found");
  });

  await seed.grantProjectAccess({
    groupKey: "acme/secret-app",
    username: "viewer",
    role: "viewer",
  });

  await test.step("viewer can see private repo after ACL grant", async () => {
    await loginAs(page, app, "viewer");

    await expectPageLoad(page, app.url("/"), {
      expectedPathname: "/",
      expectedText: "Active Repos",
    });
    await expect(page.locator("body")).toContainText("acme/public-app");
    await expect(page.locator("body")).toContainText("acme/secret-app");
    await expect(page.locator("body")).toContainText("public-host");
    await expect(page.locator("body")).toContainText("secret-host");

    await expectPageLoad(page, app.url("/machines"), {
      expectedPathname: "/machines",
      expectedText: "Machines",
    });
    await expect(page.locator("body")).toContainText("public-host");
    await expect(page.locator("body")).toContainText("secret-host");

    await expectPageLoad(page, app.url("/search?q=secret-app"), {
      expectedPathname: "/search",
      expectedText: "Turn Hits",
    });
    await expect(page.getByRole("link", { name: "acme/secret-app" }).first()).toBeVisible();

    await expectPageLoad(page, app.url("/acme/secret-app"), {
      expectedPathname: "/acme/secret-app",
      expectedText: "acme/secret-app",
    });
    await expectPageLoad(page, app.url(`/sessions/${privateSession.session_id}`), {
      expectedPathname: `/sessions/${privateSession.session_id}`,
      expectedText: "Conversation",
    });
    await expectPageLoad(page, app.url("/machines/secret-host/audit"), {
      expectedPathname: "/machines/secret-host/audit",
      expectedText: "Machine Environment Audit",
    });
    await expectPageLoad(page, app.url("/acme/secret-app/queue"), {
      expectedPathname: "/queue",
      expectedText: "Project Queue",
    });
    await expectPageStatus(page, app.url("/acme/secret-app/edit"), 403, "Admin access required");
  });

  await test.step("admin still owns the edit surface", async () => {
    await loginAs(page, app, "admin");

    await expectPageLoad(page, app.url("/acme/secret-app/edit"), {
      expectedPathname: "/acme/secret-app/edit",
      expectedText: "Edit Project",
    });
    await expectPageLoad(page, app.url(`/sessions/${publicSession.session_id}`), {
      expectedPathname: `/sessions/${publicSession.session_id}`,
      expectedText: "Conversation",
    });
  });
});
