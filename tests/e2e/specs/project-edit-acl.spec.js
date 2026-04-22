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

test("project edit updates canonical routing and ACL propagation", async ({ page, app, seed }) => {
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
    label: "Project edit E2E token",
  });
  const session = await seed.session({
    sourceHost: "builder-1",
    projectKey: "openai/codex-viewer",
    projectLabel: "openai/codex-viewer",
    githubOrg: "openai",
    githubRepo: "codex-viewer",
    turns: 4,
    commandsPerTurn: 1,
  });
  await seed.heartbeat({
    sourceHost: "builder-1",
    status: "healthy",
    uploadCount: 1,
  });

  const originalDetailPath = "/openai/codex-viewer";
  const originalEditPath = "/openai/codex-viewer/edit";
  const updatedProjectLabel = "acme/edited-private-app";
  const updatedDetailPath = "/acme/edited-private-app";
  const updatedEditPath = "/acme/edited-private-app/edit";
  const projectSearchQuery = "synthetic turn 4";

  await test.step("admin changes the canonical GitHub route in the edit UI", async () => {
    await loginAs(page, app, "admin");

    await expectPageLoad(page, app.url(originalEditPath), {
      expectedPathname: originalEditPath,
      expectedText: "Edit Project",
    });

    await page.locator('input[name="github_url"]').fill("https://github.com/acme/edited-private-app.git");
    await page.getByRole("button", { name: "Set GitHub URL" }).click();

    await expect(page).toHaveURL(app.url(updatedDetailPath));
    await expect(page.locator("body")).toContainText(updatedProjectLabel);

    await expectPageLoad(page, app.url(`/search?q=${encodeURIComponent(projectSearchQuery)}`), {
      expectedPathname: "/search",
      expectedText: "Turn Hits",
    });
    await expect(page.getByRole("link", { name: updatedProjectLabel }).first()).toBeVisible();

    await expectPageLoad(page, app.url(updatedDetailPath), {
      expectedPathname: updatedDetailPath,
      expectedText: updatedProjectLabel,
    });
    await expect(page.locator("body")).toContainText("builder-1");

    await expectPageStatus(page, app.url(originalDetailPath), 404, "Project group not found");
    await expectPageStatus(page, app.url(originalEditPath), 404, "Project group not found");
  });

  await test.step("admin makes the project private in the edit UI", async () => {
    await expectPageLoad(page, app.url(updatedEditPath), {
      expectedPathname: updatedEditPath,
      expectedText: "Visibility and Members",
    });

    await page.locator('select[name="visibility"]').selectOption("private");
    await page.getByRole("button", { name: "Save Visibility" }).click();

    await expect(page).toHaveURL(app.url(updatedEditPath));
    await expect(page.locator("body")).toContainText("No explicit members yet.");

    await expectPageLoad(page, app.url(updatedDetailPath), {
      expectedPathname: updatedDetailPath,
      expectedText: updatedProjectLabel,
    });
    await expect(page.locator("body")).toContainText("Private");
  });

  await test.step("viewer loses access after the project becomes private", async () => {
    await loginAs(page, app, "viewer");

    await expectPageLoad(page, app.url("/"), {
      expectedPathname: "/",
      expectedText: "Active Repos",
    });
    await expect(page.locator("body")).not.toContainText(updatedProjectLabel);

    await expectPageLoad(page, app.url(`/search?q=${encodeURIComponent(projectSearchQuery)}`), {
      expectedPathname: "/search",
      expectedText: "Turn Hits",
    });
    await expect(page.getByRole("link", { name: updatedProjectLabel })).toHaveCount(0);

    await expectPageStatus(page, app.url(updatedDetailPath), 404, "Project group not found");
    await expectPageStatus(page, app.url(`/sessions/${session.session_id}`), 404, "Session not found");
    await expectPageStatus(page, app.url(`${updatedDetailPath}/queue`), 404, "Project group not found");
    await expectPageStatus(page, app.url(updatedEditPath), 404, "Project group not found");
  });

  await test.step("admin grants viewer access from the edit UI", async () => {
    await loginAs(page, app, "admin");

    await expectPageLoad(page, app.url(updatedEditPath), {
      expectedPathname: updatedEditPath,
      expectedText: "Grant Project Access",
    });

    await page.locator('select[name="user_id"]').selectOption({ label: "viewer" });
    await page.locator('select[name="role"]').selectOption("viewer");
    await page.getByRole("button", { name: "Grant Access" }).click();

    await expect(page).toHaveURL(app.url(updatedEditPath));
    await expect(page.locator("body")).toContainText("1 explicit member");
    await expect(page.locator("body")).toContainText("viewer");
  });

  await test.step("viewer regains the renamed project route and read access", async () => {
    await loginAs(page, app, "viewer");

    await expectPageLoad(page, app.url("/"), {
      expectedPathname: "/",
      expectedText: "Active Repos",
    });
    await expect(page.locator("body")).toContainText(updatedProjectLabel);

    await expectPageLoad(page, app.url(`/search?q=${encodeURIComponent(projectSearchQuery)}`), {
      expectedPathname: "/search",
      expectedText: "Turn Hits",
    });
    await expect(page.getByRole("link", { name: updatedProjectLabel }).first()).toBeVisible();

    await expectPageLoad(page, app.url(updatedDetailPath), {
      expectedPathname: updatedDetailPath,
      expectedText: updatedProjectLabel,
    });
    await expect(page.locator("body")).toContainText("Private");

    await expectPageLoad(page, app.url(`/sessions/${session.session_id}`), {
      expectedPathname: `/sessions/${session.session_id}`,
      expectedText: "Conversation",
    });
    await expect(page.locator("body")).toContainText("Investigate synthetic turn 1");

    await expectPageLoad(page, app.url(`${updatedDetailPath}/queue`), {
      expectedPathname: "/queue",
      expectedText: "Project Queue",
    });
    await expect(page.locator("body")).toContainText(updatedProjectLabel);

    await expectPageStatus(page, app.url(updatedEditPath), 403, "Admin access required");
    await expectPageStatus(page, app.url(originalDetailPath), 404, "Project group not found");
  });
});
