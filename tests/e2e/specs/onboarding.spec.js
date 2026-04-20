const { test, expect } = require("../helpers/fixtures");
const { completePasswordSetup, createSetupToken } = require("../helpers/auth");
const { expectNoServerError } = require("../helpers/assertions");

test("first-run onboarding reaches the dashboard after first heartbeat and session", async ({ page, app, seed }) => {
  await page.goto(app.url("/"));
  await expect(page).toHaveURL(/\/setup$/);
  await completePasswordSetup(page, app, {
    username: "admin",
    password: "Password123!",
  });
  await expect(page.getByRole("heading", { name: "Finish first-run onboarding" })).toBeVisible();

  await createSetupToken(page, {
    label: "First machine token",
  });
  await expect(page.locator("#agent-config-snippet")).toContainText("CODEX_VIEWER_SERVER_URL");

  await seed.heartbeat({
    sourceHost: "builder-1",
    status: "healthy",
    uploadCount: 0,
    skipCount: 0,
    failCount: 0,
  });
  await expect(page.locator("[data-onboarding-status-region]")).toContainText("Connected, waiting for session data", {
    timeout: 10_000,
  });

  await seed.session({
    sourceHost: "builder-1",
    projectKey: "openai/codex-viewer",
    projectLabel: "openai/codex-viewer",
    githubOrg: "openai",
    githubRepo: "codex-viewer",
    turns: 2,
    commandsPerTurn: 1,
  });

  await page.waitForURL((url) => url.pathname === "/", {
    timeout: 15_000,
  });
  await expect(page.getByRole("heading", { name: "Active Repos" })).toBeVisible();
  await expectNoServerError(page);
});
