const { test, expect } = require("../helpers/fixtures");
const { completePasswordSetup, createSetupToken } = require("../helpers/auth");
const { expectNoServerError } = require("../helpers/assertions");

test("first-run setup opens the dashboard after token creation while verification continues", async ({ page, app, seed }) => {
  await page.goto(app.url("/"));
  await expect(page).toHaveURL(/\/setup$/);
  await expect(page.getByRole("heading", { name: "Create the first admin" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Create a token for the first machine" })).toBeHidden();

  await completePasswordSetup(page, app, {
    username: "admin",
    password: "Password123!",
  });
  await expect(page.getByRole("heading", { name: "Connect the first machine" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Create the first admin" })).toBeHidden();

  await createSetupToken(page, {
    label: "First machine token",
  });
  await expect(page.getByRole("heading", { name: "Copy this token now" })).toBeVisible();
  await expect(page.getByRole("button", { name: "Open Projects" })).toBeVisible();

  await page.goto(app.url("/"));
  await expect(page).toHaveURL(app.url("/"));
  await expect(page.getByRole("heading", { name: "Active Repos" })).toBeVisible();
  await expect(page.getByText("First machine still pending")).toBeVisible();

  await seed.heartbeat({
    sourceHost: "builder-1",
    status: "healthy",
    uploadCount: 0,
    skipCount: 0,
    failCount: 0,
  });
  await page.reload();
  await expect(page.getByText("Point the daemon at a Codex sessions directory")).toBeVisible();

  await seed.session({
    sourceHost: "builder-1",
    projectKey: "openai/codex-viewer",
    projectLabel: "openai/codex-viewer",
    githubOrg: "openai",
    githubRepo: "codex-viewer",
    turns: 2,
    commandsPerTurn: 1,
  });

  await page.reload();
  await expect(page.getByText("First machine still pending")).toHaveCount(0);
  await expectNoServerError(page);
});
