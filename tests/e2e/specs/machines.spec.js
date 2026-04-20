const { test, expect } = require("../helpers/fixtures");
const { login } = require("../helpers/auth");
const { expectNoServerError } = require("../helpers/assertions");

test("machines page shows healthy and degraded hosts with concrete issue details", async ({ page, app, seed }) => {
  await seed.createAdmin({
    username: "admin",
    password: "Password123!",
  });
  await seed.session({
    sourceHost: "builder-1",
    projectKey: "openai/codex-viewer",
    projectLabel: "openai/codex-viewer",
    githubOrg: "openai",
    githubRepo: "codex-viewer",
    sessionIndex: 1,
    turns: 2,
    commandsPerTurn: 1,
  });
  await seed.session({
    sourceHost: "builder-2",
    projectKey: "openai/codex-viewer",
    projectLabel: "openai/codex-viewer",
    githubOrg: "openai",
    githubRepo: "codex-viewer",
    sessionIndex: 2,
    turns: 2,
    commandsPerTurn: 1,
  });
  await seed.heartbeat({
    sourceHost: "builder-1",
    status: "healthy",
    uploadCount: 2,
  });
  await seed.heartbeat({
    sourceHost: "builder-2",
    status: "degraded",
    uploadCount: 0,
    failCount: 1,
    lastError: "Upload failures occurred",
  });

  await login(page, app, {
    username: "admin",
    password: "Password123!",
  });
  await page.goto(app.url("/remotes"));

  await expect(page.getByRole("heading", { name: "Machines" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Needs Attention" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Active Hosts" })).toBeVisible();

  await page.getByRole("button", { name: /builder-2/i }).first().click();
  await expect(page.getByText("What Needs Attention")).toBeVisible();
  await expect(page.getByText("Sync error:")).toBeVisible();
  await expectNoServerError(page);
});
