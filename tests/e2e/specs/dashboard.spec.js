const { test, expect } = require("../helpers/fixtures");
const { login } = require("../helpers/auth");
const { expectNoServerError } = require("../helpers/assertions");

test("dashboard loads for a seeded install", async ({ page, app, seed }) => {
  await seed.createAdmin({
    username: "admin",
    password: "Password123!",
  });
  await seed.heartbeat({
    sourceHost: "builder-1",
    status: "healthy",
    uploadCount: 1,
  });
  await seed.project({
    sourceHost: "builder-1",
    projectKey: "openai/codex-viewer",
    projectLabel: "openai/codex-viewer",
    githubOrg: "openai",
    githubRepo: "codex-viewer",
    sessionCount: 2,
    turns: 3,
    commandsPerTurn: 1,
  });

  await login(page, app, {
    username: "admin",
    password: "Password123!",
  });
  await page.goto(app.url("/"));

  await expect(page.getByRole("heading", { name: "Active Repos" })).toBeVisible();
  await expect(page.getByRole("link", { name: "View machines" })).toBeVisible();
  await expect(page.getByText("Today’s Turns")).toBeVisible();
  await expectNoServerError(page);
});
