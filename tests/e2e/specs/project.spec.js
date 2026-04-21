const { test, expect } = require("../helpers/fixtures");
const { login } = require("../helpers/auth");
const { expectNoServerError } = require("../helpers/assertions");

test("project control pane renders recent activity and host summaries", async ({ page, app, seed }) => {
  await seed.createAdmin({
    username: "admin",
    password: "Password123!",
  });
  await seed.heartbeat({
    sourceHost: "builder-1",
    status: "healthy",
    uploadCount: 2,
  });
  await seed.heartbeat({
    sourceHost: "builder-2",
    status: "healthy",
    uploadCount: 1,
  });
  await seed.session({
    sourceHost: "builder-1",
    projectKey: "openai/codex-viewer",
    projectLabel: "openai/codex-viewer",
    githubOrg: "openai",
    githubRepo: "codex-viewer",
    sessionIndex: 1,
    turns: 4,
    commandsPerTurn: 1,
  });
  await seed.session({
    sourceHost: "builder-2",
    projectKey: "openai/codex-viewer",
    projectLabel: "openai/codex-viewer",
    githubOrg: "openai",
    githubRepo: "codex-viewer",
    sessionIndex: 2,
    turns: 3,
    commandsPerTurn: 1,
  });

  await login(page, app, {
    username: "admin",
    password: "Password123!",
  });
  await page.goto(app.url("/openai/codex-viewer"));

  await expect(page.getByRole("heading", { name: "Recent Turns" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Sessions" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Repo Blockers" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Operational Context" })).toBeVisible();
  await expectNoServerError(page);
});
