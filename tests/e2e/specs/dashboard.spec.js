const { test, expect } = require("../helpers/fixtures");
const { login } = require("../helpers/auth");
const { expectHorizontalFit, expectNoServerError } = require("../helpers/assertions");

test("dashboard focuses on project activity and keeps diagnostics on Machines", async ({ page, app, seed }, testInfo) => {
  await seed.createAdmin({
    username: "admin",
    password: "Password123!",
  });
  await seed.createToken({ label: "Dashboard machine token" });
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
  await seed.heartbeat({
    sourceHost: "builder-1",
    status: "degraded",
    uploadCount: 1,
    failCount: 1,
    lastError: "Synthetic upload failure for dashboard cleanup",
  });
  await page.goto(app.url("/"));

  await expect(page.getByRole("heading", { name: "Active Repos" })).toBeVisible();
  await expect(page.getByRole("link", { name: "View machines" })).toBeVisible();
  await expect(page.getByText("Today’s Turns")).toBeVisible();
  await expect(page.getByRole("heading", { name: "Needs Attention", exact: true })).toHaveCount(0);
  await expect(page.getByText("Machines Needing Attention", { exact: true })).toHaveCount(0);
  await expect(page.getByText("Synthetic upload failure for dashboard cleanup")).toHaveCount(0);
  await expect(page.locator('[data-project-item] [title]')).toHaveCount(0);
  await expectHorizontalFit(page, 0);
  await page.screenshot({ path: testInfo.outputPath("dashboard-desktop.png"), fullPage: true });
  for (const width of [390, 320]) {
    await page.setViewportSize({ width, height: 844 });
    await expectHorizontalFit(page, 0);
    const projects = page.locator('[data-project-pane="mobile"]');
    await expect(projects).not.toBeVisible();
    await page.getByText("Browse repos", { exact: true }).click();
    await projects.getByRole("textbox", { name: "Find a repository" }).fill("codex-viewer");
    await expect(projects.locator('[data-project-item]:visible')).toHaveCount(1);
    await projects.getByRole("textbox", { name: "Find a repository" }).fill("missing-project");
    await expect(projects.getByText("No repositories matched your search.")).toBeVisible();
    await projects.getByRole("textbox", { name: "Find a repository" }).fill("");
    await page.getByText("Browse repos", { exact: true }).click();
    await page.screenshot({ path: testInfo.outputPath(`dashboard-mobile-${width}.png`), fullPage: true });
  }
  await expectNoServerError(page);
  await page.getByRole("link", { name: "View machines" }).click();
  await expect(page.getByRole("heading", { name: "Machines Needing Attention", exact: true })).toBeVisible();
  await expect(page.getByRole("button", { name: /Synthetic upload failure for dashboard cleanup/ })).toBeVisible();
});
