const { test, expect } = require("../helpers/fixtures");
const { login } = require("../helpers/auth");
const { expectNoServerError } = require("../helpers/assertions");

test("session view supports audit mode and page navigation", async ({ page, app, seed }) => {
  await seed.createAdmin({
    username: "admin",
    password: "Password123!",
  });
  const session = await seed.session({
    sourceHost: "builder-1",
    projectKey: "openai/codex-viewer",
    projectLabel: "openai/codex-viewer",
    githubOrg: "openai",
    githubRepo: "codex-viewer",
    turns: 24,
    commandsPerTurn: 1,
  });

  await login(page, app, {
    username: "admin",
    password: "Password123!",
  });
  await page.goto(app.url(`/sessions/${session.session_id}`));

  await expect(page.getByRole("link", { name: "Conversation" })).toBeVisible();
  await expect(page.getByRole("link", { name: "Audit" })).toBeVisible();
  await expect(page.getByText(/Page 1 of 3/i)).toBeVisible();

  await page.getByRole("link", { name: "Audit" }).click();
  await expect(page.getByText("Audit Summary")).toBeVisible();
  await expect(page.getByText(/Page 1 of 5/i)).toBeVisible();

  await page.getByRole("link", { name: "Conversation" }).click();
  await expect(page.getByText(/Page 1 of 3/i)).toBeVisible();

  await page.getByRole("link", { name: "3", exact: true }).click();
  await expect(page.getByText(/Page 3 of 3/i)).toBeVisible();
  await expectNoServerError(page);
});
