const { test, expect } = require("../helpers/fixtures");
const { login } = require("../helpers/auth");
const { expectHorizontalFit, expectNoServerError } = require("../helpers/assertions");

test("mobile session view remains usable in conversation and audit modes", async ({ page, app, seed }) => {
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
    turns: 8,
    commandsPerTurn: 1,
  });

  await login(page, app, {
    username: "admin",
    password: "Password123!",
  });
  await page.goto(app.url(`/sessions/${session.session_id}`));

  await expect(page.getByRole("link", { name: "Conversation" })).toBeVisible();
  await expect(page.getByRole("link", { name: "Audit" })).toBeVisible();
  await expectHorizontalFit(page);

  await page.getByRole("link", { name: "Audit" }).click();
  await expect(page.getByText("Audit Summary")).toBeVisible();
  await expectHorizontalFit(page);
  await expectNoServerError(page);
});
