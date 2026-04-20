const { test, expect } = require("../helpers/fixtures");
const { login } = require("../helpers/auth");
const { expectNoServerError } = require("../helpers/assertions");

test("review queue save, resolve, and reopen flow works", async ({ page, app, seed }) => {
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
    turns: 2,
    commandsPerTurn: 1,
  });

  await login(page, app, {
    username: "admin",
    password: "Password123!",
  });
  await page.goto(app.url(`/sessions/${session.session_id}`));

  await page.getByRole("button", { name: "Save for review" }).first().click();
  await expect(page.getByText("Saved")).toBeVisible();

  await page.goto(app.url("/queue"));
  await expect(page.getByRole("heading", { name: "Review Queue" })).toBeVisible();
  await expect(page.getByRole("button", { name: "Resolve" })).toBeVisible();

  await page.getByRole("button", { name: "Resolve" }).first().click();
  await expect(page.getByText("No saved turns yet.")).toBeVisible();

  await page.getByRole("link", { name: "Resolved" }).click();
  await expect(page.getByRole("button", { name: "Reopen" })).toBeVisible();
  await page.getByRole("button", { name: "Reopen" }).first().click();

  await page.getByRole("link", { name: "Open" }).click();
  await expect(page.getByRole("button", { name: "Resolve" })).toBeVisible();
  await expectNoServerError(page);
});
