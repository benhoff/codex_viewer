const { test, expect } = require("../helpers/fixtures");
const { completePasswordSetup, createSetupToken, login } = require("../helpers/auth");
const { expectNoServerError } = require("../helpers/assertions");

test("password admin onboarding becomes authenticated-only after bootstrap", async ({ page, app }) => {
  await page.goto(app.url("/setup"));
  await expect(page).toHaveURL(/\/setup$/);
  await expect(page.getByRole("heading", { name: "Create the first admin" })).toBeVisible();

  await completePasswordSetup(page, app, {
    username: "admin",
    password: "Password123!",
  });
  await expect(page.getByRole("heading", { name: "Create a token for the first machine" })).toBeVisible();

  await page.context().clearCookies();

  await page.goto(app.url("/setup"));
  await expect(page).toHaveURL(/\/login\?next=\/setup$/);
  await expect(page.getByRole("heading", { name: "Sign In" })).toBeVisible();

  await page.goto(app.url("/setup/status"));
  await expect(page).toHaveURL(/\/login\?next=\/setup\/status$/);
  await expect(page.getByRole("heading", { name: "Sign In" })).toBeVisible();

  await login(page, app, {
    username: "admin",
    password: "Password123!",
  });
  await expect(page).toHaveURL(/\/setup$/);
  await expect(page.getByRole("heading", { name: "Create a token for the first machine" })).toBeVisible();

  await createSetupToken(page, {
    label: "First machine token",
  });
  await expect(page.getByRole("heading", { name: "Copy this token now" })).toBeVisible();
  await page.context().clearCookies();

  await login(page, app, {
    username: "admin",
    password: "Password123!",
  });
  await expect(page).toHaveURL(app.url("/"));
  await expect(page.getByText("First machine still pending")).toBeVisible();
  await expectNoServerError(page);
});
