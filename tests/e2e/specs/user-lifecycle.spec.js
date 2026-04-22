const { test, expect } = require("../helpers/fixtures");
const { expectPageLoad } = require("../helpers/assertions");
const { expectLoginFailure, loginAs } = require("../helpers/browser");

function userCard(page, username) {
  return page.locator("article").filter({ hasText: `@${username}` }).first();
}

test("user accounts can be created, disabled, re-enabled, and updated through the UI", async ({
  page,
  app,
  seed,
}) => {
  await seed.createAdmin({
    username: "admin",
    password: "Password123!",
  });
  await seed.createToken({
    label: "Bootstrap token",
  });
  await seed.session({
    sourceHost: "bootstrap-host",
    projectKey: "acme/bootstrap-app",
    projectLabel: "acme/bootstrap-app",
    githubOrg: "acme",
    githubRepo: "bootstrap-app",
  });
  await seed.heartbeat({
    sourceHost: "bootstrap-host",
    status: "healthy",
    uploadCount: 1,
  });

  const username = "viewer-lifecycle";
  const originalPassword = "Viewer123!";
  const updatedPassword = "Viewer456!";

  await loginAs(page, app, {
    username: "admin",
    password: "Password123!",
  });

  await expectPageLoad(page, app.url("/settings"), {
    expectedPathname: "/settings",
    expectedText: "Users",
  });

  await page.locator('form[action="/settings/users"] input[name="username"]').fill(username);
  await page.locator('form[action="/settings/users"] input[name="password"]').fill(originalPassword);
  await page.locator('form[action="/settings/users"] input[name="confirm_password"]').fill(originalPassword);
  await page.locator('form[action="/settings/users"] select[name="role"]').selectOption("viewer");
  await page.getByRole("button", { name: "Create User" }).click();
  await expect(page.locator("body")).toContainText(`Created viewer account ${username}.`);
  await expect(userCard(page, username)).toContainText("@viewer-lifecycle");

  await loginAs(page, app, {
    username,
    password: originalPassword,
  });
  await expectPageLoad(page, app.url("/"), {
    expectedPathname: "/",
    expectedText: "Active Repos",
  });

  await loginAs(page, app, {
    username: "admin",
    password: "Password123!",
  });
  await expectPageLoad(page, app.url("/settings"), {
    expectedPathname: "/settings",
    expectedText: "Users",
  });

  await userCard(page, username).getByRole("button", { name: "Disable" }).click();
  await expect(page.locator("body")).toContainText("User disabled.");
  await expect(userCard(page, username)).toContainText("disabled");

  await expectLoginFailure(page, app, {
    username,
    password: originalPassword,
  });

  await loginAs(page, app, {
    username: "admin",
    password: "Password123!",
  });
  await expectPageLoad(page, app.url("/settings"), {
    expectedPathname: "/settings",
    expectedText: "Users",
  });

  await userCard(page, username).getByRole("button", { name: "Enable" }).click();
  await expect(page.locator("body")).toContainText("User enabled.");

  await loginAs(page, app, {
    username,
    password: originalPassword,
  });
  await expectPageLoad(page, app.url("/settings"), {
    expectedPathname: "/settings",
    expectedText: "Update Password",
  });

  await page.locator('form[action="/settings/password"] input[name="current_password"]').fill(originalPassword);
  await page.locator('form[action="/settings/password"] input[name="new_password"]').fill(updatedPassword);
  await page.locator('form[action="/settings/password"] input[name="confirm_password"]').fill(updatedPassword);
  await page.getByRole("button", { name: "Update Password" }).click();
  await expect(page.locator("body")).toContainText("Password updated.");

  await expectLoginFailure(page, app, {
    username,
    password: originalPassword,
  });

  await loginAs(page, app, {
    username,
    password: updatedPassword,
  });
  await expectPageLoad(page, app.url("/"), {
    expectedPathname: "/",
    expectedText: "Active Repos",
  });
});
