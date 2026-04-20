const { expect } = require("@playwright/test");

async function completePasswordSetup(page, app, options = {}) {
  const username = options.username || "admin";
  const password = options.password || "Password123!";
  await page.goto(app.url("/setup"));
  await page.locator('input[name="username"]').fill(username);
  await page.locator('input[name="password"]').fill(password);
  await page.locator('input[name="confirm_password"]').fill(password);
  await page.getByRole("button", { name: "Create Local Admin" }).click();
  await expect(page).toHaveURL(/\/setup$/);
}

async function createSetupToken(page, options = {}) {
  const label = options.label || "First machine token";
  await page.locator('input[name="label"]').fill(label);
  await page.getByRole("button", { name: "Create Token" }).click();
  await expect(page.locator("#created-token-value")).toContainText("csvr_");
}

async function login(page, app, options = {}) {
  const username = options.username || "admin";
  const password = options.password || "Password123!";
  await page.goto(app.url("/login"));
  await page.locator('input[name="username"]').fill(username);
  await page.locator('input[name="password"]').fill(password);
  await page.getByRole("button", { name: "Sign In" }).click();
  await expect(page).not.toHaveURL(/\/login$/);
}

module.exports = {
  completePasswordSetup,
  createSetupToken,
  login,
};
