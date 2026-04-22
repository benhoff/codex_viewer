const { test, expect } = require("../helpers/fixtures");
const { expectPageLoad } = require("../helpers/assertions");
const { loginAs, syncHeaders } = require("../helpers/browser");

test("sync token created and revoked from settings flips daemon auth from 200 to 401", async ({
  page,
  request,
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

  const tokenLabel = "Token lifecycle daemon";
  const sourceHost = "token-lifecycle-host";

  await loginAs(page, app, {
    username: "admin",
    password: "Password123!",
  });

  await expectPageLoad(page, app.url("/settings"), {
    expectedPathname: "/settings",
    expectedText: "Create Token",
  });

  await page.locator('#settings-create-token input[name="label"]').fill(tokenLabel);
  await page.getByRole("button", { name: "Create Token" }).click();

  await expect(page.locator("#created-token-value")).toContainText("csvr_");
  const rawToken = (await page.locator("#created-token-value").innerText()).trim();
  const tokenCard = page.locator("article").filter({ hasText: tokenLabel });
  await expect(tokenCard).toContainText("Active");

  const activeHeartbeat = await request.post(app.url("/api/sync/heartbeat"), {
    headers: syncHeaders(rawToken, sourceHost),
    data: {
      source_host: sourceHost,
      last_upload_count: 0,
      last_skip_count: 0,
      last_fail_count: 0,
    },
  });
  expect(activeHeartbeat.status()).toBe(200);

  await tokenCard.getByRole("button", { name: "Revoke" }).click();
  await expect(page).toHaveURL(app.url("/settings"));
  await expect(tokenCard).toContainText("Revoked");

  const revokedHeartbeat = await request.post(app.url("/api/sync/heartbeat"), {
    headers: syncHeaders(rawToken, sourceHost),
    data: {
      source_host: sourceHost,
      last_upload_count: 0,
      last_skip_count: 0,
      last_fail_count: 0,
    },
  });
  expect(revokedHeartbeat.status()).toBe(401);
});
