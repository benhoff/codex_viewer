const { test, expect } = require("../helpers/fixtures");
const { expectPageLoad } = require("../helpers/assertions");
const { loginAs, syncHeaders } = require("../helpers/browser");

test("raw resend requests complete the full machines UI to daemon acknowledgment handshake", async ({
  page,
  request,
  app,
  seed,
}) => {
  await seed.createAdmin({
    username: "admin",
    password: "Password123!",
  });
  const apiToken = await seed.createToken({
    label: "Machine action token",
  });

  const sourceHost = "resend-builder";
  const sessionPayload = await seed.buildSessionPayload({
    sourceHost,
    projectKey: "acme/resend-app",
    projectLabel: "acme/resend-app",
    githubOrg: "acme",
    githubRepo: "resend-app",
  });
  const headers = syncHeaders(apiToken.token, sourceHost);

  const heartbeatResponse = await request.post(app.url("/api/sync/heartbeat"), {
    headers,
    data: {
      source_host: sourceHost,
      last_upload_count: 1,
      last_skip_count: 0,
      last_fail_count: 0,
    },
  });
  expect(heartbeatResponse.ok()).toBeTruthy();

  const sessionResponse = await request.post(app.url("/api/sync/session"), {
    headers,
    data: sessionPayload,
  });
  expect(sessionResponse.ok()).toBeTruthy();

  await loginAs(page, app, {
    username: "admin",
    password: "Password123!",
  });

  await expectPageLoad(page, app.url("/machines"), {
    expectedPathname: "/machines",
    expectedText: "Machines",
  });
  await expect(page.locator("body")).toContainText(sourceHost);

  const machineCard = page.locator("article").filter({ hasText: sourceHost }).first();
  await machineCard.getByRole("button", { name: "Details" }).click();
  await machineCard.getByRole("button", { name: "Request raw resend" }).click();
  await expect(page).toHaveURL(app.url("/machines"));

  const manifestResponse = await request.get(
    app.url(`/api/sync/manifest?host=${encodeURIComponent(sourceHost)}`),
    { headers },
  );
  expect(manifestResponse.ok()).toBeTruthy();
  const manifest = await manifestResponse.json();
  expect(manifest.actions.resend_raw).toMatchObject({
    note: "Requested from machines view",
  });
  expect(manifest.actions.resend_raw.token).toBeTruthy();
  expect(manifest.actions.resend_raw.requested_at).toBeTruthy();

  await expectPageLoad(page, app.url("/machines"), {
    expectedPathname: "/machines",
    expectedText: "Machines",
  });
  let refreshedCard = page.locator("article").filter({ hasText: sourceHost }).first();
  await refreshedCard.getByRole("button", { name: "Details" }).click();
  await expect(refreshedCard).toContainText("Raw resend requested");

  const acknowledgedHeartbeat = await request.post(app.url("/api/sync/heartbeat"), {
    headers,
    data: {
      source_host: sourceHost,
      last_upload_count: 1,
      last_skip_count: 0,
      last_fail_count: 0,
      acknowledged_raw_resend_token: manifest.actions.resend_raw.token,
      last_raw_resend_at: manifest.actions.resend_raw.requested_at,
    },
  });
  expect(acknowledgedHeartbeat.ok()).toBeTruthy();

  const manifestAfterAckResponse = await request.get(
    app.url(`/api/sync/manifest?host=${encodeURIComponent(sourceHost)}`),
    { headers },
  );
  expect(manifestAfterAckResponse.ok()).toBeTruthy();
  const manifestAfterAck = await manifestAfterAckResponse.json();
  expect(manifestAfterAck.actions).toEqual({});

  await expectPageLoad(page, app.url("/machines"), {
    expectedPathname: "/machines",
    expectedText: "Machines",
  });
  refreshedCard = page.locator("article").filter({ hasText: sourceHost }).first();
  await refreshedCard.getByRole("button", { name: "Details" }).click();
  await expect(refreshedCard).not.toContainText("Raw resend requested");
  await expect(refreshedCard).toContainText("Last raw resend completed");
});
