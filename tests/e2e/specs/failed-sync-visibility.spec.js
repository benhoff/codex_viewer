const { test, expect } = require("../helpers/fixtures");
const { expectPageLoad } = require("../helpers/assertions");
const { loginAs, syncHeaders } = require("../helpers/browser");

test("bad raw sync payloads surface failure counts and diagnostics in the machines UI", async ({
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
    label: "Failed sync token",
  });

  const sourceHost = "broken-builder";
  const headers = syncHeaders(apiToken.token, sourceHost);

  const badRawSyncResponse = await request.post(app.url("/api/sync/session-raw"), {
    headers,
    data: {
      source_host: sourceHost,
      source_root: "/tmp/e2e-sessions",
      source_path: "/tmp/e2e-sessions/failing.jsonl",
      raw_jsonl: "{}",
    },
  });
  expect(badRawSyncResponse.status()).toBe(400);
  const badRawSyncResult = await badRawSyncResponse.json();
  expect(badRawSyncResult.detail).toContain("Raw sync payload is missing file metadata");

  const failureHeartbeat = await request.post(app.url("/api/sync/heartbeat"), {
    headers,
    data: {
      source_host: sourceHost,
      last_upload_count: 0,
      last_skip_count: 0,
      last_fail_count: 1,
      last_error: "Upload failures occurred",
      last_failed_source_path: "/tmp/e2e-sessions/failing.jsonl",
      last_failure_detail: "RuntimeError: synthetic upload failure",
    },
  });
  expect(failureHeartbeat.ok()).toBeTruthy();

  await loginAs(page, app, {
    username: "admin",
    password: "Password123!",
  });

  await expectPageLoad(page, app.url("/machines"), {
    expectedPathname: "/machines",
    expectedText: "Machines",
  });

  const machineCard = page.locator("article").filter({ hasText: sourceHost }).first();
  await expect(machineCard).toContainText("Sync failure");
  await machineCard.getByRole("button", { name: "Details" }).click();
  await expect(machineCard).toContainText("0 up / 0 skip / 1 fail");
  await expect(machineCard).toContainText("Sync error:");
  await expect(machineCard).toContainText("Upload failures occurred");
  await expect(machineCard).toContainText("/tmp/e2e-sessions/failing.jsonl");
  await expect(machineCard).toContainText("RuntimeError: synthetic upload failure");
});
