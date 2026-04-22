const { test, expect } = require("../helpers/fixtures");
const { expectPageLoad } = require("../helpers/assertions");
const { loginAs, syncHeaders } = require("../helpers/browser");
const { runAppCommand, startWebhookServer } = require("../helpers/runtime");

test("sync failure alerts deliver open and resolved webhook notifications", async ({
  page,
  request,
  app,
  seed,
}) => {
  const webhook = await startWebhookServer();
  try {
    await seed.createAdmin({
      username: "admin",
      password: "Password123!",
    });
    const apiToken = await seed.createToken({
      label: "Alert delivery token",
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

    const healthResponse = await request.get(app.url("/api/health"));
    expect(healthResponse.ok()).toBeTruthy();
    const health = await healthResponse.json();

    await loginAs(page, app, {
      username: "admin",
      password: "Password123!",
    });

    await expectPageLoad(page, app.url("/settings"), {
      expectedPathname: "/settings",
      expectedText: "Save Server Settings",
    });

    await page.locator('input[name="alerts_webhook_url"]').fill(webhook.url);
    await page.locator('input[name="alerts_enabled"]').check();
    await page.locator('input[name="alerts_send_resolutions"]').check();
    await page.getByRole("button", { name: "Save Server Settings" }).click();
    await expect(page.locator("body")).toContainText("Server settings updated.");

    const sourceHost = "alert-failure-host";
    const headers = syncHeaders(apiToken.token, sourceHost);

    const failingHeartbeat = await request.post(app.url("/api/sync/heartbeat"), {
      headers,
      data: {
        source_host: sourceHost,
        agent_version: health.expected_agent_version,
        sync_api_version: health.sync_api_version,
        sync_mode: "remote",
        update_state: "current",
        server_version_seen: health.app_version,
        server_api_version_seen: health.sync_api_version,
        last_upload_count: 0,
        last_skip_count: 0,
        last_fail_count: 1,
        last_error: "Upload failures occurred",
        last_failed_source_path: "/tmp/e2e-sessions/failing.jsonl",
        last_failure_detail: "RuntimeError: synthetic upload failure",
      },
    });
    expect(failingHeartbeat.ok()).toBeTruthy();

    await runAppCommand(app, ["alerts", "--once"]);
    const openDeliveries = await webhook.waitForCount(1);
    const openAlert = openDeliveries.find(
      (delivery) => delivery.body && delivery.body.notification_kind === "open" && delivery.body.issue_kind === "sync_failure",
    );
    expect(openAlert).toBeTruthy();
    expect(openAlert.method).toBe("POST");
    expect(openAlert.path).toBe("/webhook");
    expect(openAlert.body).toMatchObject({
      source: "codex_session_viewer",
      notification_kind: "open",
      status: "open",
      source_host: sourceHost,
      issue_kind: "sync_failure",
      severity: "critical",
      title: "Sync failure",
    });
    expect(openAlert.body.detail).toContain("1 failed upload");
    expect(openAlert.body.detail).toContain("/tmp/e2e-sessions/failing.jsonl");
    expect(openAlert.body.detail_json.kind).toBe("sync_failure");

    const recoveredHeartbeat = await request.post(app.url("/api/sync/heartbeat"), {
      headers,
      data: {
        source_host: sourceHost,
        agent_version: health.expected_agent_version,
        sync_api_version: health.sync_api_version,
        sync_mode: "remote",
        last_upload_count: 1,
        last_skip_count: 0,
        last_fail_count: 0,
        update_state: "current",
        server_version_seen: health.app_version,
        server_api_version_seen: health.sync_api_version,
      },
    });
    expect(recoveredHeartbeat.ok()).toBeTruthy();

    await runAppCommand(app, ["alerts", "--once"]);
    const resolvedDeliveries = await webhook.waitForCount(2);
    const resolvedAlert = resolvedDeliveries.find(
      (delivery) => delivery.body && delivery.body.notification_kind === "resolved" && delivery.body.issue_kind === "sync_failure",
    );
    expect(resolvedAlert).toBeTruthy();
    expect(resolvedAlert.body).toMatchObject({
      source: "codex_session_viewer",
      notification_kind: "resolved",
      status: "resolved",
      source_host: sourceHost,
      issue_kind: "sync_failure",
      severity: "critical",
      title: "Sync failure",
    });
    expect(resolvedAlert.body.resolved_at).toBeTruthy();
  } finally {
    await webhook.close();
  }
});
