const { test, expect } = require("../helpers/fixtures");
const { login } = require("../helpers/auth");

test("task assessment saves evidence-backed personal revisions and exports them", async ({ page, app, seed }, testInfo) => {
  await seed.createAdmin({ username: "admin", password: "Password123!" });
  const session = await seed.session({
    sourceHost: "assessment-host",
    projectKey: "openai/assessment-test",
    projectLabel: "openai/assessment-test",
    githubOrg: "openai",
    githubRepo: "assessment-test",
    turns: 2,
    commandsPerTurn: 1,
  });
  await login(page, app, { username: "admin", password: "Password123!" });
  await page.goto(app.url(`/sessions/${session.session_id}`));
  await page.getByRole("link", { name: "Assess task", exact: true }).first().click();
  await expect(page.getByRole("heading", { name: "Task assessment", exact: true })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Resource Work Units" })).toBeVisible();

  const exportUrl = await page.getByRole("link", { name: "Export JSON" }).getAttribute("href");
  const before = await (await page.request.get(app.url(exportUrl))).json();
  expect(before.review.model_fit).toBe("unestablished");
  const evidenceIndex = before.report.evidence[0].event_index;
  await page.locator('select[name="model_fit"]').selectOption("candidate_for_comparison");
  await page.getByLabel("Findings", { exact: true }).fill("A bounded change; compare a cheaper configuration with the same checks.");
  await page.getByLabel("Evidence event indexes").fill(String(evidenceIndex));
  await page.getByLabel("Recommended experiment", { exact: true }).fill("Hold the initial state and acceptance checks constant; reduce effort first.");
  await page.getByRole("button", { name: "Save review revision" }).click();
  await expect(page.getByRole("status")).toContainText("Latest personal review");
  await expect(page.locator('select[name="model_fit"]')).toHaveValue("candidate_for_comparison");
  const after = await (await page.request.get(app.url(exportUrl))).json();
  expect(after.review.evidence_level).toBe("human_trace_review");
  expect(after.review.evidence_events).toEqual([evidenceIndex]);
  expect(after.report.metrics.tokens).toEqual(before.report.metrics.tokens);
  await page.screenshot({ path: testInfo.outputPath("task-assessment.png"), fullPage: true });

  await page.getByRole("link", { name: /^Revision \d+ ·/ }).first().click();
  await expect(page.getByRole("heading", { name: "Saved review", exact: true })).toBeVisible();
  await expect(page.getByRole("button", { name: "Save review revision" })).toHaveCount(0);
  await expect(page.locator(`#event-${evidenceIndex}`)).toBeAttached();
  await page.setViewportSize({ width: 390, height: 844 });
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBeTruthy();
});
