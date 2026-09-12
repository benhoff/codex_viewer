const { test, expect } = require("../helpers/fixtures");
const { login } = require("../helpers/auth");

test("chunk selection spans pages and validates ranges on mobile", async ({ page, app, seed }, testInfo) => {
  await seed.createAdmin();
  const session = await seed.session({ sourceHost: "chunk-host", projectKey: "chunk/project", turns: 12, commandsPerTurn: 0 });
  await login(page, app);
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto(app.url(`/sessions/${session.session_id}?view=conversation`));
  await page.getByRole("checkbox", { name: "Select turn 12 for chunk", exact: true }).check();
  await page.getByRole("link", { name: "Older turns", exact: true }).first().click();
  await page.getByRole("checkbox", { name: "Select turn 1 for chunk", exact: true }).check();
  await expect(page.locator("#chunk-summary")).toContainText("Turns 1–12 (12 turns)");
  await page.getByRole("link", { name: "Back to latest", exact: true }).first().click();
  await expect(page.getByRole("checkbox", { name: "Select turn 12 for chunk", exact: true })).toBeChecked();
  await expect(page.locator("#chunk-summary")).toContainText("Turns 1–12");
  const picker = page.locator("#chunk-picker");
  await picker.getByLabel("Last turn").fill("13");
  await expect(picker.getByRole("button", { name: "Review chunk & grade" })).toBeDisabled();
  await picker.getByRole("button", { name: "Clear", exact: true }).click();
  await expect(picker.getByLabel("First turn")).toHaveValue("");
  await picker.getByRole("button", { name: "Use this page" }).click();
  await expect(picker.getByLabel("First turn")).toHaveValue("3");
  await expect(picker.getByLabel("Last turn")).toHaveValue("12");
  await picker.scrollIntoViewIfNeeded();
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBeTruthy();
  await page.screenshot({ path: testInfo.outputPath("chunk-picker-mobile.png") });
  await picker.getByRole("button", { name: "Review chunk & grade" }).click();
  await expect(page).toHaveURL(app.url(`/sessions/${session.session_id}/assessment?start_turn=3&end_turn=12#llm-grader`));
  await expect(page.locator("#llm-grader")).toContainText("AI grading is disabled");
});

test("assessment dashboard covers synced machines and opens full-session reviews", async ({ page, app, seed }, testInfo) => {
  await seed.createAdmin({ username: "admin", password: "Password123!" });
  const first = await seed.session({ sourceHost: "dashboard-machine-a", projectKey: "assessment/project-a", projectLabel: "assessment/project-a", turns: 3 });
  await seed.session({ sourceHost: "dashboard-machine-b", projectKey: "assessment/project-b", projectLabel: "assessment/project-b", turns: 2 });
  await login(page, app, { username: "admin", password: "Password123!" });
  await page.goto(app.url(`/sessions/${first.session_id}`));
  await page.getByRole("link", { name: "Assessments", exact: true }).click();
  await expect(page.getByRole("heading", { name: "Assessment dashboard", exact: true })).toBeVisible();
  await expect(page.locator("article[data-session-id]")).toHaveCount(2);
  const initial = await (await page.request.get(app.url("/assessments.json"))).json();
  expect(initial.machine_count).toBe(2);
  expect(initial.reviewed_count).toBe(0);
  await page.getByRole("combobox", { name: "Machine", exact: true }).selectOption("dashboard-machine-a");
  await page.getByRole("button", { name: "Apply filters" }).click();
  await expect(page.locator("article[data-session-id]")).toHaveCount(1);
  await page.getByRole("link", { name: "Assess full session", exact: true }).click();
  await expect(page.locator('input[name="start_turn"]').first()).toHaveValue("1");
  await expect(page.locator('input[name="end_turn"]').first()).toHaveValue("3");
  await page.getByRole("button", { name: "Save review revision" }).click();
  await page.getByRole("link", { name: "← All assessments", exact: true }).click();
  await expect(page.locator(`[data-session-id="${first.session_id}"]`)).toContainText("Your latest review · Turns 1–3");
  await page.getByRole("combobox", { name: "Your reviews", exact: true }).selectOption("reviewed");
  await page.getByRole("button", { name: "Apply filters" }).click();
  await expect(page.locator("article[data-session-id]")).toHaveCount(1);
  const exportUrl = await page.getByRole("link", { name: "Export this page", exact: true }).getAttribute("href");
  const exported = await (await page.request.get(app.url(exportUrl))).json();
  expect(exported.total).toBe(1);
  expect(exported.sessions[0].review.end_turn).toBe(3);
  await page.screenshot({ path: testInfo.outputPath("assessment-dashboard.png"), fullPage: true });
  await page.setViewportSize({ width: 390, height: 844 });
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBeTruthy();
  expect((await page.locator("article h3").first().boundingBox()).width).toBeGreaterThan(240);
  await page.screenshot({ path: testInfo.outputPath("assessment-dashboard-mobile.png"), fullPage: true });
  await page.getByText("Menu", { exact: true }).click();
  await page.getByRole("link", { name: "Assessments", exact: true }).click();
  await expect(page.getByRole("heading", { name: "Assessment dashboard", exact: true })).toBeVisible();
});

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
  await page.getByRole("link", { name: "Grade turn", exact: true }).first().click();
  await expect(page.getByRole("heading", { name: "Task assessment", exact: true })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Resource Work Units" })).toBeVisible();

  const exportUrl = await page.getByRole("link", { name: "Export JSON" }).getAttribute("href");
  const before = await (await page.request.get(app.url(exportUrl))).json();
  expect(before.review.model_fit).toBe("unestablished");
  const evidenceIndex = before.report.evidence[0].event_index;
  await page.getByText("Task demand (optional)", { exact: true }).click();
  await page.locator('select[name="complexity"]').selectOption("4");
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
  expect(after.review.demand.complexity).toBe(4);
  expect(after.report.metrics.tokens).toEqual(before.report.metrics.tokens);
  await page.screenshot({ path: testInfo.outputPath("task-assessment.png"), fullPage: true });

  await page.getByRole("link", { name: /^Revision \d+ ·/ }).first().click();
  await expect(page.getByRole("heading", { name: "Saved review", exact: true })).toBeVisible();
  await expect(page.getByRole("button", { name: "Save review revision" })).toHaveCount(0);
  await expect(page.getByRole("heading", { name: "Task demand", exact: true })).toBeVisible();
  await expect(page.locator("dl div").filter({ has: page.getByText("Complexity", { exact: true }) })).toContainText("4");
  await page.getByText("Saved Work Unit policy", { exact: true }).click();
  await expect(page.locator("details pre").filter({ hasText: '"version": "work-units-v1"' })).toBeVisible();
  await expect(page.locator(`#event-${evidenceIndex}`)).toBeAttached();
  await page.setViewportSize({ width: 390, height: 844 });
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBeTruthy();
});
