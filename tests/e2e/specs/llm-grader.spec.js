const http = require("http");
const { test, expect } = require("../helpers/fixtures");
const { login } = require("../helpers/auth");

test("configure a local grader, produce cited capability bars, and retain failed attempts", async ({ page, request, app, seed }, testInfo) => {
  test.setTimeout(60000);
  const calls = [];
  const authorizations = [];
  let invalidOutput = false;
  let failBatch = null;
  let responseDelay = 200;
  let abortedRequests = 0;
  const provider = http.createServer((req, res) => {
    let body = "";
    req.on("data", chunk => { body += chunk; });
    req.on("end", () => {
      const input = JSON.parse(body);
      calls.push(input);
      authorizations.push(req.headers.authorization);
      const evidence = JSON.parse(input.messages[1].content);
      if (evidence.batch && evidence.batch.number === failBatch) {
        res.writeHead(503, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "Temporary provider failure" }));
        return;
      }
      const output = input.response_format.json_schema.name === "EvidenceNotes" ? {
        findings: [{ text: "Observed bounded work", event_indexes: [invalidOutput ? 99999 : evidence.events[0].event_index] }],
        limitations: "Only the supplied evidence was examined.",
      } : evidence.events ? {
        required_level: 2, required_low: 1, required_high: 3, confidence: "medium", outcome: "unknown",
        acceptance_criteria: "Implement the requested change", verification_notes: "Independent verification is absent.",
        findings: [{ text: "A bounded request; success remains unverified.", event_indexes: [invalidOutput ? 99999 : evidence.events[0].event_index] }],
        recommended_experiment: "Try lower effort with the same acceptance checks.",
      } : { configured_level: 4, confidence: "low", basis: "Test fixture estimate, not validated capability." };
      res.writeHead(200, { "Content-Type": "application/json" });
      const timer = setTimeout(() => res.end(JSON.stringify({ model: "mock-grader", usage: { prompt_tokens: 100, completion_tokens: 25, total_tokens: 125 }, choices: [{ finish_reason: "stop", message: { content: JSON.stringify(output) } }] })), responseDelay);
      res.on("close", () => {
        clearTimeout(timer);
        if (!res.writableFinished) abortedRequests += 1;
      });
    });
  });
  await new Promise(resolve => provider.listen(0, "127.0.0.1", resolve));
  try {
    await seed.createAdmin();
    const token = await seed.createToken();
    const session = await seed.buildRawSessionPayload({ sourceHost: "grader-machine", projectKey: "grader/project", userMessage: "Implement the small requested change." });
    const records = session.payload.raw_jsonl.split("\n").filter(Boolean).map(JSON.parse);
    records.splice(1, 0, { type: "turn_context", payload: { model: "configured-test-model", effort: "high" } });
    for (const message of ["Fix the edge case in this change.", "Verify the complete change.", "Unrelated task outside the selected chunk."]) {
      records.push({ type: "event_msg", payload: { type: "user_message", message: message.repeat(40) } });
      records.push({ type: "event_msg", payload: { type: "agent_message", message: "Completed this follow-up." } });
    }
    session.payload.raw_jsonl = records.map(record => JSON.stringify(record)).join("\n");
    session.payload.file_size = Buffer.byteLength(session.payload.raw_jsonl);
    const uploaded = await request.post(app.url("/api/sync/session-raw"), { headers: { authorization: `Bearer ${token.token}`, "x-codex-viewer-host": "grader-machine" }, data: session.payload });
    expect(uploaded.ok()).toBeTruthy();
    const sessionId = (await uploaded.json()).session_id;
    await login(page, app);
    await page.goto(app.url("/settings"));
    await page.getByRole("heading", { name: "LLM Configuration" }).scrollIntoViewIfNeeded();
    await expect(page.getByRole("heading", { name: "LLM Configuration" })).toBeVisible();
    await expect(page.locator('input[name="enabled"]')).not.toBeChecked();
    await page.locator('input[name="enabled"]').check();
    await page.locator('select[name="processing"]').selectOption("local");
    await page.locator('input[name="base_url"]').fill(`http://127.0.0.1:${provider.address().port}/v1`);
    await page.locator('input[name="model"]').fill("mock-grader");
    await page.getByLabel("API key", { exact: true }).fill("browser-test-key");
    await page.getByRole("button", { name: "Save grader configuration" }).click();
    await expect(page).toHaveURL(app.url("/settings#settings-llm"));
    await expect(page.locator('input[name="enabled"]')).toBeChecked();
    await expect(page.getByText("API key: Configured", { exact: true })).toBeVisible();
    await expect(page.getByLabel("API key", { exact: true })).toHaveValue("");
    expect(await page.content()).not.toContain("browser-test-key");
    await page.getByRole("button", { name: "Save grader configuration" }).click();
    await page.goto(app.url(`/sessions/${sessionId}?view=conversation`));
    await page.getByRole("checkbox", { name: "Select turn 1 for chunk", exact: true }).check();
    await page.getByRole("checkbox", { name: "Select turn 3 for chunk", exact: true }).check();
    await expect(page.locator("#chunk-summary")).toContainText("Turns 1–3 (3 turns)");
    await page.getByRole("button", { name: "Review chunk & grade" }).click();
    const assessment = app.url(`/sessions/${sessionId}/assessment?start_turn=1&end_turn=3`);
    await expect(page).toHaveURL(assessment + "#llm-grader");
    expect(calls).toHaveLength(0);
    const gradeEndpoint = app.url(`/sessions/${sessionId}/assessment/grade`);
    await page.route(gradeEndpoint, route => route.fulfill({ status: 409, contentType: "application/json", body: JSON.stringify({ detail: "Evidence changed. Reload before requesting grading." }) }));
    await page.getByRole("button", { name: "Submit chunk for AI grading" }).click();
    await expect(page.locator("#grading-status")).toContainText("Evidence changed");
    await expect(page.getByRole("button", { name: "Submit chunk for AI grading" })).toBeEnabled();
    expect(calls).toHaveLength(0);
    await page.unroute(gradeEndpoint);
    await page.getByRole("button", { name: "Submit chunk for AI grading" }).click();
    await expect(page.getByRole("button", { name: "Grading…", exact: true })).toBeDisabled();
    await expect(page.getByRole("group", { name: "Estimated configured versus required intelligence" })).toBeVisible();
    await expect(page.locator("#llm-grader")).toContainText("4 / 5");
    await expect(page.locator("#llm-grader")).toContainText("2 / 5");
    expect(calls).toHaveLength(2);
    expect(calls.every(call => call.chat_template_kwargs.enable_thinking === false)).toBeTruthy();
    expect(calls.every(call => call.max_completion_tokens === 512)).toBeTruthy();
    expect(authorizations).toEqual(["Bearer browser-test-key", "Bearer browser-test-key"]);
    expect(calls[0].messages[1].content).not.toContain("configured-test-model");
    expect(calls[0].messages[1].content).toContain("Fix the edge case");
    expect(calls[0].messages[1].content).not.toContain("Unrelated task outside");
    expect(calls[1].messages[1].content).toContain("configured-test-model");
    const report = await (await page.request.get(app.url(`/sessions/${sessionId}/assessment.json?start_turn=1&end_turn=3`))).json();
    expect(report.report.turns).toHaveLength(3);
    expect(report.review.outcome).toBe("unknown");
    expect(report.grade_run.status).toBe("completed");
    await page.screenshot({ path: testInfo.outputPath("llm-grader-chart.png"), fullPage: true });
    await page.setViewportSize({ width: 390, height: 844 });
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBeTruthy();
    await page.goto(app.url("/assessments"));
    await expect(page.getByRole("heading", { name: "Assessment dashboard", exact: true })).toBeVisible();
    await page.screenshot({ path: testInfo.outputPath("llm-grader-dashboard-mobile.png"), fullPage: true });
    await page.goto(assessment);
    invalidOutput = true;
    await page.getByRole("button", { name: "Submit chunk for AI grading" }).click();
    await expect(page.locator("#llm-grader").getByRole("alert")).toContainText("outside this task");
    const exported = await page.request.get(app.url(`/sessions/${sessionId}/assessment/grader/${report.grade_run.id}.json`));
    expect(exported.ok()).toBeTruthy();
    const exportedRun = await exported.json();
    expect(exportedRun.result.configuration.configured_level).toBe(4);
    expect(JSON.stringify(exportedRun)).not.toContain("browser-test-key");
    invalidOutput = false;
    responseDelay = 10000;
    const beforeCancel = calls.length;
    await page.getByRole("button", { name: "Submit chunk for AI grading", exact: true }).click();
    await expect.poll(() => calls.length).toBe(beforeCancel + 1);
    await page.getByRole("button", { name: "Cancel grading", exact: true }).click();
    await expect(page.locator("#llm-grader").getByRole("alert")).toContainText("Grading cancelled");
    expect(abortedRequests).toBe(1);
    expect(calls.length).toBe(beforeCancel + 1);
    responseDelay = 200;
    await page.getByRole("button", { name: "Submit chunk for AI grading", exact: true }).click();
    await expect(page.getByRole("button", { name: "Grading…", exact: true })).toBeDisabled();
    await expect(page.getByRole("button", { name: "Submit chunk for AI grading", exact: true })).toBeEnabled();
    await expect(page.locator("#llm-grader").getByRole("alert")).toHaveCount(0);
    expect(calls.length).toBe(beforeCancel + 3);
    await page.goto(app.url("/settings#settings-llm"));
    await page.locator('input[name="max_input_chars"]').fill("3000");
    await page.getByRole("button", { name: "Save grader configuration" }).click();
    await page.goto(assessment);
    await expect(page.locator("#llm-grader")).toContainText("Estimated plan:");
    const beforeBatches = calls.length;
    failBatch = 2;
    await page.getByRole("button", { name: "Submit chunk for AI grading", exact: true }).click();
    await expect(page.locator("#llm-grader").getByRole("alert")).toContainText("HTTP 503");
    await expect(page.locator("#grading-batches")).toContainText("Batch 1 · Turns 1–1 · completed");
    failBatch = null;
    await page.getByRole("button", { name: "Retry unfinished batches", exact: true }).click();
    await expect(page.getByRole("button", { name: "Grading…", exact: true })).toBeDisabled();
    await expect(page.locator("#llm-grader").getByRole("alert")).toHaveCount(0);
    await expect(page.locator("#grading-batches")).not.toContainText("failed");
    await expect(page.locator("#grading-batches")).not.toContainText("pending");
    const batchCalls = calls.slice(beforeBatches).map(call => JSON.parse(call.messages[1].content));
    for (const data of batchCalls.filter(data => data.batch)) {
      expect(data.task_context.requests.length).toBeGreaterThan(0);
      expect(data.task_context.requests[0].text).toContain("Implement the small");
      expect(data.task_context.requests.every(item => item.turn_number <= data.task_context.current_turn)).toBeTruthy();
      expect(JSON.stringify(data.task_context)).not.toContain("Unrelated task outside");
    }
    expect(batchCalls.filter(data => data.batch?.number === 1)).toHaveLength(1);
    expect(batchCalls.filter(data => data.batch?.number === 2)).toHaveLength(2);
    expect(calls.slice(beforeBatches).every(call => call.messages[1].content.length <= 3000)).toBeTruthy();
    const synthesisCall = calls.slice(beforeBatches).find(call => JSON.parse(call.messages[1].content).evidence_notes && call.response_format.json_schema.name === "DemandGrade");
    expect(synthesisCall).toBeTruthy();
    expect(synthesisCall.max_completion_tokens).toBe(1024);
    await page.screenshot({ path: testInfo.outputPath("batch-grading-mobile.png"), fullPage: true });
    await page.goto(app.url("/settings/grader"));
    await page.getByLabel("Remove saved API key").check();
    await page.getByRole("button", { name: "Save grader configuration" }).click();
    await expect(page.getByText("API key: Not configured", { exact: true })).toBeVisible();
  } finally {
    await new Promise(resolve => provider.close(resolve));
  }
});
