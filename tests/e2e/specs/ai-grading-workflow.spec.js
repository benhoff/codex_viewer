const { execFile } = require('child_process');
const { promisify } = require('util');
const { test, expect } = require('../helpers/fixtures');
const { login } = require('../helpers/auth');

test('large failed runs keep recovery controls accessible and citations readable', async ({ page, app, seed }, testInfo) => {
  await seed.createAdmin();
  const session = await seed.session({ turns: 3, commandsPerTurn: 2 });
  const result = await promisify(execFile)('python3', ['-c', `
import json,sqlite3,sys,os
from datetime import datetime,UTC
from agent_operations_viewer import llm_grader as g
from agent_operations_viewer.task_assessment import task_source,report_for_source,DEFAULT_POLICY
c=sqlite3.connect(os.environ['CODEX_VIEWER_DB']);c.row_factory=sqlite3.Row
s=dict(c.execute('SELECT * FROM sessions WHERE id=?',(sys.argv[1],)).fetchone())
report=report_for_source(task_source(c,s,1,2),DEFAULT_POLICY)
source=next(e for e in report['evidence'] if e.get('role')=='user')
config={**g.DEFAULT_CONFIG,'enabled':True,'model':'test-local-grader'}
g.save_config(c,config)
owner=str(c.execute("SELECT id FROM users WHERE username='admin'").fetchone()[0])
result={'prompt_version':g.PROMPT_VERSION,'calls':[],'acceptance_criteria':'','error':'Grader timed out. The active HTTP request was aborted; no retry was sent.',
        'batches':[{'number':i,'start_turn':1,'end_turn':2,'status':'completed' if i<5 else 'failed' if i==5 else 'pending',
                    'evidence':{'findings':[{'text':'Inspect the recorded request.','event_indexes':[source['event_index']]}],
                                'limitations':'Partial evidence only.'} if i<5 else None} for i in range(1,61)]}
run_id=c.execute('INSERT INTO task_grader_runs(owner_scope,session_id,start_turn,end_turn,created_at,status,evidence_digest,config_json,snapshot_json,result_json) VALUES(?,?,?,?,?,?,?,?,?,?)',
    (owner,s['id'],1,2,datetime.now(UTC).isoformat(),'failed',report['evidence_digest'],json.dumps(config),json.dumps({'report':report}),json.dumps(result))).lastrowid
c.commit();print(json.dumps({'run_id':run_id,'event_index':source['event_index']}))
`, session.session_id], {cwd: app.rootDir, env: app.env});
  const saved = JSON.parse(result.stdout);
  await login(page, app);
  await page.goto(app.url('/assessments'));
  await expect(page.locator('[data-run-id]')).toHaveCount(1);
  await expect(page.locator('[data-run-id]')).toContainText('Turns 1–2');
  await expect(page.locator('[data-run-id]')).toContainText('4 of 60 evidence batches complete');
  await page.getByRole('link', {name: 'Review and resume', exact: true}).click();
  expect(await page.locator('#llm-grader').evaluate(e => e.getBoundingClientRect().top)).toBeGreaterThanOrEqual(150);
  const retry = page.getByRole('button', {name: 'Retry unfinished batches', exact: true});
  await expect(retry).toBeVisible();
  await expect(page.locator('#batch-details')).not.toHaveAttribute('open', '');
  await expect(page.locator('#manual-review')).not.toHaveAttribute('open', '');
  await expect(page.locator('#assessment-accounting')).not.toHaveAttribute('open', '');
  expect((await retry.boundingBox()).y).toBeLessThan(1400);
  expect((await retry.boundingBox()).y).toBeLessThan((await page.locator('#batch-details').boundingBox()).y);
  await page.screenshot({path: testInfo.outputPath('failed-run-desktop.png'), fullPage: true});
  await page.getByText('Batch details and usage · 60 batches', {exact: true}).click();
  await page.locator('#grading-batches details').first().locator('summary').click();
  const citation = page.locator('#grading-batches a').first();
  await citation.click();
  await expect(page.getByRole('dialog')).toBeVisible();
  await expect(page.getByRole('dialog')).toContainText(`Event ${saved.event_index}`);
  await expect(page.getByRole('dialog').locator('pre').first()).not.toBeEmpty();
  await expect(page.getByRole('dialog').getByText('Raw source record', {exact: true})).toBeVisible();
  await page.screenshot({path: testInfo.outputPath('evidence-panel.png')});
  await page.keyboard.press('Escape');
  await expect(page.getByRole('dialog')).not.toBeVisible();
  await expect(citation).toBeFocused();
  await page.setViewportSize({width: 390, height: 844});
  await page.reload();
  await expect(retry).toBeVisible();
  expect((await retry.boundingBox()).y).toBeLessThan(1800);
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBeTruthy();
  await page.screenshot({path: testInfo.outputPath('failed-run-mobile.png'), fullPage: true});
  await page.getByText('Task goal (optional)', {exact: true}).click();
  await page.getByLabel('What should this chunk accomplish?', {exact: true}).fill('A different task');
  await expect(retry).toBeDisabled();
  await page.locator('#assessment-range input[name="end_turn"]').fill('3');
  await page.getByLabel('What should this chunk accomplish?', {exact: true}).fill('');
  await expect(retry).toBeDisabled();
  await expect(page.getByRole('button', {name: 'Submit chunk for AI grading', exact: true})).toBeDisabled();
  await page.goto(app.url(`/sessions/${session.session_id}/assessment/grader/${saved.run_id}#event-${saved.event_index}`));
  await expect(page.getByRole('dialog')).toBeVisible();
  await page.getByRole('button', {name: 'Back to result', exact: true}).click();
  await expect(page.locator('#submit-grading')).toHaveCount(0);
  await expect(page.getByRole('link', {name: 'Open current chunk to resume or regrade', exact: true})).toBeVisible();
});
