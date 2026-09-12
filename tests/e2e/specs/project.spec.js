const { test, expect } = require('../helpers/fixtures');
const { login } = require('../helpers/auth');
const { expectHorizontalFit, expectNoServerError } = require('../helpers/assertions');

async function project(seed, sessions = 3) {
  await seed.createAdmin();
  await seed.heartbeat({ sourceHost: 'builder-1', uploadCount: sessions });
  for (let index = 1; index <= sessions; index++) {
    await seed.session({ githubOrg: 'openai', githubRepo: 'codex-viewer', sessionIndex: index, turns: 8 });
  }
}

test('project timeline loads older turns and returns from the exact session turn', async ({ page, app, seed }, testInfo) => {
  await project(seed);
  await login(page, app);
  await page.goto(app.url('/openai/codex-viewer'));
  const turns = page.locator('[data-timeline-turn]');
  await expect(page.getByRole('heading', { name: 'Turn timeline' })).toBeVisible();
  await expect(turns).toHaveCount(10);
  await page.screenshot({ path: testInfo.outputPath('project-desktop.png'), fullPage: true });
  await expect(page.getByRole('heading', { name: 'Sessions', exact: true })).toHaveCount(0);
  await expect(page.getByRole('link', { name: 'Open audit', exact: true })).toHaveCount(0);
  await page.getByRole('link', { name: 'Load older turns' }).click();
  await expect(turns).toHaveCount(20);
  await page.getByRole('link', { name: 'Load older turns' }).click();
  await expect(turns).toHaveCount(24);
  const timestamps = await turns.locator('time').evaluateAll((nodes) => nodes.map((node) => Date.parse(node.dateTime)));
  expect(timestamps).toEqual([...timestamps].sort((a, b) => b - a));
  const keys = await turns.evaluateAll((nodes) => nodes.map((node) => node.dataset.timelineTurn));
  expect(new Set(keys).size).toBe(24);
  await expect(page.getByRole('link', { name: 'Load older turns' })).toHaveCount(0);

  const link = turns.nth(14).locator('[data-turn-session-link]');
  const href = await link.getAttribute('href');
  const turnNumber = new URL(href, app.baseURL).searchParams.get('turn');
  await link.scrollIntoViewIfNeeded();
  const scrollY = await page.evaluate(() => window.scrollY);
  await link.click();
  await expect(page).toHaveURL(app.url(href));
  await expect(page.locator(`[data-turn-card][data-turn-number="${turnNumber}"]`)).toBeVisible();
  await page.goBack();
  await expect(turns).toHaveCount(24);
  await expect.poll(() => page.evaluate((position) => Math.abs(window.scrollY - position), scrollY)).toBeLessThan(30);
  await expectNoServerError(page);
});

test('older turns remain retryable after a failed request', async ({ page, app, seed }) => {
  await project(seed, 2);
  await login(page, app);
  await page.goto(app.url('/openai/codex-viewer'));
  await page.route('**/*?turns_page=2', (route) => route.fulfill({ status: 503, body: 'Unavailable' }));
  await page.getByRole('link', { name: 'Load older turns' }).click();
  await expect(page.getByRole('status')).toContainText('Try again');
  await expect(page.locator('[data-timeline-turn]')).toHaveCount(10);
  await page.unroute('**/*?turns_page=2');
  await page.getByRole('link', { name: 'Load older turns' }).click();
  await expect(page.locator('[data-timeline-turn]')).toHaveCount(16);
});

test('project navigation and pagination work without JavaScript', async ({ browser, app, seed }) => {
  await project(seed, 7);
  const context = await browser.newContext({ javaScriptEnabled: false });
  const page = await context.newPage();
  try {
    await login(page, app);
    await page.goto(app.url('/openai/codex-viewer'));
    await page.getByRole('link', { name: 'Load older turns' }).click();
    await expect(page).toHaveURL(/turns_page=2$/);
    await expect(page.locator('[data-timeline-turn]')).toHaveCount(10);
    await page.getByRole('link', { name: 'Newer turns', exact: true }).click();
    await expect(page).toHaveURL(/turns_page=1$/);
    await page.getByRole('navigation', { name: 'Project views' }).getByRole('link', { name: 'Sessions', exact: true }).click();
    await expect(page.getByRole('heading', { name: 'Sessions', exact: true })).toBeVisible();
    await expect(page.locator('[data-project-timeline]')).toHaveCount(0);
    await page.getByRole('link', { name: 'Older sessions' }).click();
    await expect(page).toHaveURL(/view=sessions&sessions_page=2$/);
    await expect(page.getByRole('link', { name: 'Newer sessions' })).toBeVisible();
    await page.getByRole('navigation', { name: 'Project views' }).getByRole('link', { name: 'Activity' }).click();
    await expect(page.locator('[data-timeline-turn]')).toHaveCount(10);
    await expectNoServerError(page);
  } finally {
    await context.close();
  }
});

test('mobile project timeline leaves readable width for turn text', async ({ page, app, seed }, testInfo) => {
  await project(seed, 1);
  await login(page, app);
  for (const width of [390, 320]) {
    await page.setViewportSize({ width, height: 844 });
    await page.goto(app.url('/openai/codex-viewer'));
    await expectHorizontalFit(page, 0);
    const link = page.locator('[data-turn-session-link]').first();
    expect((await link.boundingBox()).width).toBeGreaterThan(width - 90);
    await expect(page.locator('[data-timeline-turn]').first()).not.toContainText('0 commands');
  }
  await page.screenshot({ path: testInfo.outputPath('project-mobile.png'), fullPage: true });
});


test('project history and session browsing enforce private project access', async ({ page, app, seed }) => {
  await project(seed, 2);
  await seed.createUser({ username: 'viewer' });
  await seed.setProjectVisibility({ visibility: 'private' });
  await login(page, app, { username: 'viewer' });
  for (const query of ['', '?turns_page=2', '?view=sessions']) {
    const response = await page.goto(app.url(`/openai/codex-viewer${query}`));
    expect(response.status()).toBe(404);
    await expect(page.locator('[data-timeline-turn]')).toHaveCount(0);
  }
  await seed.grantProjectAccess({ username: 'viewer' });
  await page.goto(app.url('/openai/codex-viewer'));
  await expect(page.locator('[data-timeline-turn]')).toHaveCount(10);
  await expect(page.getByLabel('Project menu')).toHaveCount(0);
  await page.getByRole('link', { name: 'Load older turns' }).click();
  await expect(page.locator('[data-timeline-turn]')).toHaveCount(16);
});

for (const action of ['Ignore', 'Delete']) {
  test(`project menu keeps edit and ${action.toLowerCase()} available`, async ({ page, app, seed }) => {
    await project(seed, 1);
    await login(page, app);
    const projectURL = app.url('/openai/codex-viewer');
    await page.goto(projectURL);
    await page.getByLabel('Project menu').click();
    await page.getByRole('link', { name: 'Edit project', exact: true }).click();
    await expect(page).toHaveURL(`${projectURL}/edit`);
    await page.goto(projectURL);
    await page.getByLabel('Project menu').click();
    page.once('dialog', (dialog) => dialog.dismiss());
    await page.getByRole('button', { name: action, exact: true }).click();
    await expect(page).toHaveURL(projectURL);
    page.once('dialog', (dialog) => dialog.accept());
    await page.getByRole('button', { name: action, exact: true }).click();
    // A fresh test installation can return to onboarding once its only project is removed.
    await expect(page).toHaveURL(new RegExp(`^${app.baseURL}/(?:setup)?$`));
    const response = await page.goto(projectURL);
    expect(response.status()).toBe(404);
  });
}
