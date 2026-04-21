const { expect } = require("@playwright/test");

async function expectNoServerError(page) {
  const body = page.locator("body");
  await expect(body).not.toContainText("Traceback");
  await expect(body).not.toContainText("Internal Server Error");
}

async function expectPageLoad(page, url, options = {}) {
  const response = await page.goto(url, {
    waitUntil: options.waitUntil || "domcontentloaded",
  });

  expect(response, `Expected a navigation response for ${url}`).not.toBeNull();
  expect(
    response.ok(),
    `Expected a successful response for ${url}, got ${response.status()} ${response.statusText()}`,
  ).toBeTruthy();

  const body = page.locator("body");
  await expect(body).toBeVisible();

  if (options.expectedPathname) {
    expect(new URL(page.url()).pathname).toBe(options.expectedPathname);
  }

  if (options.expectedText) {
    await expect(body).toContainText(options.expectedText);
  }

  await expectNoServerError(page);
}

async function expectHorizontalFit(page, extraPixels = 8) {
  const fits = await page.evaluate((slack) => {
    const doc = document.documentElement;
    return doc.scrollWidth <= window.innerWidth + slack;
  }, extraPixels);
  expect(fits).toBeTruthy();
}

module.exports = {
  expectNoServerError,
  expectPageLoad,
  expectHorizontalFit,
};
