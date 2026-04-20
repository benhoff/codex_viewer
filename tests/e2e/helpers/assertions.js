const { expect } = require("@playwright/test");

async function expectNoServerError(page) {
  const body = page.locator("body");
  await expect(body).not.toContainText("Traceback");
  await expect(body).not.toContainText("Internal Server Error");
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
  expectHorizontalFit,
};
