const { expect } = require("@playwright/test");

const { login } = require("./auth");

async function loginAs(page, app, options = {}) {
  await page.context().clearCookies();
  await login(page, app, options);
}

async function expectLoginFailure(page, app, options = {}) {
  const username = options.username || "admin";
  const password = options.password || "Password123!";
  const expectedError = options.expectedError || "Invalid username or password.";
  await page.context().clearCookies();
  await page.goto(app.url("/login"));
  await page.locator('input[name="username"]').fill(username);
  await page.locator('input[name="password"]').fill(password);
  await page.getByRole("button", { name: "Sign In" }).click();
  await expect(page).toHaveURL(/\/login$/);
  await expect(page.locator("body")).toContainText(expectedError);
}

async function expectPageStatus(page, url, expectedStatus, expectedText) {
  const response = await page.goto(url, {
    waitUntil: "domcontentloaded",
  });

  expect(response, `Expected a navigation response for ${url}`).not.toBeNull();
  expect(response.status()).toBe(expectedStatus);

  if (expectedText) {
    await expect(page.locator("body")).toContainText(expectedText);
  }
}

function syncHeaders(rawToken, sourceHost) {
  const headers = {
    authorization: `Bearer ${rawToken}`,
  };
  if (sourceHost) {
    headers["x-codex-viewer-host"] = sourceHost;
  }
  return headers;
}

async function fetchWithSession(page, url, responseType = "text") {
  return page.evaluate(
    async ({ fetchUrl, type }) => {
      const response = await fetch(fetchUrl, {
        credentials: "include",
      });
      const headers = {
        contentType: response.headers.get("content-type") || "",
        contentDisposition: response.headers.get("content-disposition") || "",
      };

      if (type === "json") {
        const text = await response.text();
        let json = null;
        try {
          json = text ? JSON.parse(text) : null;
        } catch (_error) {
          json = null;
        }
        return {
          status: response.status,
          ok: response.ok,
          headers,
          text,
          json,
        };
      }

      if (type === "bytes") {
        const bytes = new Uint8Array(await response.arrayBuffer());
        return {
          status: response.status,
          ok: response.ok,
          headers,
          byteLength: bytes.length,
          firstBytes: Array.from(bytes.slice(0, 4)),
        };
      }

      return {
        status: response.status,
        ok: response.ok,
        headers,
        text: await response.text(),
      };
    },
    { fetchUrl: url, type: responseType },
  );
}

module.exports = {
  expectLoginFailure,
  expectPageStatus,
  fetchWithSession,
  loginAs,
  syncHeaders,
};
