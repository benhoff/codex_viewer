const base = require("@playwright/test");

const { startServer, stopServer, cleanupServer } = require("./server");
const { createSeedHelpers } = require("./seed");

const test = base.test.extend({
  app: async ({}, use, testInfo) => {
    const app = await startServer({
      rootDir: process.cwd(),
      outputDir: testInfo.outputPath("server"),
    });
    try {
      await use(app);
    } finally {
      await stopServer(app);
      if (process.env.E2E_KEEP_ARTIFACTS === "1" || testInfo.status !== testInfo.expectedStatus) {
        return;
      }
      cleanupServer(app);
    }
  },

  seed: async ({ app }, use) => {
    await use(createSeedHelpers(app));
  },
});

module.exports = {
  test,
  expect: base.expect,
};
