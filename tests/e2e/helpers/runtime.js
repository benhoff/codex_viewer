const http = require("http");
const net = require("net");
const { execFile } = require("child_process");

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function getFreePort() {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.unref();
    server.on("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      const port = typeof address === "object" && address ? address.port : null;
      server.close((error) => {
        if (error) {
          reject(error);
          return;
        }
        if (!port) {
          reject(new Error("Unable to allocate a test port."));
          return;
        }
        resolve(port);
      });
    });
  });
}

function runAppCommand(app, args) {
  return new Promise((resolve, reject) => {
    execFile(
      "python3",
      ["-m", "codex_session_viewer", ...args],
      {
        cwd: app.rootDir,
        env: {
          ...process.env,
          ...app.env,
        },
      },
      (error, stdout, stderr) => {
        if (error) {
          reject(new Error(`${stderr || stdout || error.message}`.trim()));
          return;
        }
        resolve({
          stdout: stdout.trim(),
          stderr: stderr.trim(),
        });
      },
    );
  });
}

async function startWebhookServer() {
  const port = await getFreePort();
  const deliveries = [];
  const server = http.createServer((request, response) => {
    const chunks = [];
    request.on("data", (chunk) => chunks.push(chunk));
    request.on("end", () => {
      const rawBody = Buffer.concat(chunks).toString("utf-8");
      let body = rawBody;
      try {
        body = rawBody ? JSON.parse(rawBody) : null;
      } catch (_error) {
        body = rawBody;
      }
      deliveries.push({
        method: request.method || "POST",
        path: request.url || "/",
        headers: request.headers,
        body,
        rawBody,
      });
      response.writeHead(204);
      response.end();
    });
  });

  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(port, "127.0.0.1", () => resolve());
  });

  return {
    url: `http://127.0.0.1:${port}/webhook`,
    deliveries,
    async waitForCount(count, timeoutMs = 5_000) {
      const deadline = Date.now() + timeoutMs;
      while (Date.now() < deadline) {
        if (deliveries.length >= count) {
          return deliveries.slice();
        }
        await delay(50);
      }
      throw new Error(`Timed out waiting for ${count} webhook deliveries; saw ${deliveries.length}.`);
    },
    async close() {
      await new Promise((resolve, reject) => {
        server.close((error) => {
          if (error) {
            reject(error);
            return;
          }
          resolve();
        });
      });
    },
  };
}

module.exports = {
  runAppCommand,
  startWebhookServer,
};
