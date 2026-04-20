const fs = require("fs");
const os = require("os");
const path = require("path");
const net = require("net");
const { spawn } = require("child_process");

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

async function waitForHealth(baseURL, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  let lastError = null;
  while (Date.now() < deadline) {
    try {
      const response = await fetch(`${baseURL}/api/health`);
      if (response.ok) {
        const payload = await response.json();
        if (payload && payload.status === "ok") {
          return;
        }
      }
      lastError = new Error(`Health endpoint returned ${response.status}`);
    } catch (error) {
      lastError = error;
    }
    await delay(250);
  }
  throw lastError || new Error("Timed out waiting for the test server.");
}

function buildPythonPath(rootDir) {
  return [path.join(rootDir, ".deps"), rootDir].join(path.delimiter);
}

async function startServer({ rootDir, outputDir, authMode = "password", syncMode = "remote" }) {
  const port = await getFreePort();
  const runDir = fs.mkdtempSync(path.join(os.tmpdir(), "codex-viewer-e2e-"));
  const dataDir = path.join(runDir, "data");
  fs.mkdirSync(dataDir, { recursive: true });
  fs.mkdirSync(outputDir, { recursive: true });
  const logPath = path.join(outputDir, "server.log");
  const logStream = fs.createWriteStream(logPath, { flags: "a" });
  const env = {
    ...process.env,
    PYTHONPATH: buildPythonPath(rootDir),
    CODEX_VIEWER_DATA_DIR: dataDir,
    CODEX_VIEWER_AUTH_MODE: authMode,
    CODEX_VIEWER_SYNC_MODE: syncMode,
    CODEX_VIEWER_SESSION_SECRET: "e2e-session-secret",
    CODEX_VIEWER_LOG_LEVEL: "warning",
  };
  const child = spawn(
    "python3",
    ["-m", "codex_session_viewer", "serve", "--host", "127.0.0.1", "--port", String(port), "--no-sync"],
    {
      cwd: rootDir,
      env,
      stdio: ["ignore", "pipe", "pipe"],
    },
  );

  child.stdout.on("data", (chunk) => logStream.write(chunk));
  child.stderr.on("data", (chunk) => logStream.write(chunk));

  const baseURL = `http://127.0.0.1:${port}`;
  try {
    await waitForHealth(baseURL, 30_000);
  } catch (error) {
    child.kill("SIGKILL");
    logStream.end();
    throw error;
  }

  return {
    rootDir,
    runDir,
    dataDir,
    baseURL,
    port,
    logPath,
    env,
    process: child,
    logStream,
    url(pathname = "/") {
      return new URL(pathname, `${baseURL}/`).toString();
    },
  };
}

async function stopServer(app) {
  if (!app || !app.process || app.process.killed) {
    if (app && app.logStream) {
      app.logStream.end();
    }
    return;
  }

  await new Promise((resolve) => {
    let settled = false;
    const finish = () => {
      if (settled) {
        return;
      }
      settled = true;
      resolve();
    };
    const timer = setTimeout(() => {
      app.process.kill("SIGKILL");
      finish();
    }, 5_000);
    app.process.once("exit", () => {
      clearTimeout(timer);
      finish();
    });
    app.process.kill("SIGTERM");
  });

  if (app.logStream) {
    app.logStream.end();
  }
}

function cleanupServer(app) {
  if (!app) {
    return;
  }
  fs.rmSync(app.runDir, { recursive: true, force: true });
}

module.exports = {
  startServer,
  stopServer,
  cleanupServer,
};
