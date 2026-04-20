const path = require("path");
const { execFile } = require("child_process");

const ROOT_DIR = process.cwd();
const SEED_SCRIPT = path.join(ROOT_DIR, "tests", "e2e", "helpers", "seed_app.py");

function runSeedCommand(app, args) {
  return new Promise((resolve, reject) => {
    execFile(
      "python3",
      [SEED_SCRIPT, ...args],
      {
        cwd: ROOT_DIR,
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
        const text = stdout.trim();
        if (!text) {
          resolve({});
          return;
        }
        try {
          resolve(JSON.parse(text));
        } catch (_error) {
          reject(new Error(`Seed helper returned invalid JSON: ${text}`));
        }
      },
    );
  });
}

function createSeedHelpers(app) {
  return {
    createAdmin(options = {}) {
      const username = options.username || "admin";
      const password = options.password || "Password123!";
      return runSeedCommand(app, ["create-admin", "--username", username, "--password", password]);
    },

    createUser(options = {}) {
      const username = options.username || "viewer";
      const password = options.password || "Password123!";
      const role = options.role || "viewer";
      return runSeedCommand(app, ["create-user", "--username", username, "--password", password, "--role", role]);
    },

    createToken(options = {}) {
      const label = options.label || "E2E token";
      return runSeedCommand(app, ["create-token", "--label", label]);
    },

    heartbeat(options = {}) {
      const args = [
        "seed-heartbeat",
        "--source-host",
        options.sourceHost || "builder-1",
        "--status",
        options.status || "healthy",
      ];
      if (typeof options.uploadCount === "number") {
        args.push("--upload-count", String(options.uploadCount));
      }
      if (typeof options.skipCount === "number") {
        args.push("--skip-count", String(options.skipCount));
      }
      if (typeof options.failCount === "number") {
        args.push("--fail-count", String(options.failCount));
      }
      if (options.lastError) {
        args.push("--last-error", options.lastError);
      }
      return runSeedCommand(app, args);
    },

    session(options = {}) {
      const args = [
        "seed-session",
        "--source-host",
        options.sourceHost || "builder-1",
        "--project-key",
        options.projectKey || "openai/codex-viewer",
        "--project-label",
        options.projectLabel || "openai/codex-viewer",
        "--turns",
        String(options.turns || 3),
        "--commands-per-turn",
        String(options.commandsPerTurn || 1),
        "--session-index",
        String(options.sessionIndex || 1),
      ];
      if (options.githubOrg) {
        args.push("--github-org", options.githubOrg);
      }
      if (options.githubRepo) {
        args.push("--github-repo", options.githubRepo);
      }
      return runSeedCommand(app, args);
    },

    async project(options = {}) {
      const sessionCount = options.sessionCount || 2;
      const sessions = [];
      for (let index = 1; index <= sessionCount; index += 1) {
        sessions.push(
          await this.session({
            ...options,
            sessionIndex: index,
          }),
        );
      }
      return sessions;
    },

    setProjectVisibility(options = {}) {
      return runSeedCommand(app, [
        "set-project-visibility",
        "--group-key",
        options.groupKey || "openai/codex-viewer",
        "--visibility",
        options.visibility || "private",
      ]);
    },

    grantProjectAccess(options = {}) {
      return runSeedCommand(app, [
        "grant-project-access",
        "--group-key",
        options.groupKey || "openai/codex-viewer",
        "--username",
        options.username || "viewer",
        "--role",
        options.role || "viewer",
      ]);
    },
  };
}

module.exports = {
  createSeedHelpers,
};
