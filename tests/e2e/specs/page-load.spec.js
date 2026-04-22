const { test } = require("../helpers/fixtures");
const { login } = require("../helpers/auth");
const { expectPageLoad } = require("../helpers/assertions");

test("login page loads for an initialized install", async ({ page, app, seed }) => {
  await seed.createAdmin({
    username: "admin",
    password: "Password123!",
  });

  await expectPageLoad(page, app.url("/login"), {
    expectedPathname: "/login",
    expectedText: "Sign In",
  });
});

test("authenticated html routes load without server errors", async ({ page, app, seed }) => {
  await seed.createAdmin({
    username: "admin",
    password: "Password123!",
  });
  await seed.createToken({
    label: "E2E smoke token",
  });
  const session = await seed.session({
    sourceHost: "builder-1",
    projectKey: "openai/codex-viewer",
    projectLabel: "openai/codex-viewer",
    githubOrg: "openai",
    githubRepo: "codex-viewer",
    turns: 6,
    commandsPerTurn: 1,
  });
  await seed.heartbeat({
    sourceHost: "builder-1",
    status: "healthy",
    uploadCount: 1,
  });

  await login(page, app, {
    username: "admin",
    password: "Password123!",
  });

  const routes = [
    { name: "dashboard", path: "/", expectedPathname: "/" },
    { name: "dashboard stream legacy redirect", path: "/stream", expectedPathname: "/" },
    { name: "search", path: "/search?q=synthetic", expectedPathname: "/search" },
    { name: "queue", path: "/queue", expectedPathname: "/queue" },
    { name: "queue resolved filter", path: "/queue?status=resolved", expectedPathname: "/queue" },
    { name: "queue project grouping", path: "/queue?group=project", expectedPathname: "/queue" },
    { name: "queue project filter", path: "/queue?project=openai/codex-viewer", expectedPathname: "/queue" },
    { name: "machines", path: "/machines", expectedPathname: "/machines" },
    { name: "machines legacy redirect", path: "/remotes", expectedPathname: "/machines" },
    { name: "machine audit", path: "/machines/builder-1/audit", expectedPathname: "/machines/builder-1/audit" },
    {
      name: "machine audit legacy redirect",
      path: "/remotes/builder-1/audit",
      expectedPathname: "/machines/builder-1/audit",
    },
    { name: "project detail", path: "/openai/codex-viewer", expectedPathname: "/openai/codex-viewer" },
    {
      name: "project detail legacy query redirect",
      path: "/groups?key=openai/codex-viewer",
      expectedPathname: "/openai/codex-viewer",
    },
    {
      name: "project detail legacy key redirect",
      path: "/projects/key/openai/codex-viewer",
      expectedPathname: "/openai/codex-viewer",
    },
    {
      name: "project detail legacy github redirect",
      path: "/projects/github/openai/codex-viewer",
      expectedPathname: "/openai/codex-viewer",
    },
    {
      name: "project detail legacy redirect",
      path: "/projects/openai/codex-viewer",
      expectedPathname: "/openai/codex-viewer",
    },
    {
      name: "project edit",
      path: "/openai/codex-viewer/edit",
      expectedPathname: "/openai/codex-viewer/edit",
    },
    {
      name: "project edit legacy redirect",
      path: "/projects/edit?key=openai/codex-viewer",
      expectedPathname: "/openai/codex-viewer/edit",
    },
    {
      name: "project edit legacy path redirect",
      path: "/projects/openai/codex-viewer/edit",
      expectedPathname: "/openai/codex-viewer/edit",
    },
    {
      name: "project environment audit",
      path: "/openai/codex-viewer/environment",
      expectedPathname: "/openai/codex-viewer/environment",
    },
    {
      name: "project environment legacy redirect",
      path: "/projects/openai/codex-viewer/environment",
      expectedPathname: "/openai/codex-viewer/environment",
    },
    {
      name: "project stream",
      path: "/openai/codex-viewer/stream",
      expectedPathname: "/openai/codex-viewer/stream",
    },
    {
      name: "project stream legacy redirect",
      path: "/projects/openai/codex-viewer/stream",
      expectedPathname: "/openai/codex-viewer/stream",
    },
    {
      name: "project queue redirect",
      path: "/openai/codex-viewer/queue",
      expectedPathname: "/queue",
    },
    {
      name: "project queue legacy redirect",
      path: "/projects/openai/codex-viewer/queue",
      expectedPathname: "/queue",
    },
    { name: "session conversation", path: `/sessions/${session.session_id}`, expectedPathname: `/sessions/${session.session_id}` },
    {
      name: "session audit",
      path: `/sessions/${session.session_id}?view=audit`,
      expectedPathname: `/sessions/${session.session_id}`,
    },
    { name: "settings", path: "/settings", expectedPathname: "/settings" },
  ];

  for (const route of routes) {
    await test.step(route.name, async () => {
      await expectPageLoad(page, app.url(route.path), {
        expectedPathname: route.expectedPathname,
      });
    });
  }
});
