# Reverse Proxy Examples

The viewer accepts trusted identity headers from a reverse proxy when `CODEX_VIEWER_AUTH_MODE=proxy` or `CODEX_VIEWER_AUTH_MODE=password_or_proxy`.

Only use proxy auth when the app is reachable exclusively through your trusted proxy. Do not expose the viewer directly to the internet with proxy auth enabled.

## Caddy + Authentik

Files:

- [Caddyfile.authentik.example](Caddyfile.authentik.example)

Viewer env:

```env
CODEX_VIEWER_AUTH_MODE=proxy
CODEX_VIEWER_AUTH_PROXY_USER_HEADER=X-Forwarded-User
CODEX_VIEWER_AUTH_PROXY_NAME_HEADER=X-Forwarded-Name
CODEX_VIEWER_AUTH_PROXY_EMAIL_HEADER=X-Forwarded-Email
# Optional if you want a direct login button on /login:
# CODEX_VIEWER_AUTH_PROXY_LOGIN_URL=https://viewer.example.com
# Optional if you want the in-app logout button to jump to your IdP logout:
# CODEX_VIEWER_AUTH_PROXY_LOGOUT_URL=https://auth.example.com/outpost.goauthentik.io/sign_out
```

The Caddy example copies Authentik's `X-authentik-*` response headers into the viewer's default `X-Forwarded-*` headers, so no header overrides are required.

## Traefik + Authelia

Files:

- [traefik.authelia.dynamic.yml.example](traefik.authelia.dynamic.yml.example)
- [traefik.authelia.viewer-compose.example.yml](traefik.authelia.viewer-compose.example.yml)

Viewer env:

```env
CODEX_VIEWER_AUTH_MODE=proxy
CODEX_VIEWER_AUTH_PROXY_USER_HEADER=Remote-User
CODEX_VIEWER_AUTH_PROXY_NAME_HEADER=Remote-Name
CODEX_VIEWER_AUTH_PROXY_EMAIL_HEADER=Remote-Email
# Optional if you want a direct login button on /login:
# CODEX_VIEWER_AUTH_PROXY_LOGIN_URL=https://viewer.example.com
# Optional if you want the in-app logout button to jump to your IdP logout:
# CODEX_VIEWER_AUTH_PROXY_LOGOUT_URL=https://auth.example.com/logout
```

Authelia's forward-auth middleware returns `Remote-*` headers, so the viewer env must be updated to match.

## Healthcheck

[compose.yml](../../compose.yml) includes a Docker `healthcheck:` that probes `http://127.0.0.1:8000/api/health`. That makes health visible in `docker ps`, Compose, and container monitors without needing a separate probe container.
