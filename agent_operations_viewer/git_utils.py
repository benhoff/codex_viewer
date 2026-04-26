from __future__ import annotations

import os
import re
import subprocess
import tomllib
from functools import lru_cache
from pathlib import Path
from urllib.parse import urlsplit


DEFAULT_PROJECT_ROOT_MARKERS = (".git",)
REMOTE_LINE_RE = re.compile(r"^(?P<name>\S+)\s+(?P<url>\S+)\s+\((?P<kind>fetch|push)\)$")
GITHUB_PATTERNS = [
    re.compile(r"^git@github\.com:(?P<org>[^/]+)/(?P<repo>[^/]+?)(?:\.git)?/?$"),
    re.compile(r"^ssh://git@github\.com/(?P<org>[^/]+)/(?P<repo>[^/]+?)(?:\.git)?/?$"),
    re.compile(r"^https://github\.com/(?P<org>[^/]+)/(?P<repo>[^/]+?)(?:\.git)?/?$"),
    re.compile(r"^http://github\.com/(?P<org>[^/]+)/(?P<repo>[^/]+?)(?:\.git)?/?$"),
]
SCP_REMOTE_RE = re.compile(r"^(?:(?P<user>[^@/]+)@)?(?P<host>[^:/]+):(?P<path>.+)$")
WINDOWS_ABSOLUTE_PATH_RE = re.compile(r"^[A-Za-z]:[\\/]")


def parse_github_remote(url: str | None) -> tuple[str, str] | None:
    if not url:
        return None
    for pattern in GITHUB_PATTERNS:
        match = pattern.match(url.strip())
        if match:
            return match.group("org"), match.group("repo")
    return None


def normalize_github_remote(url: str | None) -> dict[str, str] | None:
    parsed = parse_github_remote(url)
    if parsed is None:
        return None
    org, repo = parsed
    slug = f"{org}/{repo}".lower()
    return {
        "org": org,
        "repo": repo,
        "slug": slug,
        "group_key": f"github:{slug}",
        "canonical_url": f"https://github.com/{org}/{repo}",
    }


def codex_home_path() -> Path:
    raw = (os.getenv("CODEX_HOME") or "").strip()
    if raw:
        return Path(raw).expanduser()
    return Path.home() / ".codex"


@lru_cache(maxsize=16)
def load_project_root_markers(codex_home: str | None = None) -> tuple[str, ...]:
    root = Path(codex_home).expanduser() if codex_home else codex_home_path()
    config_path = root / "config.toml"
    try:
        parsed = tomllib.loads(config_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return DEFAULT_PROJECT_ROOT_MARKERS
    except (OSError, tomllib.TOMLDecodeError):
        return DEFAULT_PROJECT_ROOT_MARKERS

    markers = parsed.get("project_root_markers")
    if markers is None:
        return DEFAULT_PROJECT_ROOT_MARKERS
    if not isinstance(markers, list) or not all(isinstance(marker, str) for marker in markers):
        return DEFAULT_PROJECT_ROOT_MARKERS
    return tuple(marker.strip() for marker in markers if marker.strip())


@lru_cache(maxsize=2048)
def detect_project_root(cwd: str, markers: tuple[str, ...]) -> str | None:
    if not cwd or not markers:
        return None

    path = Path(cwd).expanduser()
    base = path if path.is_dir() else path.parent
    if not base:
        return None

    for ancestor in (base, *base.parents):
        for marker in markers:
            if (ancestor / marker).exists():
                return str(ancestor)
    return None


def resolve_project_root(cwd: str | None, *, allow_probe: bool = True) -> str | None:
    if not allow_probe or not isinstance(cwd, str) or not cwd.strip():
        return None
    return detect_project_root(cwd, load_project_root_markers(str(codex_home_path())))


def _strip_git_suffix(value: str) -> str:
    candidate = value.strip().rstrip("/\\")
    if candidate.lower().endswith(".git"):
        candidate = candidate[:-4]
    return candidate.rstrip("/\\")


def _normalize_remote_path_fragment(value: str) -> str:
    return _strip_git_suffix(value.strip().strip("/"))


def _normalize_local_remote_path(value: str) -> str:
    candidate = value.strip()
    if candidate.startswith("~"):
        candidate = str(Path(candidate).expanduser())
    elif WINDOWS_ABSOLUTE_PATH_RE.match(candidate):
        candidate = candidate.replace("\\", "/")
    return _strip_git_suffix(candidate)


def normalize_git_remote(url: str | None) -> dict[str, str | bool] | None:
    candidate = str(url or "").strip()
    if not candidate:
        return None

    if "://" in candidate:
        parsed = urlsplit(candidate)
        scheme = parsed.scheme.lower()
        if scheme in {"http", "https", "ssh", "git"} and parsed.hostname:
            host = parsed.hostname.lower()
            path = _normalize_remote_path_fragment(parsed.path or "")
            if path:
                repo = path.rsplit("/", 1)[-1]
                return {
                    "host": host,
                    "path": path,
                    "repo": repo,
                    "is_local": False,
                }
        if scheme == "file":
            path = _normalize_local_remote_path(parsed.path or parsed.netloc or "")
            if path:
                repo = Path(path).name or path
                return {
                    "host": "",
                    "path": path,
                    "repo": repo,
                    "is_local": True,
                }

    scp_match = SCP_REMOTE_RE.match(candidate)
    if scp_match and "/" in scp_match.group("path"):
        host = scp_match.group("host").lower()
        path = _normalize_remote_path_fragment(scp_match.group("path"))
        if path:
            repo = path.rsplit("/", 1)[-1]
            return {
                "host": host,
                "path": path,
                "repo": repo,
                "is_local": False,
            }

    if candidate.startswith(("/", "./", "../", "~/")) or WINDOWS_ABSOLUTE_PATH_RE.match(candidate):
        path = _normalize_local_remote_path(candidate)
        if path:
            repo = Path(path).name or path
            return {
                "host": "",
                "path": path,
                "repo": repo,
                "is_local": True,
            }

    return None


@lru_cache(maxsize=512)
def probe_git_directory(cwd: str) -> dict[str, object]:
    path = Path(cwd)
    if not path.exists() or not path.is_dir():
        return {}

    try:
        output = subprocess.check_output(
            ["git", "-C", cwd, "remote", "-v"],
            text=True,
            stderr=subprocess.DEVNULL,
            timeout=3,
        )
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
        return {}

    remotes: list[tuple[str, str, str]] = []
    for line in output.splitlines():
        match = REMOTE_LINE_RE.match(line.strip())
        if not match:
            continue
        remotes.append((match.group("name"), match.group("url"), match.group("kind")))

    fetch_candidates: list[tuple[str, str]] = []
    for name, url, kind in remotes:
        if kind != "fetch":
            continue
        fetch_candidates.append((name, url))

    chosen_url = None
    for name, url in fetch_candidates:
        if name == "origin":
            chosen_url = url
            break
    if chosen_url is None and fetch_candidates:
        chosen_url = fetch_candidates[0][1]

    chosen_org = None
    chosen_repo = None
    chosen_github_url = None
    if chosen_url:
        chosen_org, chosen_repo = parse_github_remote(chosen_url) or (None, None)
        if chosen_org and chosen_repo:
            chosen_github_url = chosen_url

    return {
        "repository_url": chosen_url,
        "github_remote_url": chosen_github_url,
        "github_org": chosen_org,
        "github_repo": chosen_repo,
    }


def resolve_git_info(
    cwd: str | None,
    raw_git: dict[str, object] | None,
    *,
    allow_probe: bool = True,
) -> dict[str, str | None]:
    payload = raw_git if isinstance(raw_git, dict) else {}

    branch = payload.get("branch") if isinstance(payload.get("branch"), str) else None
    commit_hash = payload.get("commit_hash") if isinstance(payload.get("commit_hash"), str) else None
    repository_url = (
        payload.get("repository_url") if isinstance(payload.get("repository_url"), str) else None
    )

    git_probe = probe_git_directory(cwd) if cwd and allow_probe else {}
    repository_probe_url = (
        git_probe.get("repository_url")
        if isinstance(git_probe.get("repository_url"), str)
        else None
    )
    github_remote_url = (
        git_probe.get("github_remote_url")
        if isinstance(git_probe.get("github_remote_url"), str)
        else None
    )
    github_org = git_probe.get("github_org") if isinstance(git_probe.get("github_org"), str) else None
    github_repo = git_probe.get("github_repo") if isinstance(git_probe.get("github_repo"), str) else None

    if not repository_url and repository_probe_url:
        repository_url = repository_probe_url
    if not github_remote_url and repository_url:
        parsed = parse_github_remote(repository_url)
        if parsed is not None:
            github_org, github_repo = parsed
            github_remote_url = repository_url

    github_slug = None
    if github_org and github_repo:
        github_slug = f"{github_org}/{github_repo}".lower()

    return {
        "branch": branch,
        "commit_hash": commit_hash,
        "repository_url": repository_url or github_remote_url,
        "github_remote_url": github_remote_url,
        "github_org": github_org,
        "github_repo": github_repo,
        "github_slug": github_slug,
    }


def infer_project_identity(
    source_host: str,
    cwd: str | None,
    github_org: str | None,
    github_repo: str | None,
    github_slug: str | None,
    git_repository_url: str | None,
    project_root: str | None = None,
) -> dict[str, str]:
    cwd_name = Path(cwd).name if cwd else ""
    if github_slug and github_org and github_repo:
        return {
            "kind": "github",
            "key": f"github:{github_slug}",
            "label": f"{github_org}/{github_repo}",
        }

    remote = normalize_git_remote(git_repository_url)
    if remote is not None:
        path = str(remote["path"])
        repo = str(remote["repo"])
        if bool(remote["is_local"]):
            return {
                "kind": "git",
                "key": f"git:{source_host}:file:{path}",
                "label": f"{source_host}/{repo}",
            }

        host = str(remote["host"])
        return {
            "kind": "git",
            "key": f"git:{host}/{path}",
            "label": f"{host}/{path}",
        }

    if project_root:
        project_name = Path(project_root).name or project_root
        return {
            "kind": "project",
            "key": f"project:{source_host}:{project_root}",
            "label": f"{source_host}/{project_name}",
        }

    directory_key = cwd or "unknown-directory"
    repo_name = cwd_name or directory_key
    return {
        "kind": "directory",
        "key": f"directory:{source_host}:{directory_key}",
        "label": f"{source_host}/{repo_name}",
    }
