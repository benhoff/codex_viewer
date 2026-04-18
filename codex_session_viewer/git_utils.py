from __future__ import annotations

import re
import subprocess
from functools import lru_cache
from pathlib import Path


REMOTE_LINE_RE = re.compile(r"^(?P<name>\S+)\s+(?P<url>\S+)\s+\((?P<kind>fetch|push)\)$")
GITHUB_PATTERNS = [
    re.compile(r"^git@github\.com:(?P<org>[^/]+)/(?P<repo>[^/]+?)(?:\.git)?/?$"),
    re.compile(r"^ssh://git@github\.com/(?P<org>[^/]+)/(?P<repo>[^/]+?)(?:\.git)?/?$"),
    re.compile(r"^https://github\.com/(?P<org>[^/]+)/(?P<repo>[^/]+?)(?:\.git)?/?$"),
    re.compile(r"^http://github\.com/(?P<org>[^/]+)/(?P<repo>[^/]+?)(?:\.git)?/?$"),
]


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

    github_candidates: list[tuple[str, str]] = []
    for name, url, kind in remotes:
        if kind != "fetch":
            continue
        parsed = parse_github_remote(url)
        if parsed is not None:
            github_candidates.append((name, url))

    chosen_url = None
    for name, url in github_candidates:
        if name == "origin":
            chosen_url = url
            break
    if chosen_url is None and github_candidates:
        chosen_url = github_candidates[0][1]

    chosen_org = None
    chosen_repo = None
    if chosen_url:
        chosen_org, chosen_repo = parse_github_remote(chosen_url) or (None, None)

    return {
        "github_remote_url": chosen_url,
        "github_org": chosen_org,
        "github_repo": chosen_repo,
    }


def resolve_git_info(cwd: str | None, raw_git: dict[str, object] | None) -> dict[str, str | None]:
    payload = raw_git if isinstance(raw_git, dict) else {}

    branch = payload.get("branch") if isinstance(payload.get("branch"), str) else None
    commit_hash = payload.get("commit_hash") if isinstance(payload.get("commit_hash"), str) else None
    repository_url = (
        payload.get("repository_url") if isinstance(payload.get("repository_url"), str) else None
    )

    git_probe = probe_git_directory(cwd) if cwd else {}
    github_remote_url = (
        git_probe.get("github_remote_url")
        if isinstance(git_probe.get("github_remote_url"), str)
        else None
    )
    github_org = git_probe.get("github_org") if isinstance(git_probe.get("github_org"), str) else None
    github_repo = git_probe.get("github_repo") if isinstance(git_probe.get("github_repo"), str) else None

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
) -> dict[str, str]:
    cwd_name = Path(cwd).name if cwd else ""
    if github_slug and github_org and github_repo:
        return {
            "kind": "github",
            "key": f"github:{github_slug}",
            "label": f"{github_org}/{github_repo}",
        }

    directory_key = cwd or "unknown-directory"
    repo_name = cwd_name or directory_key
    return {
        "kind": "directory",
        "key": f"directory:{source_host}:{directory_key}",
        "label": f"{source_host}/{repo_name}",
    }
