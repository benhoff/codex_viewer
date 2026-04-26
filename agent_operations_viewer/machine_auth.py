from __future__ import annotations

import base64
import hashlib
import secrets
from dataclasses import dataclass
from datetime import UTC, datetime

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)


MACHINE_ID_HEADER = "X-Codex-Viewer-Machine-Id"
MACHINE_TIMESTAMP_HEADER = "X-Codex-Viewer-Machine-Timestamp"
MACHINE_NONCE_HEADER = "X-Codex-Viewer-Machine-Nonce"
MACHINE_SIGNATURE_HEADER = "X-Codex-Viewer-Machine-Signature"
MACHINE_BODY_SHA256_HEADER = "X-Codex-Viewer-Machine-Body-Sha256"
MACHINE_AUTH_WINDOW_SECONDS = 300


@dataclass(slots=True)
class MachineKeyPair:
    private_key: str
    public_key: str


def utc_now_iso() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat()


def trimmed(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def parse_timestamp(value: str | None) -> datetime | None:
    candidate = trimmed(value)
    if not candidate:
        return None
    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def timestamp_is_fresh(
    value: str | None,
    *,
    now: datetime | None = None,
    max_skew_seconds: int = MACHINE_AUTH_WINDOW_SECONDS,
) -> bool:
    parsed = parse_timestamp(value)
    if parsed is None:
        return False
    reference = (now or datetime.now(tz=UTC)).astimezone(UTC)
    age_seconds = abs((reference - parsed).total_seconds())
    return age_seconds <= max_skew_seconds


def _b64encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _b64decode(value: str) -> bytes:
    candidate = value.encode("ascii")
    padding = b"=" * ((4 - (len(candidate) % 4)) % 4)
    return base64.urlsafe_b64decode(candidate + padding)


def generate_machine_keypair() -> MachineKeyPair:
    private_key = Ed25519PrivateKey.generate()
    public_key = private_key.public_key()
    return MachineKeyPair(
        private_key=_b64encode(
            private_key.private_bytes(
                encoding=serialization.Encoding.Raw,
                format=serialization.PrivateFormat.Raw,
                encryption_algorithm=serialization.NoEncryption(),
            )
        ),
        public_key=_b64encode(
            public_key.public_bytes(
                encoding=serialization.Encoding.Raw,
                format=serialization.PublicFormat.Raw,
            )
        ),
    )


def public_key_from_private_key(private_key: str) -> str:
    key = Ed25519PrivateKey.from_private_bytes(_b64decode(private_key))
    return _b64encode(
        key.public_key().public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        )
    )


def body_sha256_hex(raw_body: bytes | None) -> str:
    payload = raw_body or b""
    return hashlib.sha256(payload).hexdigest()


def generate_machine_nonce() -> str:
    return secrets.token_urlsafe(18)


def canonical_request(
    *,
    machine_id: str,
    method: str,
    path: str,
    body_sha256: str,
    timestamp: str,
    nonce: str,
    source_host: str | None,
) -> bytes:
    return "\n".join(
        [
            trimmed(machine_id) or "",
            method.strip().upper(),
            path.strip(),
            trimmed(body_sha256) or "",
            trimmed(timestamp) or "",
            trimmed(nonce) or "",
            trimmed(source_host) or "",
        ]
    ).encode("utf-8")


def build_machine_auth_headers(
    *,
    private_key: str,
    machine_id: str,
    method: str,
    path: str,
    raw_body: bytes | None,
    source_host: str | None,
    timestamp: str | None = None,
    nonce: str | None = None,
) -> dict[str, str]:
    effective_timestamp = trimmed(timestamp) or utc_now_iso()
    effective_nonce = trimmed(nonce) or generate_machine_nonce()
    body_sha256 = body_sha256_hex(raw_body)
    signing_input = canonical_request(
        machine_id=machine_id,
        method=method,
        path=path,
        body_sha256=body_sha256,
        timestamp=effective_timestamp,
        nonce=effective_nonce,
        source_host=source_host,
    )
    signature = Ed25519PrivateKey.from_private_bytes(
        _b64decode(private_key)
    ).sign(signing_input)
    return {
        MACHINE_ID_HEADER: machine_id,
        MACHINE_TIMESTAMP_HEADER: effective_timestamp,
        MACHINE_NONCE_HEADER: effective_nonce,
        MACHINE_SIGNATURE_HEADER: _b64encode(signature),
        MACHINE_BODY_SHA256_HEADER: body_sha256,
    }


def verify_machine_request_signature(
    *,
    public_key: str,
    machine_id: str,
    method: str,
    path: str,
    raw_body: bytes | None,
    source_host: str | None,
    timestamp: str,
    nonce: str,
    signature: str,
    body_sha256: str | None = None,
) -> bool:
    effective_body_sha256 = trimmed(body_sha256) or body_sha256_hex(raw_body)
    if effective_body_sha256 != body_sha256_hex(raw_body):
        return False
    signing_input = canonical_request(
        machine_id=machine_id,
        method=method,
        path=path,
        body_sha256=effective_body_sha256,
        timestamp=timestamp,
        nonce=nonce,
        source_host=source_host,
    )
    try:
        Ed25519PublicKey.from_public_bytes(_b64decode(public_key)).verify(
            _b64decode(signature),
            signing_input,
        )
    except Exception:
        return False
    return True
