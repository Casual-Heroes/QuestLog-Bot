"""Verification primitives for signed internal API requests.

The website signs a canonical representation of each request with Ed25519.
Warden stores only public keys, so a bot host compromise does not disclose a
credential that can be used to impersonate dashboard users.
"""

from __future__ import annotations

import base64
import hashlib
import json
import re
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Iterable, Mapping

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey


AUTH_VERSION = "1"
DEFAULT_MAX_CLOCK_SKEW_SECONDS = 60
DEFAULT_REPLAY_CACHE_SIZE = 10_000

_KEY_ID_RE = re.compile(r"^[A-Za-z0-9._-]{1,64}$")
_SCOPE_RE = re.compile(r"^[a-z][a-z0-9_.:-]{0,63}$")
_NONCE_RE = re.compile(r"^[A-Za-z0-9_-]{22,64}$")
_ACTOR_RE = re.compile(r"^[1-9][0-9]{0,18}$")
_TIMESTAMP_RE = re.compile(r"^[0-9]{10,11}$")


class AuthenticationError(ValueError):
    """A signed request could not be authenticated."""


@dataclass(frozen=True)
class TrustedSigner:
    public_key: Ed25519PublicKey
    scopes: frozenset[str]


@dataclass(frozen=True)
class AuthenticationContext:
    key_id: str
    scope: str
    actor_id: str | None


def _decode_base64url(value: str, expected_length: int, field_name: str) -> bytes:
    if not isinstance(value, str) or not value:
        raise AuthenticationError(f"Missing {field_name}")
    try:
        decoded = base64.b64decode(
            value + "=" * (-len(value) % 4),
            altchars=b"-_",
            validate=True,
        )
    except (ValueError, TypeError) as exc:
        raise AuthenticationError(f"Invalid {field_name}") from exc
    if len(decoded) != expected_length:
        raise AuthenticationError(f"Invalid {field_name}")
    return decoded


def load_trusted_signers(raw_config: str | None) -> dict[str, TrustedSigner]:
    """Load a strict key-id to public-key and scope mapping from JSON."""
    if not raw_config:
        return {}
    try:
        config = json.loads(raw_config)
    except json.JSONDecodeError as exc:
        raise RuntimeError("WARDEN_API_TRUSTED_SIGNERS must be valid JSON") from exc
    if not isinstance(config, dict) or not config:
        raise RuntimeError("WARDEN_API_TRUSTED_SIGNERS must be a non-empty JSON object")

    signers: dict[str, TrustedSigner] = {}
    for key_id, entry in config.items():
        if not isinstance(key_id, str) or not _KEY_ID_RE.fullmatch(key_id):
            raise RuntimeError("WARDEN_API_TRUSTED_SIGNERS contains an invalid key ID")
        if not isinstance(entry, dict) or set(entry) != {"public_key", "scopes"}:
            raise RuntimeError(
                f"Trusted signer {key_id!r} must contain only public_key and scopes"
            )
        scopes = entry["scopes"]
        if (
            not isinstance(scopes, list)
            or not scopes
            or any(not isinstance(scope, str) or not _SCOPE_RE.fullmatch(scope) for scope in scopes)
        ):
            raise RuntimeError(f"Trusted signer {key_id!r} has invalid scopes")
        if len(scopes) != len(set(scopes)):
            raise RuntimeError(f"Trusted signer {key_id!r} has duplicate scopes")
        try:
            public_bytes = _decode_base64url(entry["public_key"], 32, "public key")
            public_key = Ed25519PublicKey.from_public_bytes(public_bytes)
        except AuthenticationError as exc:
            raise RuntimeError(f"Trusted signer {key_id!r} has an invalid public key") from exc
        signers[key_id] = TrustedSigner(public_key, frozenset(scopes))
    return signers


def canonical_request(
    *,
    key_id: str,
    timestamp: str,
    nonce: str,
    scope: str,
    actor_id: str,
    method: str,
    path: str,
    body: bytes,
) -> bytes:
    """Return the protocol v1 canonical request bytes."""
    body_digest = hashlib.sha256(body).hexdigest()
    fields = (
        f"v{AUTH_VERSION}",
        key_id,
        timestamp,
        nonce,
        scope,
        actor_id,
        method.upper(),
        path,
        body_digest,
    )
    return "\n".join(fields).encode("utf-8")


class ReplayCache:
    """Bounded, expiring cache of successfully authenticated nonces."""

    def __init__(self, max_entries: int = DEFAULT_REPLAY_CACHE_SIZE):
        if max_entries < 1:
            raise ValueError("max_entries must be positive")
        self._max_entries = max_entries
        self._entries: OrderedDict[tuple[str, str], float] = OrderedDict()

    def remember(self, key_id: str, nonce: str, *, now: float, ttl: int) -> None:
        while self._entries:
            _, oldest_expiry = next(iter(self._entries.items()))
            if oldest_expiry > now:
                break
            self._entries.popitem(last=False)

        cache_key = (key_id, nonce)
        if cache_key in self._entries:
            raise AuthenticationError("Request replay detected")
        self._entries[cache_key] = now + ttl
        while len(self._entries) > self._max_entries:
            self._entries.popitem(last=False)


def verify_signed_request(
    *,
    headers: Mapping[str, str],
    method: str,
    path: str,
    body: bytes,
    expected_scope: str,
    actor_required: bool,
    signers: Mapping[str, TrustedSigner],
    replay_cache: ReplayCache,
    now: float | None = None,
    max_clock_skew_seconds: int = DEFAULT_MAX_CLOCK_SKEW_SECONDS,
) -> AuthenticationContext:
    """Verify one signed request and return its authenticated identity."""
    if headers.get("X-Warden-Auth-Version", "") != AUTH_VERSION:
        raise AuthenticationError("Unsupported authentication version")

    key_id = headers.get("X-Warden-Key-Id", "")
    timestamp = headers.get("X-Warden-Timestamp", "")
    nonce = headers.get("X-Warden-Nonce", "")
    scope = headers.get("X-Warden-Scope", "")
    actor_id = headers.get("X-Warden-Actor-Id", "")
    signature_text = headers.get("X-Warden-Signature", "")

    if not _KEY_ID_RE.fullmatch(key_id):
        raise AuthenticationError("Invalid key ID")
    if not _NONCE_RE.fullmatch(nonce):
        raise AuthenticationError("Invalid nonce")
    if not _SCOPE_RE.fullmatch(scope) or scope != expected_scope:
        raise AuthenticationError("Invalid request scope")
    if actor_id and not _ACTOR_RE.fullmatch(actor_id):
        raise AuthenticationError("Invalid actor ID")
    if actor_required and not actor_id:
        raise AuthenticationError("An authenticated actor is required")

    if not _TIMESTAMP_RE.fullmatch(timestamp):
        raise AuthenticationError("Invalid timestamp")
    try:
        timestamp_value = int(timestamp)
    except (TypeError, ValueError) as exc:
        raise AuthenticationError("Invalid timestamp") from exc
    current_time = time.time() if now is None else now
    if abs(current_time - timestamp_value) > max_clock_skew_seconds:
        raise AuthenticationError("Request timestamp is outside the allowed window")

    signer = signers.get(key_id)
    if signer is None:
        raise AuthenticationError("Unknown signing key")
    if scope not in signer.scopes:
        raise AuthenticationError("Signing key is not authorized for this scope")

    signature = _decode_base64url(signature_text, 64, "signature")
    canonical = canonical_request(
        key_id=key_id,
        timestamp=timestamp,
        nonce=nonce,
        scope=scope,
        actor_id=actor_id,
        method=method,
        path=path,
        body=body,
    )
    try:
        signer.public_key.verify(signature, canonical)
    except InvalidSignature as exc:
        raise AuthenticationError("Invalid signature") from exc

    replay_cache.remember(
        key_id,
        nonce,
        now=current_time,
        ttl=max_clock_skew_seconds * 2,
    )
    return AuthenticationContext(key_id, scope, actor_id or None)


def validate_required_scopes(
    signers: Mapping[str, TrustedSigner], required_scopes: Iterable[str]
) -> None:
    """Fail startup if strict mode lacks a verifier for any route scope."""
    available = set().union(*(signer.scopes for signer in signers.values())) if signers else set()
    missing = sorted(set(required_scopes) - available)
    if missing:
        raise RuntimeError(
            "WARDEN_API_TRUSTED_SIGNERS does not authorize required scopes: "
            + ", ".join(missing)
        )
