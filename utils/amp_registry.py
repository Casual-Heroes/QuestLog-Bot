"""Secure loading and atomic administration of AMP-to-guild bindings."""

from __future__ import annotations

import json
import os
import re
import stat
from dataclasses import dataclass
from pathlib import Path


REGISTRY_PATH = Path("/etc/casual-heroes/amp-guild-bindings.json")
MAX_REGISTRY_BYTES = 64 * 1024
MAX_INSTANCES = 4096
MAX_GUILDS_PER_INSTANCE = 64
MAX_SNOWFLAKE = 2**63 - 1
_INSTANCE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


@dataclass(frozen=True)
class AmpRegistryPolicy:
    bindings: dict[str, frozenset[int]]
    public_passwords: dict[str, frozenset[int]]


def _unique_object(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError(f"duplicate JSON key: {key}")
        value[key] = item
    return value


def _parse_document(raw: str):
    try:
        return json.loads(raw, object_pairs_hook=_unique_object)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ValueError("AMP guild bindings must be valid JSON") from exc


def _parse_bindings(parsed, *, allow_scalar: bool) -> dict[str, frozenset[int]]:
    if not isinstance(parsed, dict):
        raise ValueError("AMP guild bindings must be a JSON object")
    if len(parsed) > MAX_INSTANCES:
        raise ValueError("AMP guild bindings contain too many instances")

    bindings = {}
    for instance_name, raw_guild_ids in parsed.items():
        if not isinstance(instance_name, str) or not _INSTANCE_RE.fullmatch(instance_name):
            raise ValueError("AMP guild bindings contain an invalid instance name")
        if isinstance(raw_guild_ids, list):
            values = raw_guild_ids
        elif allow_scalar:
            values = [raw_guild_ids]
        else:
            raise ValueError("registry guild bindings must be JSON arrays")
        if not values or len(values) > MAX_GUILDS_PER_INSTANCE:
            raise ValueError("AMP guild bindings contain an invalid guild list")

        guild_ids = set()
        for raw_guild_id in values:
            if isinstance(raw_guild_id, bool):
                raise ValueError("AMP guild bindings contain an invalid guild ID")
            if isinstance(raw_guild_id, int):
                guild_id = raw_guild_id
            elif allow_scalar and isinstance(raw_guild_id, str) and re.fullmatch(
                r"[1-9][0-9]{0,18}", raw_guild_id
            ):
                guild_id = int(raw_guild_id)
            else:
                raise ValueError("AMP guild bindings contain an invalid guild ID")
            if not 0 < guild_id <= MAX_SNOWFLAKE:
                raise ValueError("AMP guild bindings contain an invalid guild ID")
            guild_ids.add(guild_id)
        bindings[instance_name] = frozenset(guild_ids)
    return bindings


def parse_legacy_amp_guild_bindings(raw: str | None) -> dict[str, frozenset[int]]:
    """Parse the deprecated environment mapping during registry migration."""
    if not raw:
        return {}
    return _parse_bindings(_parse_document(raw), allow_scalar=True)


def parse_amp_registry_policy(raw: str) -> AmpRegistryPolicy:
    """Parse the strict, versioned root-owned registry policy."""
    document = _parse_document(raw)
    allowed_keys = {"version", "bindings", "public_passwords"}
    if (
        not isinstance(document, dict)
        or not {"version", "bindings"}.issubset(document)
        or not set(document).issubset(allowed_keys)
    ):
        raise ValueError(
            "AMP guild registry must contain version and bindings plus optional public_passwords"
        )
    if type(document["version"]) is not int or document["version"] != 1:
        raise ValueError("unsupported AMP guild registry version")
    bindings = _parse_bindings(document["bindings"], allow_scalar=False)
    public_passwords = _parse_bindings(
        document.get("public_passwords", {}), allow_scalar=False
    )
    for instance_name, guild_ids in public_passwords.items():
        if not guild_ids.issubset(bindings.get(instance_name, frozenset())):
            raise ValueError(
                "public password grants must be a subset of authorized guild bindings"
            )
    return AmpRegistryPolicy(bindings, public_passwords)


def parse_amp_guild_registry(raw: str) -> dict[str, frozenset[int]]:
    """Compatibility view of only the registry's guild bindings."""
    return parse_amp_registry_policy(raw).bindings


def read_secure_amp_registry_policy(
    path: Path = REGISTRY_PATH,
    *,
    required_owner_uid: int = 0,
) -> AmpRegistryPolicy | None:
    """Read one trusted snapshot; return None only when the file is absent."""
    path = Path(path)
    if not path.is_absolute():
        raise ValueError("AMP guild registry path must be absolute")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags)
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise ValueError(f"cannot securely open AMP guild registry: {exc}") from exc

    try:
        try:
            parent_metadata = path.parent.stat(follow_symlinks=False)
        except OSError as exc:
            raise ValueError(f"cannot inspect AMP guild registry directory: {exc}") from exc
        if (
            not stat.S_ISDIR(parent_metadata.st_mode)
            or parent_metadata.st_uid != required_owner_uid
            or parent_metadata.st_mode & 0o022
        ):
            raise ValueError(
                "AMP guild registry directory must be trusted-owner controlled and not writable"
            )
        metadata = os.fstat(fd)
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError("AMP guild registry is not a regular file")
        if metadata.st_uid != required_owner_uid:
            raise ValueError("AMP guild registry is not owned by root")
        if metadata.st_mode & 0o022:
            raise ValueError("AMP guild registry must not be group/world writable")
        if metadata.st_size > MAX_REGISTRY_BYTES:
            raise ValueError("AMP guild registry is too large")
        with os.fdopen(fd, "rb", closefd=False) as registry_file:
            raw = registry_file.read(MAX_REGISTRY_BYTES + 1)
        if len(raw) > MAX_REGISTRY_BYTES:
            raise ValueError("AMP guild registry is too large")
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError("AMP guild registry must be UTF-8") from exc
        return parse_amp_registry_policy(text)
    finally:
        os.close(fd)


def read_secure_amp_guild_registry(
    path: Path = REGISTRY_PATH,
    *,
    required_owner_uid: int = 0,
) -> dict[str, frozenset[int]] | None:
    """Compatibility view of only the secure registry's guild bindings."""
    policy = read_secure_amp_registry_policy(
        path, required_owner_uid=required_owner_uid
    )
    return None if policy is None else policy.bindings


def registry_document(
    bindings: dict[str, object],
    public_passwords: dict[str, object] | None = None,
) -> str:
    """Validate and serialize the canonical versioned registry document."""
    normalized = _parse_bindings(bindings, allow_scalar=False)
    normalized_public_passwords = _parse_bindings(
        public_passwords or {}, allow_scalar=False
    )
    for instance_name, guild_ids in normalized_public_passwords.items():
        if not guild_ids.issubset(normalized.get(instance_name, frozenset())):
            raise ValueError(
                "public password grants must be a subset of authorized guild bindings"
            )
    document = {
        "version": 1,
        "bindings": {
            instance: sorted(guild_ids)
            for instance, guild_ids in sorted(normalized.items())
        },
        "public_passwords": {
            instance: sorted(guild_ids)
            for instance, guild_ids in sorted(normalized_public_passwords.items())
        },
    }
    return json.dumps(document, indent=2, sort_keys=True) + "\n"
