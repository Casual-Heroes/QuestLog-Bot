"""Security policy for AMP management endpoints."""

from __future__ import annotations

from urllib.parse import urlsplit


def validate_amp_url(value: str | None) -> str:
    """Return a normalized HTTPS AMP base URL or raise for unsafe input."""
    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        parsed = urlsplit(raw)
        port = parsed.port
    except (TypeError, ValueError) as exc:
        raise ValueError("AMP_URL must be a valid absolute HTTPS URL") from exc
    if (
        parsed.scheme.lower() != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError(
            "AMP_URL must be an absolute HTTPS URL without credentials, query, or fragment"
        )
    authority = parsed.hostname.lower().rstrip(".")
    if ":" in authority and not authority.startswith("["):
        authority = f"[{authority}]"
    if port is not None:
        authority = f"{authority}:{port}"
    path = parsed.path.rstrip("/")
    return f"https://{authority}{path}"
