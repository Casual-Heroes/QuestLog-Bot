"""Validation for remote service URLs that receive Warden credentials."""

from urllib.parse import urlsplit


def validate_https_service_url(value: object, setting_name: str) -> str:
    """Return a normalized HTTPS service URL or raise a safe configuration error."""
    url = str(value or "").strip().rstrip("/")
    try:
        parsed = urlsplit(url)
        port = parsed.port
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{setting_name} is not a valid URL") from exc

    if (
        parsed.scheme.lower() != "https"
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or port not in (None, 443)
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError(
            f"{setting_name} must be an HTTPS URL without credentials, "
            "a custom port, query, or fragment"
        )
    return url
