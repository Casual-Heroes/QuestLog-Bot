"""Typed adapter for QuestLog's authoritative progression event API."""

import asyncio
import hashlib
from dataclasses import dataclass
from typing import Optional

import aiohttp


RETRYABLE_STATUSES = {429, 500, 502, 503, 504}


class ProgressionAPIError(Exception):
    def __init__(self, code, message, *, status=None, retryable=False):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status
        self.retryable = retryable


@dataclass(frozen=True)
class ProgressionAPIResult:
    status: str
    awarded_xp: int
    awarded_legacy: int = 0
    identity_linked: bool = False
    current_xp: Optional[int] = None
    current_level: Optional[int] = None
    hero_points: Optional[int] = None
    previous_level: Optional[int] = None
    level_changed: bool = False
    idempotent_replay: bool = False


def progression_idempotency_key(guild_id, event_type, evidence_id):
    """Return the stable key shared by Warden's outbox and QuestLog."""
    key = f"discord:{guild_id}:{event_type}:{evidence_id}"
    if len(key) <= 120:
        return key
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
    return f"discord-progression:{digest}"


class ProgressionAPIClient:
    def __init__(self, url, token, *, timeout_seconds=10, max_attempts=3):
        self.url = str(url or "").rstrip("/") + "/"
        self.token = str(token or "").strip()
        self.timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        self.max_attempts = max(1, int(max_attempts))
        self._session: Optional[aiohttp.ClientSession] = None
        self.configuration_error = None
        if self.token.startswith("qlc_"):
            self.configuration_error = "Use the one-time qlp_ token, not the qlc_ client ID"
        elif self.token.lower().startswith("client:"):
            self.configuration_error = "Do not include a client: label in the token"

    @property
    def configured(self):
        return bool(self.url and self.token and not self.configuration_error)

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def _get_session(self):
        if not self._session or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self.timeout)
        return self._session

    async def record_discord_event(
        self, *, user_id, guild_id, event_type, evidence_id, occurred_at
    ):
        if self.configuration_error:
            raise ProgressionAPIError("invalid_token_format", self.configuration_error)
        if not self.configured:
            raise ProgressionAPIError("client_not_configured", "Progression API is not configured")

        payload = {
            "provider": "discord",
            "provider_user_id": str(user_id),
            "provider_guild_id": str(guild_id),
            "event_type": str(event_type),
            "evidence_id": str(evidence_id),
            "occurred_at": int(occurred_at),
        }
        idempotency_key = progression_idempotency_key(
            guild_id, event_type, evidence_id
        )
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "Idempotency-Key": idempotency_key,
        }
        session = await self._get_session()
        last_error = None
        for attempt in range(1, self.max_attempts + 1):
            try:
                async with session.post(self.url, json=payload, headers=headers) as response:
                    try:
                        body = await response.json(content_type=None)
                    except (aiohttp.ContentTypeError, ValueError):
                        body = {}
                    if 200 <= response.status < 300:
                        data = body.get("data", {}) if isinstance(body, dict) else {}
                        return ProgressionAPIResult(
                            status=data.get("status", "accepted"),
                            awarded_xp=int(data.get("awarded_xp") or 0),
                            awarded_legacy=int(data.get("awarded_legacy") or 0),
                            identity_linked=bool(data.get("identity_linked", False)),
                            current_xp=(
                                int(data["current_xp"])
                                if data.get("current_xp") is not None else None
                            ),
                            current_level=(
                                int(data["current_level"])
                                if data.get("current_level") is not None else None
                            ),
                            hero_points=(
                                int(data["hero_points"])
                                if data.get("hero_points") is not None else None
                            ),
                            previous_level=(
                                int(data["previous_level"])
                                if data.get("previous_level") is not None else None
                            ),
                            level_changed=bool(data.get("level_changed", False)),
                            idempotent_replay=(
                                response.headers.get("Idempotent-Replay", "").lower()
                                == "true"
                            ),
                        )
                    error = body.get("error", {}) if isinstance(body, dict) else {}
                    last_error = ProgressionAPIError(
                        error.get("code", f"http_{response.status}"),
                        error.get("message", "QuestLog progression request failed"),
                        status=response.status,
                        retryable=response.status in RETRYABLE_STATUSES,
                    )
                    if not last_error.retryable or attempt == self.max_attempts:
                        raise last_error
                await asyncio.sleep(2 ** (attempt - 1))
            except ProgressionAPIError:
                raise
            except (aiohttp.ClientError, asyncio.TimeoutError) as error:
                last_error = ProgressionAPIError(
                    "transport_error", str(error) or "QuestLog is unavailable", retryable=True
                )
                if attempt == self.max_attempts:
                    raise last_error from error
                await asyncio.sleep(2 ** (attempt - 1))
        raise last_error
