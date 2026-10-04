"""Typed HTTP adapter for the canonical QuestLog LFG API v1."""

import asyncio
from dataclasses import dataclass
from typing import Any, Optional
from urllib.parse import quote, urlparse

import aiohttp


RETRYABLE_HTTP_STATUSES = {429, 500, 502, 503, 504}


class LFGAPIError(Exception):
    """A normalized QuestLog API or transport error."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        status: Optional[int] = None,
        details: Optional[dict] = None,
        retryable: bool = False,
    ):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status
        self.details = details or {}
        self.retryable = retryable


@dataclass(frozen=True)
class LFGAPIResult:
    """Successful API response plus idempotent replay metadata."""

    data: dict[str, Any]
    idempotent_replay: bool = False


class LFGAPIClient:
    """One reusable asynchronous client for QuestLog's LFG service."""

    def __init__(
        self,
        base_url: str,
        token: str = "",
        *,
        timeout_seconds: float = 15,
        max_attempts: int = 3,
    ):
        self.base_url = base_url.rstrip("/")
        self.token = token.strip()
        self.configuration_error = None
        if self.token.startswith("qlc_"):
            self.configuration_error = (
                "QUESTLOG_LFG_API_TOKEN contains the qlc_ client ID. "
                "Use the separately generated one-time qlp_ token instead."
            )
        elif self.token.lower().startswith("client:"):
            self.configuration_error = (
                "QUESTLOG_LFG_API_TOKEN must contain only the one-time qlp_ "
                "token, without a 'client:' label."
            )
        self.timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        self.max_attempts = max(1, max_attempts)
        self._session: Optional[aiohttp.ClientSession] = None

    @property
    def configured(self) -> bool:
        return bool(self.base_url and self.token and not self.configuration_error)

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def _get_session(self) -> aiohttp.ClientSession:
        if not self._session or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self.timeout)
        return self._session

    def _group_url(self, group_id_or_token: Any, suffix: str = "") -> str:
        identifier = quote(str(group_id_or_token), safe="")
        path = f"{self.base_url}/{identifier}/"
        if suffix:
            path += f"{suffix.strip('/')}/"
        return path

    def _validate_callback_url(self, callback_url: str) -> str:
        callback = urlparse(callback_url)
        base = urlparse(self.base_url)
        if (
            callback.scheme not in ("http", "https")
            or callback.scheme != base.scheme
            or callback.netloc != base.netloc
        ):
            raise LFGAPIError(
                "invalid_callback_url",
                "Delivery callback URL is outside the configured QuestLog API host",
            )
        return callback_url

    async def _request(
        self,
        method: str,
        url: str,
        *,
        payload: Optional[dict] = None,
        params: Optional[dict] = None,
        actor_user_id: Optional[int] = None,
        idempotency_key: Optional[str] = None,
        authenticated: bool = True,
    ) -> LFGAPIResult:
        if authenticated and self.configuration_error:
            raise LFGAPIError(
                "invalid_token_format",
                self.configuration_error,
            )
        if authenticated and not self.token:
            raise LFGAPIError(
                "client_not_configured",
                "QuestLog LFG API token is not configured",
            )

        headers = {"Accept": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        if payload is not None:
            headers["Content-Type"] = "application/json"
        if actor_user_id is not None:
            headers["X-QuestLog-Actor-User"] = str(actor_user_id)
        if idempotency_key:
            headers["Idempotency-Key"] = idempotency_key

        if method.upper() in {"POST", "PATCH", "PUT", "DELETE"} and not idempotency_key:
            raise LFGAPIError(
                "idempotency_key_required",
                "Canonical LFG writes require an idempotency key",
            )

        session = await self._get_session()
        last_error = None
        for attempt in range(1, self.max_attempts + 1):
            try:
                async with session.request(
                    method,
                    url,
                    json=payload,
                    params=params,
                    headers=headers,
                ) as response:
                    try:
                        body = await response.json(content_type=None)
                    except (aiohttp.ContentTypeError, ValueError):
                        body = {}

                    if 200 <= response.status < 300:
                        return LFGAPIResult(
                            data=body if isinstance(body, dict) else {"results": body},
                            idempotent_replay=(
                                response.headers.get("Idempotent-Replay", "").lower()
                                == "true"
                            ),
                        )

                    error = body.get("error", {}) if isinstance(body, dict) else {}
                    retryable = response.status in RETRYABLE_HTTP_STATUSES
                    last_error = LFGAPIError(
                        error.get("code", f"http_{response.status}"),
                        error.get("message", "QuestLog LFG API request failed"),
                        status=response.status,
                        details=error.get("details", {}),
                        retryable=retryable,
                    )
                    if not retryable or attempt == self.max_attempts:
                        raise last_error

                    retry_after = response.headers.get("Retry-After")
                    try:
                        delay = min(float(retry_after), 10) if retry_after else 2 ** (attempt - 1)
                    except (TypeError, ValueError):
                        delay = 2 ** (attempt - 1)
                    await asyncio.sleep(delay)
            except LFGAPIError:
                raise
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                last_error = LFGAPIError(
                    "transport_error",
                    str(exc) or "QuestLog LFG API transport failure",
                    retryable=True,
                )
                if attempt == self.max_attempts:
                    raise last_error from exc
                await asyncio.sleep(2 ** (attempt - 1))

        raise last_error or LFGAPIError("unknown_error", "Unknown LFG API failure")

    async def browse(self, **filters) -> LFGAPIResult:
        params = {key: value for key, value in filters.items() if value is not None}
        return await self._request(
            "GET", f"{self.base_url}/", params=params, authenticated=False
        )

    async def detail(
        self,
        group_id_or_token: Any,
        *,
        actor_user_id: Optional[int] = None,
    ) -> LFGAPIResult:
        return await self._request(
            "GET",
            self._group_url(group_id_or_token),
            actor_user_id=actor_user_id,
            authenticated=False,
        )

    async def create_group(
        self, payload: dict, *, actor_user_id: int, idempotency_key: str
    ) -> LFGAPIResult:
        return await self._request(
            "POST",
            f"{self.base_url}/",
            payload=payload,
            actor_user_id=actor_user_id,
            idempotency_key=idempotency_key,
        )

    async def update_group(
        self, group_id_or_token: Any, payload: dict, *, actor_user_id: int,
        idempotency_key: str
    ) -> LFGAPIResult:
        return await self._request(
            "PATCH", self._group_url(group_id_or_token), payload=payload,
            actor_user_id=actor_user_id, idempotency_key=idempotency_key
        )

    async def delete_group(
        self,
        group_id_or_token: Any,
        *,
        actor_user_id: int,
        idempotency_key: str,
    ) -> LFGAPIResult:
        return await self._request(
            "DELETE",
            self._group_url(group_id_or_token),
            payload={},
            actor_user_id=actor_user_id,
            idempotency_key=idempotency_key,
        )

    async def join_group(
        self, group_id_or_token: Any, payload: dict, *, actor_user_id: int,
        idempotency_key: str
    ) -> LFGAPIResult:
        return await self._request(
            "POST", self._group_url(group_id_or_token, "join"), payload=payload,
            actor_user_id=actor_user_id, idempotency_key=idempotency_key
        )

    async def leave_group(
        self, group_id_or_token: Any, *, actor_user_id: int,
        idempotency_key: str
    ) -> LFGAPIResult:
        return await self._request(
            "POST", self._group_url(group_id_or_token, "leave"), payload={},
            actor_user_id=actor_user_id, idempotency_key=idempotency_key
        )

    async def update_member(
        self, group_id_or_token: Any, payload: dict, *, actor_user_id: int,
        idempotency_key: str
    ) -> LFGAPIResult:
        return await self._request(
            "PATCH", self._group_url(group_id_or_token, "member"), payload=payload,
            actor_user_id=actor_user_id, idempotency_key=idempotency_key
        )

    async def transition_status(
        self, group_id_or_token: Any, status: str, *, actor_user_id: int,
        idempotency_key: str
    ) -> LFGAPIResult:
        return await self._request(
            "POST", self._group_url(group_id_or_token, "status"),
            payload={"status": status}, actor_user_id=actor_user_id,
            idempotency_key=idempotency_key
        )

    async def acknowledge_delivery(
        self,
        delivery_job_id: str,
        status: str,
        *,
        idempotency_key: str,
        callback_url: Optional[str] = None,
        details: Optional[dict] = None,
    ) -> LFGAPIResult:
        url = (
            self._validate_callback_url(callback_url)
            if callback_url
            else f"{self.base_url}/deliveries/{quote(str(delivery_job_id), safe='')}/"
        )
        payload = {"status": status}
        if details:
            payload["details"] = details
        return await self._request(
            "POST", url, payload=payload, idempotency_key=idempotency_key
        )
