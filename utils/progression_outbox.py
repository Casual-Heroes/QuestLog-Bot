"""Durable, idempotent delivery for QuestLog progression evidence."""

import asyncio
import json
import time
from dataclasses import asdict
from weakref import WeakValueDictionary

from models import ProgressionOutboxEvent
from utils.progression_api import (
    ProgressionAPIError,
    ProgressionAPIResult,
    progression_idempotency_key,
)


class ProgressionOutbox:
    """Persist evidence before attempting network delivery.

    Rows remain queued after process/network failures and can be retried after
    restart. QuestLog receives the same Idempotency-Key on every attempt.
    """

    def __init__(self, client, session_scope, logger):
        self.client = client
        self.session_scope = session_scope
        self.logger = logger
        self._locks = WeakValueDictionary()

    def enqueue(self, *, guild_id, user_id, event_type, evidence_id, occurred_at):
        event_key = progression_idempotency_key(
            guild_id, event_type, evidence_id
        )
        with self.session_scope() as session:
            row = session.get(ProgressionOutboxEvent, event_key)
            if row is None:
                row = ProgressionOutboxEvent(
                    event_key=event_key,
                    guild_id=int(guild_id),
                    user_id=int(user_id),
                    event_type=str(event_type),
                    evidence_id=str(evidence_id),
                    occurred_at=int(occurred_at),
                    status="queued",
                    next_attempt_at=0,
                )
                session.add(row)
            elif (
                int(row.guild_id) != int(guild_id)
                or int(row.user_id) != int(user_id)
                or row.event_type != str(event_type)
                or row.evidence_id != str(evidence_id)
            ):
                raise ValueError(
                    "Progression idempotency key was reused with different evidence"
                )
        return event_key

    @staticmethod
    def _decode_result(payload):
        if not payload:
            return None
        return ProgressionAPIResult(**json.loads(payload))

    @staticmethod
    def _backoff_seconds(attempt_count):
        return min(3600, 15 * (2 ** min(max(0, attempt_count - 1), 8)))

    async def submit(self, **event):
        event_key = self.enqueue(**event)
        return await self.deliver(event_key, force=True)

    async def deliver(self, event_key, *, force=False):
        lock = self._locks.setdefault(event_key, asyncio.Lock())
        async with lock:
            now = int(time.time())
            with self.session_scope() as session:
                row = session.get(ProgressionOutboxEvent, event_key)
                if row is None:
                    return None
                if row.status == "delivered":
                    return self._decode_result(row.result_payload)
                if row.status == "failed":
                    return None
                if not force and int(row.next_attempt_at or 0) > now:
                    return None
                row.attempt_count = int(row.attempt_count or 0) + 1
                row.updated_at = now
                event = {
                    "guild_id": int(row.guild_id),
                    "user_id": int(row.user_id),
                    "event_type": row.event_type,
                    "evidence_id": row.evidence_id,
                    "occurred_at": int(row.occurred_at),
                }
                attempt_count = row.attempt_count

            try:
                result = await self.client.record_discord_event(**event)
            except ProgressionAPIError as error:
                retryable = (
                    error.retryable
                    or error.code in {"client_not_configured", "invalid_token_format"}
                    or error.status in {401, 403}
                )
                retry_at = now + self._backoff_seconds(attempt_count)
                with self.session_scope() as session:
                    row = session.get(ProgressionOutboxEvent, event_key)
                    if row is not None and row.status != "delivered":
                        row.status = "retry" if retryable else "failed"
                        row.next_attempt_at = retry_at if retryable else 0
                        row.last_error_code = str(error.code)[:100]
                        row.last_error_message = str(error.message)
                        row.updated_at = int(time.time())
                if retryable:
                    self.logger.warning(
                        "QuestLog progression queued for retry: key=%s "
                        "attempt=%s retry_at=%s error=%s",
                        event_key,
                        attempt_count,
                        retry_at,
                        error.code,
                    )
                else:
                    self.logger.error(
                        "QuestLog progression permanently rejected: key=%s "
                        "attempt=%s error=%s",
                        event_key,
                        attempt_count,
                        error.code,
                    )
                return None
            except Exception as error:
                retry_at = now + self._backoff_seconds(attempt_count)
                with self.session_scope() as session:
                    row = session.get(ProgressionOutboxEvent, event_key)
                    if row is not None and row.status != "delivered":
                        row.status = "retry"
                        row.next_attempt_at = retry_at
                        row.last_error_code = "unexpected_error"
                        row.last_error_message = str(error)
                        row.updated_at = int(time.time())
                self.logger.exception(
                    "Unexpected QuestLog progression failure queued for retry: %s",
                    event_key,
                )
                return None

            delivered_at = int(time.time())
            with self.session_scope() as session:
                row = session.get(ProgressionOutboxEvent, event_key)
                if row is not None:
                    row.status = "delivered"
                    row.next_attempt_at = 0
                    row.last_error_code = None
                    row.last_error_message = None
                    row.result_payload = json.dumps(asdict(result), sort_keys=True)
                    row.updated_at = delivered_at
                    row.delivered_at = delivered_at
            return result

    def due_event_keys(self, *, limit=50, now=None):
        now = int(time.time()) if now is None else int(now)
        with self.session_scope() as session:
            rows = (
                session.query(ProgressionOutboxEvent.event_key)
                .filter(
                    ProgressionOutboxEvent.status.in_(("queued", "retry")),
                    ProgressionOutboxEvent.next_attempt_at <= now,
                )
                .order_by(ProgressionOutboxEvent.created_at.asc())
                .limit(int(limit))
                .all()
            )
            return [row[0] for row in rows]

    def prune_delivered(self, *, retain_seconds=30 * 24 * 60 * 60, now=None):
        now = int(time.time()) if now is None else int(now)
        cutoff = now - int(retain_seconds)
        with self.session_scope() as session:
            return (
                session.query(ProgressionOutboxEvent)
                .filter(
                    ProgressionOutboxEvent.status == "delivered",
                    ProgressionOutboxEvent.delivered_at < cutoff,
                )
                .delete(synchronize_session=False)
            )
