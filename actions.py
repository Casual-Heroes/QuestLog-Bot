# warden/actions.py
"""
Action Queue helpers for Django views.

This module provides easy-to-use functions for queueing actions
from the Django web dashboard that will be processed by the bot.

Usage:
    from warden.actions import queue_action, ActionType

    # Add a role to a user
    action_id = queue_action(
        guild_id=123456789,
        action_type=ActionType.ROLE_ADD,
        payload={"user_id": 987654321, "role_id": 111111111},
        triggered_by=user_id,
        triggered_by_name=username
    )

    # Bulk XP set from CSV
    action_id = queue_action(
        guild_id=123456789,
        action_type=ActionType.XP_BULK_SET,
        payload={"users": [{"user_id": 123, "xp": 1000}, ...]},
        triggered_by=user_id,
        source="csv_import"
    )
"""

import json
import time
from typing import Optional, Any, List, Dict

from db import get_db_session
from models import (
    PendingAction, ActionStatus, ActionType,
    BulkImportJob, Guild, GuildMember
)


def queue_action(
    guild_id: int,
    action_type: ActionType,
    payload: dict,
    triggered_by: Optional[int] = None,
    triggered_by_name: Optional[str] = None,
    source: str = "website",
    priority: int = 5
) -> int:
    """
    Queue an action for the bot to process.

    Args:
        guild_id: The Discord guild ID
        action_type: The type of action (from ActionType enum)
        payload: Action-specific data as a dictionary
        triggered_by: User ID who triggered this action
        triggered_by_name: Display name of user who triggered
        source: Where this came from ("website", "api", "csv_import")
        priority: 1=highest, 10=lowest (default 5)

    Returns:
        The ID of the created PendingAction record
    """
    with get_db_session() as session:
        action = PendingAction(
            guild_id=guild_id,
            action_type=action_type,
            status=ActionStatus.PENDING,
            priority=priority,
            payload=json.dumps(payload),
            triggered_by=triggered_by,
            triggered_by_name=triggered_by_name,
            source=source
        )
        session.add(action)
        session.commit()
        return action.id


def get_action_status(action_id: int) -> Optional[dict]:
    """
    Get the status of a queued action.

    Returns:
        Dictionary with action status, or None if not found
    """
    with get_db_session() as session:
        action = session.query(PendingAction).filter_by(id=action_id).first()
        if not action:
            return None

        return {
            "id": action.id,
            "guild_id": action.guild_id,
            "action_type": action.action_type.value,
            "status": action.status.value,
            "created_at": action.created_at,
            "started_at": action.started_at,
            "completed_at": action.completed_at,
            "result": json.loads(action.result) if action.result else None,
            "error_message": action.error_message,
            "retry_count": action.retry_count
        }


def cancel_action(action_id: int) -> bool:
    """
    Cancel a pending action (only works if not yet processing).

    Returns:
        True if cancelled, False if already processing/completed
    """
    with get_db_session() as session:
        action = session.query(PendingAction).filter_by(id=action_id).first()
        if not action:
            return False

        if action.status != ActionStatus.PENDING:
            return False

        action.status = ActionStatus.CANCELLED
        action.completed_at = int(time.time())
        session.commit()
        return True


def get_pending_actions(guild_id: int, limit: int = 50) -> List[dict]:
    """Get all pending actions for a guild."""
    with get_db_session() as session:
        actions = session.query(PendingAction).filter_by(
            guild_id=guild_id
        ).filter(
            PendingAction.status.in_([ActionStatus.PENDING, ActionStatus.PROCESSING])
        ).order_by(
            PendingAction.created_at.desc()
        ).limit(limit).all()

        return [{
            "id": a.id,
            "action_type": a.action_type.value,
            "status": a.status.value,
            "created_at": a.created_at,
            "triggered_by_name": a.triggered_by_name,
            "source": a.source
        } for a in actions]


def get_recent_actions(guild_id: int, limit: int = 50) -> List[dict]:
    """Get recent completed/failed actions for a guild."""
    with get_db_session() as session:
        actions = session.query(PendingAction).filter_by(
            guild_id=guild_id
        ).filter(
            PendingAction.status.in_([ActionStatus.COMPLETED, ActionStatus.FAILED])
        ).order_by(
            PendingAction.completed_at.desc()
        ).limit(limit).all()

        return [{
            "id": a.id,
            "action_type": a.action_type.value,
            "status": a.status.value,
            "created_at": a.created_at,
            "completed_at": a.completed_at,
            "triggered_by_name": a.triggered_by_name,
            "source": a.source,
            "error_message": a.error_message
        } for a in actions]


# ═══════════════════════════════════════════════════════════════
# CONVENIENCE FUNCTIONS FOR COMMON ACTIONS
# ═══════════════════════════════════════════════════════════════

def queue_role_add(
    guild_id: int,
    user_id: int,
    role_id: int,
    reason: str = "Added via web dashboard",
    triggered_by: int = None,
    triggered_by_name: str = None
) -> int:
    """Queue adding a role to a user."""
    return queue_action(
        guild_id=guild_id,
        action_type=ActionType.ROLE_ADD,
        payload={"user_id": user_id, "role_id": role_id, "reason": reason},
        triggered_by=triggered_by,
        triggered_by_name=triggered_by_name
    )


def queue_role_remove(
    guild_id: int,
    user_id: int,
    role_id: int,
    reason: str = "Removed via web dashboard",
    triggered_by: int = None,
    triggered_by_name: str = None
) -> int:
    """Queue removing a role from a user."""
    return queue_action(
        guild_id=guild_id,
        action_type=ActionType.ROLE_REMOVE,
        payload={"user_id": user_id, "role_id": role_id, "reason": reason},
        triggered_by=triggered_by,
        triggered_by_name=triggered_by_name
    )


def queue_bulk_role_add(
    guild_id: int,
    role_id: int,
    user_ids: List[int],
    reason: str = "Bulk add via web dashboard",
    triggered_by: int = None,
    triggered_by_name: str = None
) -> int:
    """Queue adding a role to multiple users (for CSV imports)."""
    return queue_action(
        guild_id=guild_id,
        action_type=ActionType.ROLE_BULK_ADD,
        payload={"role_id": role_id, "user_ids": user_ids, "reason": reason},
        triggered_by=triggered_by,
        triggered_by_name=triggered_by_name,
        source="csv_import",
        priority=3  # Higher priority for bulk operations
    )


def queue_xp_add(
    guild_id: int,
    user_id: int,
    amount: float,
    triggered_by: int = None,
    triggered_by_name: str = None
) -> int:
    """Queue adding XP to a user."""
    return queue_action(
        guild_id=guild_id,
        action_type=ActionType.XP_ADD,
        payload={"user_id": user_id, "amount": amount},
        triggered_by=triggered_by,
        triggered_by_name=triggered_by_name
    )


def queue_xp_set(
    guild_id: int,
    user_id: int,
    amount: float,
    triggered_by: int = None,
    triggered_by_name: str = None
) -> int:
    """Queue setting a user's XP to a specific value."""
    return queue_action(
        guild_id=guild_id,
        action_type=ActionType.XP_SET,
        payload={"user_id": user_id, "amount": amount},
        triggered_by=triggered_by,
        triggered_by_name=triggered_by_name
    )


def queue_bulk_xp_set(
    guild_id: int,
    users: List[Dict[str, Any]],  # [{"user_id": 123, "xp": 1000}, ...]
    triggered_by: int = None,
    triggered_by_name: str = None
) -> int:
    """Queue setting XP for multiple users (for CSV imports)."""
    return queue_action(
        guild_id=guild_id,
        action_type=ActionType.XP_BULK_SET,
        payload={"users": users},
        triggered_by=triggered_by,
        triggered_by_name=triggered_by_name,
        source="csv_import",
        priority=3
    )


def queue_tokens_add(
    guild_id: int,
    user_id: int,
    amount: int,
    triggered_by: int = None,
    triggered_by_name: str = None
) -> int:
    """Queue adding Hero Tokens to a user."""
    return queue_action(
        guild_id=guild_id,
        action_type=ActionType.TOKENS_ADD,
        payload={"user_id": user_id, "amount": amount},
        triggered_by=triggered_by,
        triggered_by_name=triggered_by_name
    )


def queue_member_kick(
    guild_id: int,
    user_id: int,
    reason: str = "Kicked via web dashboard",
    triggered_by: int = None,
    triggered_by_name: str = None
) -> int:
    """Queue kicking a member."""
    return queue_action(
        guild_id=guild_id,
        action_type=ActionType.MEMBER_KICK,
        payload={"user_id": user_id, "reason": reason},
        triggered_by=triggered_by,
        triggered_by_name=triggered_by_name,
        priority=2  # Higher priority for mod actions
    )


def queue_member_ban(
    guild_id: int,
    user_id: int,
    reason: str = "Banned via web dashboard",
    delete_message_days: int = 0,
    triggered_by: int = None,
    triggered_by_name: str = None
) -> int:
    """Queue banning a member."""
    return queue_action(
        guild_id=guild_id,
        action_type=ActionType.MEMBER_BAN,
        payload={
            "user_id": user_id,
            "reason": reason,
            "delete_message_days": delete_message_days
        },
        triggered_by=triggered_by,
        triggered_by_name=triggered_by_name,
        priority=2
    )


def queue_member_timeout(
    guild_id: int,
    user_id: int,
    duration_minutes: int = 60,
    reason: str = "Timed out via web dashboard",
    triggered_by: int = None,
    triggered_by_name: str = None
) -> int:
    """Queue timing out a member."""
    return queue_action(
        guild_id=guild_id,
        action_type=ActionType.MEMBER_TIMEOUT,
        payload={
            "user_id": user_id,
            "duration_minutes": duration_minutes,
            "reason": reason
        },
        triggered_by=triggered_by,
        triggered_by_name=triggered_by_name,
        priority=2
    )


def queue_warning_add(
    guild_id: int,
    user_id: int,
    reason: str,
    severity: int = 1,
    triggered_by: int = None,
    triggered_by_name: str = None
) -> int:
    """Queue adding a warning to a user."""
    return queue_action(
        guild_id=guild_id,
        action_type=ActionType.WARNING_ADD,
        payload={
            "user_id": user_id,
            "reason": reason,
            "severity": severity,
            "issued_by": triggered_by,
            "issued_by_name": triggered_by_name or "Web Dashboard"
        },
        triggered_by=triggered_by,
        triggered_by_name=triggered_by_name
    )


def queue_message_send(
    guild_id: int,
    channel_id: int,
    content: str = None,
    embed: dict = None,
    triggered_by: int = None,
    triggered_by_name: str = None
) -> int:
    """Queue sending a message to a channel."""
    return queue_action(
        guild_id=guild_id,
        action_type=ActionType.MESSAGE_SEND,
        payload={"channel_id": channel_id, "content": content, "embed": embed},
        triggered_by=triggered_by,
        triggered_by_name=triggered_by_name
    )


# ═══════════════════════════════════════════════════════════════
# BULK IMPORT JOB TRACKING
# ═══════════════════════════════════════════════════════════════

def create_bulk_import_job(
    guild_id: int,
    job_type: str,
    filename: str,
    total_records: int,
    triggered_by: int = None,
    triggered_by_name: str = None
) -> int:
    """Create a bulk import job for tracking progress."""
    with get_db_session() as session:
        job = BulkImportJob(
            guild_id=guild_id,
            job_type=job_type,
            filename=filename,
            total_records=total_records,
            triggered_by=triggered_by,
            triggered_by_name=triggered_by_name
        )
        session.add(job)
        session.commit()
        return job.id


def update_bulk_import_progress(
    job_id: int,
    processed_records: int = None,
    success_count: int = None,
    error_count: int = None,
    status: str = None,
    errors: List[dict] = None
):
    """Update the progress of a bulk import job."""
    with get_db_session() as session:
        job = session.query(BulkImportJob).filter_by(id=job_id).first()
        if not job:
            return

        if processed_records is not None:
            job.processed_records = processed_records
        if success_count is not None:
            job.success_count = success_count
        if error_count is not None:
            job.error_count = error_count
        if status:
            job.status = status
            if status == "processing" and not job.started_at:
                job.started_at = int(time.time())
            elif status in ("completed", "failed"):
                job.completed_at = int(time.time())
        if errors:
            job.errors = json.dumps(errors)

        session.commit()


def get_bulk_import_job(job_id: int) -> Optional[dict]:
    """Get the status of a bulk import job."""
    with get_db_session() as session:
        job = session.query(BulkImportJob).filter_by(id=job_id).first()
        if not job:
            return None

        return {
            "id": job.id,
            "guild_id": job.guild_id,
            "job_type": job.job_type,
            "filename": job.filename,
            "status": job.status,
            "total_records": job.total_records,
            "processed_records": job.processed_records,
            "success_count": job.success_count,
            "error_count": job.error_count,
            "errors": json.loads(job.errors) if job.errors else [],
            "created_at": job.created_at,
            "started_at": job.started_at,
            "completed_at": job.completed_at,
            "triggered_by_name": job.triggered_by_name,
            "progress_percent": (
                int((job.processed_records / job.total_records) * 100)
                if job.total_records > 0 else 0
            )
        }
