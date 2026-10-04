"""Quarantine abandoned legacy queue rows without replaying them."""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from sqlalchemy import func


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config import db_session_scope  # noqa: E402
from models import ActionStatus, PendingAction  # noqa: E402


QUARANTINE_REASON = (
    "QUARANTINED: legacy processing outcome is ambiguous; this action must not "
    "be replayed automatically"
)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--older-than-seconds", type=int, default=3600)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    if args.older_than_seconds < 300:
        parser.error("--older-than-seconds must be at least 300")

    now = int(time.time())
    cutoff = now - args.older_than_seconds
    with db_session_scope() as session:
        base = session.query(PendingAction).filter(
            PendingAction.status == ActionStatus.PROCESSING,
            PendingAction.created_at < cutoff,
        )
        grouped = session.query(
            PendingAction.action_type,
            func.count(PendingAction.id),
        ).filter(
            PendingAction.status == ActionStatus.PROCESSING,
            PendingAction.created_at < cutoff,
        ).group_by(PendingAction.action_type).all()
        count = base.count()

        print({
            "mode": "apply" if args.apply else "dry-run",
            "eligible": count,
            "by_type": {
                getattr(action_type, "value", str(action_type)): amount
                for action_type, amount in grouped
            },
        })

        if not args.apply or not count:
            return

        for action in base.all():
            prior = (action.error_message or "").strip()
            action.status = ActionStatus.FAILED
            action.completed_at = now
            action.error_message = (
                QUARANTINE_REASON if not prior
                else f"{QUARANTINE_REASON}; prior_error={prior}"
            )[:2000]

        print({"quarantined": count, "replayed": 0})


if __name__ == "__main__":
    main()
