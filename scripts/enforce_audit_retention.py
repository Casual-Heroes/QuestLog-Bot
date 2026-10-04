"""Dry-run or enforce Warden audit metadata retention without reading content."""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from sqlalchemy import func


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config import db_session_scope  # noqa: E402
from models import AuditLog  # noqa: E402


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=90)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    if not 30 <= args.days <= 365:
        parser.error("--days must be between 30 and 365")

    cutoff = int(time.time()) - (args.days * 86400)
    with db_session_scope() as session:
        eligible = session.query(func.count(AuditLog.id)).filter(
            AuditLog.timestamp < cutoff
        ).scalar() or 0
        print({
            "mode": "apply" if args.apply else "dry-run",
            "retention_days": args.days,
            "eligible": int(eligible),
        })
        if not args.apply or not eligible:
            return
        deleted = session.query(AuditLog).filter(
            AuditLog.timestamp < cutoff
        ).delete(synchronize_session=False)
        print({"deleted": int(deleted), "retained_days": args.days})


if __name__ == "__main__":
    main()
