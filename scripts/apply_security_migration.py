"""Apply one reviewed, additive Warden security migration.

Only migrations in the allowlist can be selected.  The SQL files contain
idempotent DDL and are executed through Warden's configured database engine.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config import get_engine  # noqa: E402


ALLOWED_MIGRATIONS = {
    "flair_delivery_receipts.sql",
    "progression_outbox.sql",
}


def statements(path: Path):
    sql = "\n".join(
        line for line in path.read_text(encoding="utf-8").splitlines()
        if not line.lstrip().startswith("--")
    )
    return [statement.strip() for statement in sql.split(";") if statement.strip()]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("migration", choices=sorted(ALLOWED_MIGRATIONS))
    args = parser.parse_args()

    path = ROOT / "migrations" / args.migration
    migration_statements = statements(path)
    with get_engine().begin() as connection:
        for statement in migration_statements:
            connection.exec_driver_sql(statement)
    print(f"Applied {args.migration}: {len(migration_statements)} statements")


if __name__ == "__main__":
    main()
