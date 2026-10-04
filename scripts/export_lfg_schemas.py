#!/usr/bin/env python3
"""Export Warden's per-guild LFG configuration without secrets or member data.

The resulting JSON is an input to QuestLog's ``migrate_lfg_templates`` command.
It contains configuration only; it does not contain group posts, attendance,
Discord tokens, webhook URLs, or user identities.
"""

import argparse
import json
import sys
from pathlib import Path

from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--guild-id", type=int)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--env-file", type=Path, default=ROOT / ".env")
    args = parser.parse_args()
    load_dotenv(args.env_file, override=True)
    # Database modules must be imported only after the selected environment file
    # is loaded because their engine is constructed at import time.
    from db import get_db_session
    from models import LFGGame
    with get_db_session() as db:
        query = db.query(LFGGame)
        if args.guild_id:
            query = query.filter(LFGGame.guild_id == args.guild_id)
        rows = query.order_by(LFGGame.guild_id, LFGGame.game_name).all()
        games = []
        for row in rows:
            try:
                custom_options = json.loads(row.custom_options or "[]")
            except (TypeError, ValueError):
                custom_options = row.custom_options
            games.append({
                "id": row.id,
                "guild_id": str(row.guild_id),
                "game_name": row.game_name,
                "game_short": row.game_short,
                "igdb_id": row.igdb_id,
                "platforms": row.platforms,
                "custom_options": custom_options,
                "role_detection_mode": row.role_detection_mode,
                "max_group_size": row.max_group_size,
                "require_rank": bool(row.require_rank),
                "rank_label": row.rank_label,
                "rank_min": row.rank_min,
                "rank_max": row.rank_max,
                "enabled": bool(row.enabled),
            })
    payload = {"format": "warden-lfg-schema-export-v1", "games": games}
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    print(f"Exported {len(games)} LFG game configurations to {args.output}")


if __name__ == "__main__":
    main()
