"""Helpers for keeping stored Discord guild presence aligned with the gateway."""


def mark_departed_guilds(records, current_guild_ids, *, left_at):
    """Mark active database records absent from the live gateway list as departed.

    Discord normally emits ``on_guild_remove``, but that event can be missed while
    the process is offline.  A full READY guild list is therefore the authority
    used to reconcile persisted presence at startup.
    """
    current_ids = {int(guild_id) for guild_id in current_guild_ids}
    departed = []

    for record in records:
        if int(record.guild_id) in current_ids:
            continue
        record.bot_present = False
        record.left_at = left_at
        departed.append(record)

    return departed
