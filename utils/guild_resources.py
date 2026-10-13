"""Fail-closed resolution of Discord resources owned by a specific guild."""

from __future__ import annotations


def resource_belongs_to_guild(resource, guild_id: int, resource_id: int | None = None) -> bool:
    """Return whether a Discord resource has the expected ID and owning guild."""
    resource_guild = getattr(resource, "guild", None)
    try:
        if int(getattr(resource_guild, "id", 0)) != int(guild_id):
            return False
        return resource_id is None or int(getattr(resource, "id", 0)) == int(resource_id)
    except (TypeError, ValueError):
        return False


async def resolve_guild_channel(bot, guild_id: int, channel_id: int):
    """Resolve a channel or thread only within its expected Discord guild.

    Database identifiers are not treated as proof of ownership.  Resolution
    starts from the expected guild and validates the returned object's guild
    again after a cache lookup or API fetch.
    """
    try:
        expected_guild_id = int(guild_id)
        expected_channel_id = int(channel_id)
    except (TypeError, ValueError):
        return None

    guild = bot.get_guild(expected_guild_id)
    if guild is None:
        return None

    get_cached = getattr(guild, "get_channel_or_thread", None)
    channel = get_cached(expected_channel_id) if get_cached else guild.get_channel(expected_channel_id)
    if channel is None:
        try:
            channel = await guild.fetch_channel(expected_channel_id)
        except Exception:
            return None

    if not resource_belongs_to_guild(channel, expected_guild_id, expected_channel_id):
        return None
    return channel
