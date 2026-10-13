"""Shared authorization policy for automatically assigned Discord roles."""

from __future__ import annotations


DANGEROUS_AUTOMATIC_ROLE_PERMISSIONS = frozenset({
    "administrator",
    "ban_members",
    "kick_members",
    "manage_guild",
    "manage_roles",
    "manage_channels",
    "manage_webhooks",
    "manage_messages",
    "manage_threads",
    "manage_expressions",
    "manage_emojis",
    "manage_emojis_and_stickers",
    "manage_events",
    "manage_nicknames",
    "moderate_members",
    "mute_members",
    "deafen_members",
    "move_members",
    "mention_everyone",
    "view_audit_log",
    "view_guild_insights",
})


def automatic_role_execution_error(guild, role) -> str | None:
    """Validate the live role state immediately before an automatic grant."""
    if role is None:
        return "Role not found."
    if int(getattr(getattr(role, "guild", None), "id", 0)) != int(guild.id):
        return "That role does not belong to this server."
    if role.is_default() or role.managed:
        return "Default and integration-managed roles cannot be assigned automatically."

    permissions = getattr(role, "permissions", None)
    dangerous = sorted(
        name
        for name in DANGEROUS_AUTOMATIC_ROLE_PERMISSIONS
        if getattr(permissions, name, False)
    )
    if dangerous:
        return "Privileged roles cannot be assigned automatically: " + ", ".join(dangerous)

    bot_member = getattr(guild, "me", None)
    if bot_member is None or role >= bot_member.top_role:
        return "Warden does not outrank that role."

    return None


def automatic_role_error(guild, role, *, approver=None) -> str | None:
    """Validate an automatic role and the administrator responsible for it."""
    role_error = automatic_role_execution_error(guild, role)
    if role_error:
        return role_error
    if approver is None:
        return "The approving administrator is no longer a member of this server."
    if int(approver.id) != int(guild.owner_id) and role >= approver.top_role:
        return "You cannot delegate a role at or above your highest role."
    return None


def delegated_role_grant_error(guild, role, *, actor, target=None) -> str | None:
    """Validate a bot-assisted role grant against the initiating actor."""
    role_error = automatic_role_execution_error(guild, role)
    if role_error:
        return role_error
    if actor is None:
        return "The initiating moderator is no longer a member of this server."
    bot_member = getattr(guild, "me", None)
    if target is not None and (
        bot_member is None or target.top_role >= bot_member.top_role
    ):
        return "Warden does not outrank the target member."
    if int(actor.id) == int(guild.owner_id):
        return None
    if role >= actor.top_role:
        return "You cannot grant a role at or above your highest role."
    if target is not None and target.top_role >= actor.top_role:
        return "You cannot manage a member with an equal or higher role."
    return None


async def resolve_role_approver(guild, approver_id):
    """Resolve a stored approver without treating a Discord cache miss as removal."""
    if not approver_id:
        return None
    approver = guild.get_member(int(approver_id))
    if approver is not None:
        return approver
    try:
        return await guild.fetch_member(int(approver_id))
    except Exception as exc:
        # Avoid coupling this low-level helper to a specific Discord library's
        # exception hierarchy while still failing closed on lookup failures.
        if exc.__class__.__module__.split('.', 1)[0] == "discord":
            return None
        raise
