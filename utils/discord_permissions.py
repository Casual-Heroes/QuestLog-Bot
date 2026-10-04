"""Canonical Discord installation permissions for WardenBot."""

from urllib.parse import urlencode

import discord


# These permissions cover the operations used by the bot without granting the
# Administrator bypass. Channel overwrites can still hide a private channel;
# guild owners must explicitly allow the bot role in channels they want covered.
STANDARD_PERMISSION_NAMES = (
    "kick_members",
    "ban_members",
    "manage_channels",
    "manage_guild",
    "add_reactions",
    "view_audit_log",
    "view_channel",
    "send_messages",
    "manage_messages",
    "embed_links",
    "attach_files",
    "read_message_history",
    "use_external_emojis",
    "manage_roles",
    "manage_threads",
    "create_public_threads",
    "send_messages_in_threads",
    "moderate_members",
)


def standard_permissions() -> discord.Permissions:
    permissions = discord.Permissions.none()
    for name in STANDARD_PERMISSION_NAMES:
        setattr(permissions, name, True)
    return permissions


STANDARD_PERMISSIONS_VALUE = standard_permissions().value


def installation_url(client_id: int | str, *, administrator: bool = False) -> str:
    """Build the standard invite, with Administrator available only by opt-in."""
    permissions = (
        discord.Permissions(administrator=True).value
        if administrator
        else STANDARD_PERMISSIONS_VALUE
    )
    return "https://discord.com/oauth2/authorize?" + urlencode({
        "client_id": str(client_id),
        "scope": "bot applications.commands",
        "permissions": permissions,
    })
