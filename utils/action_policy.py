"""Defense-in-depth policy for legacy website-to-Warden queue actions.

The shared queue is being retired.  Until that migration is complete, every
human-triggered mutation must be authorized again against live Discord state
immediately before execution.  This module intentionally contains no database
or network access so the policy can be tested independently.
"""

from __future__ import annotations

import json
import time
from collections.abc import Mapping


class ActionPolicyError(ValueError):
    """Raised when a legacy action is unsafe or no longer authorized."""


MAX_PAYLOAD_BYTES = 64 * 1024
MAX_BULK_MEMBERS = 100
HUMAN_ACTION_MAX_AGE_SECONDS = 15 * 60
INTEGRATION_ACTION_MAX_AGE_SECONDS = 24 * 60 * 60

# These are compatibility-only system deliveries.  They must move to their
# scoped adapters; no new action type should be added here.
LEGACY_INTEGRATION_ACTIONS = frozenset({
    "flair_assign",
    "lfg_thread_create",
    "lfg_thread_update",
    "lfg_thread_delete",
})

PERMISSION_BY_ACTION = {
    "role_add": "manage_roles",
    "role_remove": "manage_roles",
    "role_bulk_add": "manage_roles",
    "role_bulk_remove": "manage_roles",
    "xp_add": "manage_guild",
    "xp_remove": "manage_guild",
    "xp_set": "manage_guild",
    "xp_bulk_set": "manage_guild",
    "tokens_add": "manage_guild",
    "tokens_remove": "manage_guild",
    "tokens_set": "manage_guild",
    "member_kick": "kick_members",
    "member_ban": "ban_members",
    "member_unban": "ban_members",
    "member_timeout": "moderate_members",
    "member_untimeout": "moderate_members",
    "warning_add": "moderate_members",
    "warning_pardon": "moderate_members",
    "message_send": "manage_guild",
    "dm_send": "manage_guild",
    "boost_event_start": "manage_guild",
    "channel_topic_set": "manage_channels",
    "force_feature": "manage_guild",
    "clear_featured": "manage_guild",
    "test_channel_embed": "manage_guild",
    "test_forum_embed": "manage_guild",
    "check_games": "manage_guild",
    "flair_assign": "manage_roles",
    "flair_seed_roles": "manage_roles",
    "channel_create": "manage_channels",
    "role_create": "manage_roles",
    "rss_test_send": "manage_guild",
}

ROLE_ACTIONS = frozenset({
    "role_add", "role_remove", "role_bulk_add", "role_bulk_remove",
})
MODERATION_ACTIONS = frozenset({
    "member_kick", "member_ban", "member_timeout", "member_untimeout",
})
DANGEROUS_ROLE_PERMISSIONS = frozenset({
    "administrator",
    "manage_guild",
    "manage_roles",
    "manage_channels",
    "kick_members",
    "ban_members",
    "moderate_members",
    "manage_webhooks",
    "mention_everyone",
})


def _action_name(action_type) -> str:
    return str(getattr(action_type, "value", action_type))


def _role_ids(member) -> set[int]:
    return {
        int(role.id)
        for role in getattr(member, "roles", ())
        if getattr(role, "id", None) is not None
    }


def _is_owner(guild, member) -> bool:
    return int(getattr(guild, "owner_id", 0) or 0) == int(member.id)


def _is_custom_admin(member, custom_admin_role_ids) -> bool:
    configured = {int(role_id) for role_id in custom_admin_role_ids or ()}
    return bool(configured & _role_ids(member))


def _outranks(member, target) -> bool:
    if int(member.id) == int(target.id):
        return True
    return getattr(member, "top_role", None) > getattr(target, "top_role", None)


def _validate_role_template(payload: Mapping) -> None:
    raw = payload.get("template_data", "[]")
    try:
        roles = json.loads(raw) if isinstance(raw, str) else raw
    except (TypeError, ValueError) as exc:
        raise ActionPolicyError("Role template is not valid JSON") from exc
    if not isinstance(roles, list) or len(roles) > 50:
        raise ActionPolicyError("Role template must contain at most 50 roles")
    for role in roles:
        if not isinstance(role, Mapping):
            raise ActionPolicyError("Role template entries must be objects")
        requested = {str(name) for name in role.get("permissions", ())}
        dangerous = requested & DANGEROUS_ROLE_PERMISSIONS
        if dangerous:
            names = ", ".join(sorted(dangerous))
            raise ActionPolicyError(
                f"Legacy role templates cannot grant privileged permissions: {names}"
            )


def _validate_role_target(guild, actor, payload: Mapping) -> None:
    role_id = payload.get("role_id")
    if role_id is None:
        raise ActionPolicyError("Role action is missing role_id")
    role = guild.get_role(int(role_id))
    if role is None:
        raise ActionPolicyError("Role no longer exists")
    if getattr(role, "is_default", lambda: False)() or getattr(role, "managed", False):
        raise ActionPolicyError("Default and integration-managed roles cannot be changed")
    if getattr(getattr(role, "permissions", None), "administrator", False):
        raise ActionPolicyError("Administrator roles cannot be assigned through the legacy queue")

    bot_member = getattr(guild, "me", None)
    if bot_member is None or not _outranks(bot_member, type("RoleTarget", (), {
        "id": -1, "top_role": role,
    })()):
        raise ActionPolicyError("Warden does not outrank the requested role")
    if not _is_owner(guild, actor) and not (
        getattr(actor, "top_role", None) > role
    ):
        raise ActionPolicyError("The initiating administrator does not outrank the role")


def _validate_member_targets(guild, actor, payload: Mapping, action_name: str) -> None:
    target_ids = []
    if "user_id" in payload:
        target_ids.append(payload["user_id"])
    if "target_user_id" in payload:
        target_ids.append(payload["target_user_id"])
    if "user_ids" in payload:
        user_ids = payload["user_ids"]
        if not isinstance(user_ids, list) or len(user_ids) > MAX_BULK_MEMBERS:
            raise ActionPolicyError(
                f"Legacy bulk actions are limited to {MAX_BULK_MEMBERS} members"
            )
        target_ids.extend(user_ids)

    if action_name == "xp_bulk_set":
        users = payload.get("users")
        if not isinstance(users, list) or len(users) > MAX_BULK_MEMBERS:
            raise ActionPolicyError(
                f"Legacy bulk actions are limited to {MAX_BULK_MEMBERS} members"
            )
        target_ids.extend(item.get("user_id") for item in users if isinstance(item, Mapping))

    hierarchy_required = action_name in MODERATION_ACTIONS
    for target_id in target_ids:
        try:
            member = guild.get_member(int(target_id))
        except (TypeError, ValueError) as exc:
            raise ActionPolicyError("Member target is invalid") from exc
        if member is None:
            # Unban legitimately targets a user who is no longer a member.
            if action_name == "member_unban":
                continue
            raise ActionPolicyError("Member target is no longer in the community")
        if hierarchy_required:
            if int(member.id) == int(getattr(guild, "owner_id", 0) or 0):
                raise ActionPolicyError("The Discord community owner cannot be moderated")
            if not _is_owner(guild, actor) and not _outranks(actor, member):
                raise ActionPolicyError("The initiating moderator does not outrank the target")
            bot_member = getattr(guild, "me", None)
            if bot_member is None or not _outranks(bot_member, member):
                raise ActionPolicyError("Warden does not outrank the moderation target")


def authorize_legacy_action(
    *,
    guild,
    action_type,
    payload,
    actor_id,
    created_at,
    custom_admin_role_ids=(),
    now: int | None = None,
) -> None:
    """Validate one queued action against live Discord state.

    Compatibility-only LFG/flair deliveries may be system-generated. All other
    actions require a live owner, native Discord permission, or a currently held
    configured custom-admin role.
    """

    action_name = _action_name(action_type)
    if not isinstance(payload, Mapping):
        raise ActionPolicyError("Action payload must be a JSON object")
    if len(json.dumps(payload, separators=(",", ":")).encode("utf-8")) > MAX_PAYLOAD_BYTES:
        raise ActionPolicyError("Action payload exceeds the 64 KiB limit")

    now = int(time.time()) if now is None else int(now)
    created_at = int(created_at or 0)
    age = max(0, now - created_at)

    if action_name in LEGACY_INTEGRATION_ACTIONS and not actor_id:
        if age > INTEGRATION_ACTION_MAX_AGE_SECONDS:
            raise ActionPolicyError("Legacy integration action has expired")
        return

    if action_name == "flair_assign" and actor_id:
        if age > HUMAN_ACTION_MAX_AGE_SECONDS:
            raise ActionPolicyError("Human-triggered legacy action has expired")
        try:
            target_user_id = int(payload.get("target_user_id"))
        except (TypeError, ValueError) as exc:
            raise ActionPolicyError("Flair target is invalid") from exc
        actor = guild.get_member(int(actor_id))
        if actor is None:
            raise ActionPolicyError("The initiating actor is no longer in the community")
        # A QuestLog member may update only their own equipped flair. Admin
        # changes for another member continue through manage_roles below.
        if int(actor_id) == target_user_id:
            return

    if age > HUMAN_ACTION_MAX_AGE_SECONDS:
        raise ActionPolicyError("Human-triggered legacy action has expired")
    if not actor_id:
        raise ActionPolicyError("Human-triggered legacy action has no initiating actor")

    actor = guild.get_member(int(actor_id))
    if actor is None:
        raise ActionPolicyError("The initiating actor is no longer in the community")

    permissions = getattr(actor, "guild_permissions", None)
    administrator = bool(getattr(permissions, "administrator", False))
    custom_admin = _is_custom_admin(actor, custom_admin_role_ids)
    required_permission = PERMISSION_BY_ACTION.get(action_name)
    if required_permission is None:
        raise ActionPolicyError("Action type is not allowed by the legacy policy")
    if not (
        _is_owner(guild, actor)
        or administrator
        or custom_admin
        or bool(getattr(permissions, required_permission, False))
    ):
        raise ActionPolicyError(
            f"The initiating actor no longer has {required_permission}"
        )

    _validate_member_targets(guild, actor, payload, action_name)
    if action_name in ROLE_ACTIONS:
        _validate_role_target(guild, actor, payload)
    if action_name == "role_create":
        _validate_role_template(payload)
