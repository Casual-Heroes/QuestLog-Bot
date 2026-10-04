import unittest

from utils.action_policy import ActionPolicyError, authorize_legacy_action


class FakePermissions:
    def __init__(self, **values):
        self.administrator = False
        for name, value in values.items():
            setattr(self, name, value)

    def __getattr__(self, _name):
        return False


class FakeRole:
    def __init__(self, role_id, position, *, administrator=False, managed=False):
        self.id = role_id
        self.position = position
        self.managed = managed
        self.permissions = FakePermissions(administrator=administrator)

    def __gt__(self, other):
        return self.position > other.position

    def is_default(self):
        return False


class FakeMember:
    def __init__(self, user_id, top_role, *, roles=(), **permissions):
        self.id = user_id
        self.top_role = top_role
        self.roles = list(roles)
        self.guild_permissions = FakePermissions(**permissions)


class FakeGuild:
    def __init__(self, *, owner_id=1):
        self.owner_id = owner_id
        self.roles = {}
        self.members = {}
        self.me = FakeMember(999, FakeRole(999, 100), administrator=True)
        self.members[self.me.id] = self.me

    def get_member(self, user_id):
        return self.members.get(user_id)

    def get_role(self, role_id):
        return self.roles.get(role_id)


class LegacyActionPolicyTests(unittest.TestCase):
    def setUp(self):
        self.now = 1_000_000
        self.guild = FakeGuild(owner_id=1)
        self.admin_role = FakeRole(10, 50)
        self.actor = FakeMember(
            2,
            self.admin_role,
            roles=[self.admin_role],
            manage_guild=True,
            manage_roles=True,
            moderate_members=True,
        )
        self.member = FakeMember(3, FakeRole(11, 10))
        self.guild.members.update({2: self.actor, 3: self.member})

    def authorize(self, action_type, payload, **overrides):
        values = {
            "guild": self.guild,
            "action_type": action_type,
            "payload": payload,
            "actor_id": self.actor.id,
            "created_at": self.now - 10,
            "now": self.now,
        }
        values.update(overrides)
        authorize_legacy_action(**values)

    def test_human_mutation_requires_a_live_actor(self):
        with self.assertRaisesRegex(ActionPolicyError, "no initiating actor"):
            self.authorize("message_send", {}, actor_id=None)

    def test_expired_human_mutation_is_rejected(self):
        with self.assertRaisesRegex(ActionPolicyError, "expired"):
            self.authorize("message_send", {}, created_at=self.now - 901)

    def test_configured_custom_admin_role_is_rechecked_live(self):
        delegated = FakeMember(4, FakeRole(12, 30), roles=[FakeRole(77, 20)])
        self.guild.members[4] = delegated
        self.authorize(
            "message_send",
            {},
            actor_id=4,
            custom_admin_role_ids=[77],
        )

    def test_legacy_lfg_system_delivery_has_bounded_compatibility_window(self):
        self.authorize("lfg_thread_update", {}, actor_id=None)
        with self.assertRaisesRegex(ActionPolicyError, "expired"):
            self.authorize(
                "lfg_thread_update",
                {},
                actor_id=None,
                created_at=self.now - 86_401,
            )

    def test_member_can_only_update_their_own_flair(self):
        self.authorize(
            "flair_assign",
            {"target_user_id": self.actor.id, "flair_name": "Raider"},
        )

        ordinary = FakeMember(5, FakeRole(14, 5))
        self.guild.members[5] = ordinary
        with self.assertRaisesRegex(ActionPolicyError, "manage_roles"):
            self.authorize(
                "flair_assign",
                {"target_user_id": self.actor.id, "flair_name": "Raider"},
                actor_id=5,
            )

    def test_administrator_role_cannot_be_assigned(self):
        dangerous = FakeRole(20, 20, administrator=True)
        self.guild.roles[dangerous.id] = dangerous
        with self.assertRaisesRegex(ActionPolicyError, "Administrator roles"):
            self.authorize("role_add", {"user_id": 3, "role_id": 20})

    def test_role_template_cannot_create_privileged_role(self):
        with self.assertRaisesRegex(ActionPolicyError, "administrator"):
            self.authorize(
                "role_create",
                {"template_data": '[{"name":"Admin","permissions":["administrator"]}]'},
            )

    def test_moderator_must_outrank_target(self):
        self.guild.members[3] = FakeMember(3, FakeRole(13, 60))
        with self.assertRaisesRegex(ActionPolicyError, "does not outrank"):
            self.authorize("member_timeout", {"user_id": 3})

    def test_bulk_actions_are_bounded(self):
        with self.assertRaisesRegex(ActionPolicyError, "limited to 100"):
            self.authorize("role_bulk_add", {
                "role_id": 123,
                "user_ids": list(range(101)),
            })


if __name__ == "__main__":
    unittest.main()
