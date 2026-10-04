import asyncio
import unittest
from collections import defaultdict
from types import SimpleNamespace
from unittest.mock import AsyncMock

from cogs.audit import AuditCog


def invite_snapshot(code, uses, max_uses=0):
    return {
        "code": code,
        "uses": uses,
        "max_uses": max_uses,
        "inviter_id": 123,
        "inviter_name": "Inviter",
        "deleted_at": None,
    }


class AuditInviteTrackingTests(unittest.IsolatedAsyncioTestCase):
    def make_cog(self, previous, current):
        cog = AuditCog.__new__(AuditCog)
        cog.invite_cache = {1: previous}
        cog.invite_locks = defaultdict(asyncio.Lock)
        cog.pending_invite_uses = defaultdict(list)
        cog._fetch_invite_snapshot = AsyncMock(return_value=current)
        return cog

    async def test_matches_invite_with_increased_use_count(self):
        old = {"abc": invite_snapshot("abc", 2)}
        current = {"abc": invite_snapshot("abc", 3)}
        cog = self.make_cog(old, current)

        invite, status = await cog._find_used_invite(SimpleNamespace(id=1))

        self.assertEqual(status, "matched")
        self.assertEqual(invite["code"], "abc")
        self.assertEqual(invite["uses"], 3)
        self.assertEqual(cog.invite_cache[1], current)

    async def test_matches_consumed_one_use_invite_that_disappeared(self):
        old = {"single": invite_snapshot("single", 0, max_uses=1)}
        cog = self.make_cog(old, {})

        invite, status = await cog._find_used_invite(SimpleNamespace(id=1))

        self.assertEqual(status, "matched")
        self.assertEqual(invite["code"], "single")
        self.assertEqual(invite["uses"], 1)

    async def test_queues_extra_matches_for_simultaneous_joins(self):
        old = {"party": invite_snapshot("party", 4)}
        current = {"party": invite_snapshot("party", 6)}
        cog = self.make_cog(old, current)
        guild = SimpleNamespace(id=1)

        first, first_status = await cog._find_used_invite(guild)
        second, second_status = await cog._find_used_invite(guild)

        self.assertEqual((first_status, second_status), ("matched", "matched"))
        self.assertEqual((first["code"], second["code"]), ("party", "party"))
        cog._fetch_invite_snapshot.assert_awaited_once()

    async def test_reports_unavailable_without_replacing_cache(self):
        old = {"abc": invite_snapshot("abc", 2)}
        cog = self.make_cog(old, None)

        invite, status = await cog._find_used_invite(SimpleNamespace(id=1))

        self.assertIsNone(invite)
        self.assertEqual(status, "unavailable")
        self.assertEqual(cog.invite_cache[1], old)


if __name__ == "__main__":
    unittest.main()
