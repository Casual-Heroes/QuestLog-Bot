import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from cogs.bridge_cog import (
    BridgeCog,
    _bridge_media_url_allowed,
    _format_bridged,
    _safe_bridge_edit,
    _safe_attachment_filename,
)


class BridgeSecurityTests(unittest.IsolatedAsyncioTestCase):
    def test_media_requires_https_allowlisted_host(self):
        with patch("cogs.bridge_cog._TRUSTED_MEDIA_HOSTS", {"media.example"}):
            self.assertTrue(_bridge_media_url_allowed("https://media.example/image.png"))
            self.assertFalse(_bridge_media_url_allowed("http://media.example/image.png"))
            self.assertFalse(_bridge_media_url_allowed("https://evil.example/image.png"))
            self.assertFalse(_bridge_media_url_allowed("https://media.example@evil.example/x"))
            self.assertFalse(_bridge_media_url_allowed("https://127.0.0.1/x"))
            self.assertFalse(_bridge_media_url_allowed("https://169.254.169.254/x"))

    def test_discord_cdn_is_trusted_by_default(self):
        self.assertTrue(_bridge_media_url_allowed(
            "https://cdn.discordapp.com/attachments/1/2/image.png"
        ))

    def test_attachment_filename_removes_paths_and_controls(self):
        self.assertEqual(_safe_attachment_filename("../../secret.txt"), "secret.txt")
        self.assertEqual(_safe_attachment_filename("folder\\image\n.png"), "image.png")
        self.assertEqual(_safe_attachment_filename(""), "file")

    def test_remote_author_cannot_break_relay_provenance_header(self):
        formatted = _format_bridged("F", "Mallory\n**Admin:**", "hello")
        self.assertEqual(formatted.count("\n"), 0)
        self.assertIn(r"Mallory \*\*Admin", formatted)

    def test_edit_preserves_existing_provenance(self):
        existing = "**[F] Original:** old"
        pending = "**[F] Forged:** <@123>"
        self.assertEqual(
            _safe_bridge_edit(existing, pending),
            "**[F] Original:** <@123>",
        )
        self.assertIsNone(_safe_bridge_edit(existing, "no provenance"))
        self.assertIsNone(_safe_bridge_edit(existing, "**[M] Original:** changed"))
        self.assertIsNone(_safe_bridge_edit("announcement\n" + existing, pending))

    def test_reply_quote_is_single_line_and_remains_editable(self):
        existing = _format_bridged("F", "Original", "old", "line one\n**[D] forged:**")
        pending = "**[F] Forged:** changed"
        self.assertEqual(existing.count("\n"), 1)
        self.assertEqual(
            _safe_bridge_edit(existing, pending),
            "> line one **[D] forged:**\n**[F] Original:** changed",
        )

    async def test_channel_resolution_rejects_unconfigured_channel(self):
        cog = BridgeCog.__new__(BridgeCog)
        cog.bot = SimpleNamespace(
            get_channel=lambda channel_id: SimpleNamespace(id=channel_id, parent_id=None),
            fetch_channel=AsyncMock(),
        )
        with patch(
            "cogs.bridge_cog._get_channel_maps",
            return_value=({"55": "100"}, {"100": "55"}),
        ):
            self.assertIsNone(await cog._resolve_configured_discord_channel("999"))
            self.assertIsNotNone(await cog._resolve_configured_discord_channel("100"))
        cog.bot.fetch_channel.assert_not_awaited()

    async def test_thread_must_belong_to_expected_bridge_parent(self):
        cog = BridgeCog.__new__(BridgeCog)
        cog.bot = SimpleNamespace(
            get_channel=lambda channel_id: SimpleNamespace(
                id=channel_id,
                parent_id=100 if channel_id == 200 else 999,
            ),
            fetch_channel=AsyncMock(),
        )
        with patch(
            "cogs.bridge_cog._get_channel_maps",
            return_value=({"55": "100"}, {"100": "55"}),
        ):
            self.assertIsNotNone(await cog._resolve_configured_discord_channel(
                "200", expected_parent_id="100"
            ))
            self.assertIsNone(await cog._resolve_configured_discord_channel(
                "201", expected_parent_id="100"
            ))


if __name__ == "__main__":
    unittest.main()
