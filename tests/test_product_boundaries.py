import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]


def source(relative_path: str) -> str:
    return (ROOT / relative_path).read_text(encoding="utf-8")


class ProductBoundaryTests(unittest.TestCase):
    def test_legacy_duplicate_products_are_disabled_by_default(self):
        config = source("config.py")
        example = source(".env.example")
        bot = source("bot.py")

        self.assertIn('"ENABLE_LEGACY_STREAMING_MONITOR", "false"', config)
        self.assertIn('"ENABLE_LEGACY_DISCORD_FLAIR_STORE", "false"', config)
        self.assertIn('"ENABLE_EMERGENCY_SERVICE_CONTROL", "false"', config)
        self.assertIn('os.getenv("ENABLE_BRIDGE")', config)
        self.assertIn("ENABLE_BRIDGE_IMPLICIT", config)
        self.assertIn('"ENABLE_LEGACY_SITE_ACTIVITY_EXPORT", "true"', config)
        self.assertIn("ENABLE_LEGACY_STREAMING_MONITOR=false", example)
        self.assertIn("ENABLE_LEGACY_DISCORD_FLAIR_STORE=false", example)
        self.assertIn("ENABLE_EMERGENCY_SERVICE_CONTROL=false", example)
        self.assertIn("ENABLE_BRIDGE=false", example)
        self.assertIn("ENABLE_LEGACY_SITE_ACTIVITY_EXPORT=true", example)
        self.assertNotIn('        "cogs.streaming_monitor",', bot)
        self.assertNotIn('        "cogs.flair_cog",', bot)
        self.assertNotIn('        "cogs.emergency",', bot)
        self.assertNotIn('        "cogs.bridge_cog",', bot)
        self.assertNotIn('        "cogs.site_activity_tracker",', bot)

    def test_security_cogs_fail_closed(self):
        bot = source("bot.py")

        for cog in (
            "cogs.core",
            "cogs.security",
            "cogs.verification",
            "cogs.audit",
            "cogs.moderation",
            "cogs.action_processor",
        ):
            self.assertIn(f'"{cog}"', bot)
        self.assertIn("failed_critical_cogs", bot)
        self.assertIn("Refusing to start without critical cogs", bot)

    def test_free_features_do_not_call_removed_tier_helpers(self):
        verification = source("cogs/verification.py")
        roles = source("cogs/roles.py")
        security = source("cogs/security.py")

        self.assertNotIn("has_moderation_access", verification)
        self.assertNotIn("class FeatureLimits", source("config.py"))
        self.assertNotIn("SubscriptionTier", source("__init__.py"))
        self.assertNotIn("{limit}** members (tier limit)", roles)
        self.assertNotIn("{tier} tier", security)

    def test_obsolete_vip_and_local_lfg_admin_commands_are_removed(self):
        admin = source("cogs/admin.py")

        self.assertNotIn("SlashCommandGroup(\n        name=\"vip\"", admin)
        self.assertNotIn('name="purgelfgs"', admin)
        self.assertNotIn("w!vip", admin)

    def test_internal_api_has_bounded_constant_time_auth(self):
        api = source("api_server.py")

        self.assertIn("secrets.compare_digest", api)
        self.assertIn("client_max_size=64 * 1024", api)
        self.assertIn("len(API_TOKEN) < 32", api)
        self.assertIn("'Authorization': f'Bearer {api_token}'", source("cogs/action_processor.py"))

    def test_legacy_action_queue_has_execution_boundary_policy(self):
        processor = source("cogs/action_processor.py")
        policy = source("utils/action_policy.py")

        self.assertIn("authorize_legacy_action(", processor)
        self.assertIn("_quarantine_stale_processing_actions", processor)
        self.assertIn("AllowedMentions.none()", processor)
        self.assertIn("DANGEROUS_ROLE_PERMISSIONS", policy)
        self.assertIn("custom_admin_role_ids", policy)
        self.assertIn("Human-triggered legacy action has expired", policy)

    def test_audit_defaults_to_metadata_only_and_bounded_retention(self):
        audit = source("cogs/audit.py")
        example = source(".env.example")

        self.assertIn('os.getenv("AUDIT_DELETED_CONTENT_ENABLED", "false")', audit)
        self.assertIn('os.getenv(name, str(default))', audit)
        self.assertIn('"AUDIT_RETENTION_DAYS", 90', audit)
        self.assertIn('os.getenv("AUDIT_RETENTION_ENFORCEMENT_ENABLED", "true")', audit)
        self.assertIn("AuditLog.timestamp < audit_cutoff", audit)
        self.assertIn("AUDIT_RETENTION_DAYS=90", example)
        self.assertIn("AUDIT_RETENTION_ENFORCEMENT_ENABLED=true", example)
        self.assertIn("AUDIT_DELETED_CONTENT_ENABLED=false", example)

    def test_invite_codes_are_not_written_to_logs(self):
        invite = source("cogs/invite.py")

        self.assertNotIn("code {code_str} sent", invite)

    def test_flair_delivery_is_not_acknowledged_after_failure(self):
        flair_sync = source("cogs/flair_sync.py")

        failure = flair_sync.index("except Exception as e:", flair_sync.index("for row in rows:"))
        acknowledgement = flair_sync.index("UPDATE discord_pending_role_updates")
        self.assertLess(failure, acknowledgement)
        self.assertIn("retained for retry", flair_sync[failure:acknowledgement])

    def test_flair_delivery_uses_per_guild_receipts_and_owned_role_ids(self):
        flair_sync = source("cogs/flair_sync.py")
        migration = source("migrations/flair_delivery_receipts.sql")

        self.assertIn("warden_flair_delivery_receipts", flair_sync)
        self.assertIn("warden_flair_role_bindings", flair_sync)
        self.assertIn("Only role IDs created and bound by Warden are managed", flair_sync)
        self.assertNotIn("r.name.startswith(FLAIR_ROLE_PREFIX)", flair_sync)
        self.assertIn("permissions=discord.Permissions.none()", flair_sync)
        self.assertIn("UNIQUE KEY uq_warden_flair_delivery", migration)
        self.assertIn("UNIQUE KEY uq_warden_flair_binding_role", migration)

    def test_legacy_site_export_keeps_blocking_io_off_event_loop(self):
        tracker = source("cogs/site_activity_tracker.py")

        self.assertIn("await asyncio.to_thread(self.load_config_from_db)", tracker)
        self.assertIn("await asyncio.to_thread(self._write_counts, counts)", tracker)

    def test_required_discord_workflows_remain_registered(self):
        expected = {
            "cogs/activity_tracker.py": ("bot.add_cog(ActivityTrackerCog(bot))", '@tracker_group.command(name="add"', '@tracker_group.command(name="edit"'),
            "cogs/welcome.py": ("bot.add_cog(WelcomeCog(bot))", '@welcome.command(name="config"', '@welcome.command(name="set-embed"'),
            "cogs/admin.py": ("bot.add_cog(AdminCog(bot))", '@message.command(name="send_embed"'),
            "cogs/discovery.py": ("bot.add_cog(DiscoveryCog(bot))", '@discovery.command(name="game-settings"', '@discovery.command(name="game-filters"'),
            "cogs/lfg_cog.py": ("bot.add_cog(LFGCog(bot))", '@discord.slash_command(name="lfg_search"', '@discord.slash_command(name="lfg_setup"'),
            "cogs/verification.py": ("bot.add_cog(VerificationCog(bot))", '@verify.command(name="config"', '@verify.command(name="setup"'),
            "cogs/nominations.py": ("bot.add_cog(NominationsCog(bot))", '@nominations_group.command(name="nominate"'),
            "cogs/xp.py": ("bot.add_cog(XPCog(bot))", '@xp.command(name="export-members"'),
            "cogs/roles.py": ("bot.add_cog(RolesCog(bot))", '@iam.command(name="mass-assign"', '@roles.command(name="add-react"'),
            "cogs/channels.py": ("bot.add_cog(ChannelsCog(bot))", '@channels.command(name="save-template"', '@channels.command(name="mass-delete"'),
            "cogs/audit.py": ("bot.add_cog(AuditCog(bot))", '@audit.command(name="search"', '@audit.command(name="config"'),
        }

        for path, markers in expected.items():
            with self.subTest(path=path):
                cog_source = source(path)
                self.assertNotIn("add_cog_with_command_policy", cog_source)
                for marker in markers:
                    self.assertIn(marker, cog_source)

    def test_discord_presence_always_reports_total_community_count(self):
        bot = source("bot.py")
        presence_block = bot[
            bot.index("PRESENCE_MESSAGES = ["):bot.index("]", bot.index("PRESENCE_MESSAGES = ["))
        ]
        templates = [
            line for line in presence_block.splitlines() if '("' in line
        ]
        self.assertTrue(templates)
        for template in templates:
            self.assertIn("{server_count} communities", template)

        self.assertGreaterEqual(
            bot.count('f"{len(bot.guilds)} communities | /questlog help"'),
            3,
        )
        core = source("cogs/core.py")
        self.assertIn("Installed communities", core)
        self.assertIn("Mutual Servers tab", core)


if __name__ == "__main__":
    unittest.main()
