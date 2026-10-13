import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class EnvironmentLoadingBoundaryTests(unittest.TestCase):
    def test_runtime_cogs_do_not_reopen_repository_dotenv(self):
        for relative_path in (
            "cogs/site_activity_tracker.py",
            "cogs/guild_sync.py",
        ):
            source = (ROOT / relative_path).read_text(encoding="utf-8")
            self.assertNotIn("load_dotenv", source, relative_path)

    def test_shared_database_module_prefers_production_secret_file(self):
        source = (ROOT / "db.py").read_text(encoding="utf-8")
        production_path = 'Path("/etc/casual-heroes/warden.env")'

        self.assertIn(production_path, source)
        self.assertLess(source.index(production_path), source.index("load_dotenv(override=False)"))

    def test_discord_message_cache_is_bounded_and_configurable(self):
        source = (ROOT / "config.py").read_text(encoding="utf-8")

        self.assertIn('"DISCORD_MESSAGE_CACHE_SIZE"', source)
        self.assertIn("default=25_000", source)
        self.assertIn("maximum=100_000", source)
        self.assertIn("max_messages=DISCORD_MESSAGE_CACHE_SIZE", source)


if __name__ == "__main__":
    unittest.main()
