import unittest
from urllib.parse import parse_qs, urlsplit

from utils.discord_permissions import (
    STANDARD_PERMISSIONS_VALUE,
    installation_url,
    standard_permissions,
)


class DiscordInstallationPermissionTests(unittest.TestCase):
    def test_standard_permissions_never_grant_administrator(self):
        permissions = standard_permissions()

        self.assertFalse(permissions.administrator)
        self.assertTrue(permissions.manage_roles)
        self.assertTrue(permissions.manage_channels)
        self.assertTrue(permissions.moderate_members)
        self.assertEqual(STANDARD_PERMISSIONS_VALUE, 1426197966070)

    def test_administrator_invite_requires_explicit_opt_in(self):
        standard = parse_qs(urlsplit(installation_url(123)).query)
        elevated = parse_qs(urlsplit(
            installation_url(123, administrator=True)
        ).query)

        self.assertEqual(standard["permissions"], [str(STANDARD_PERMISSIONS_VALUE)])
        self.assertEqual(elevated["permissions"], ["8"])


if __name__ == "__main__":
    unittest.main()
