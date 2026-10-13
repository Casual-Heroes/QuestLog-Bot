import unittest

from utils.amp_security import validate_amp_url


class AMPURLSecurityTests(unittest.TestCase):
    def test_https_url_is_normalized(self):
        self.assertEqual(
            validate_amp_url(" HTTPS://AMP.Example.COM:8443/base/ "),
            "https://amp.example.com:8443/base",
        )

    def test_empty_url_keeps_optional_feature_disabled(self):
        self.assertEqual(validate_amp_url(""), "")

    def test_unsafe_amp_urls_are_rejected(self):
        unsafe = (
            "http://amp.example.com:8080",
            "https://user:secret@amp.example.com",
            "//amp.example.com",
            "/relative/path",
            "https://amp.example.com/?token=secret",
            "not a url",
        )
        for value in unsafe:
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    validate_amp_url(value)


if __name__ == "__main__":
    unittest.main()
