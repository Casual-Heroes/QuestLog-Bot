import unittest

from cogs.channel_directory import (
    ChannelDirectoryCog,
    MAX_EMBED_LENGTH,
    MAX_FIELD_LENGTH,
)


class ChannelDirectoryFormattingTests(unittest.TestCase):
    def test_normalizes_supported_directory_channel_names(self):
        normalize = ChannelDirectoryCog._normalized_channel_name

        self.assertEqual(normalize("channel-directory"), "channel-directory")
        self.assertEqual(normalize("Channel Directory"), "channel-directory")
        self.assertEqual(normalize("channel_directory"), "channel-directory")

    def test_splits_category_lines_at_embed_field_limit(self):
        lines = ["a" * 600, "b" * 600, "short"]

        chunks = ChannelDirectoryCog._split_lines(lines)

        self.assertEqual(len(chunks), 2)
        self.assertTrue(all(len(chunk) <= MAX_FIELD_LENGTH for chunk in chunks))
        self.assertTrue(chunks[1].endswith("short"))

    def test_packs_no_more_than_twenty_five_fields_per_embed(self):
        fields = [(f"Category {index}", "channel") for index in range(30)]

        pages = ChannelDirectoryCog._pack_fields(fields)

        self.assertEqual([len(page) for page in pages], [25, 5])

    def test_packs_fields_below_embed_character_budget(self):
        fields = [(f"Category {index}", "x" * 1000) for index in range(12)]

        pages = ChannelDirectoryCog._pack_fields(fields)

        for page in pages:
            length = sum(len(name) + len(value) for name, value in page)
            self.assertLessEqual(length, MAX_EMBED_LENGTH)


if __name__ == "__main__":
    unittest.main()
