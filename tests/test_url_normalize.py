import unittest
from unittest.mock import patch

from trendradar.utils.url import normalize_url


class NormalizeUrlTests(unittest.TestCase):
    def test_empty_and_query_less_urls_are_returned_unchanged(self):
        self.assertEqual(normalize_url(""), "")
        self.assertEqual(normalize_url("https://example.com/a/b"), "https://example.com/a/b")

    def test_strips_common_tracking_params_and_fragment(self):
        self.assertEqual(
            normalize_url("https://example.com/page?id=1&utm_source=twitter&ref=x#top"),
            "https://example.com/page?id=1",
        )

    def test_strips_platform_specific_params(self):
        self.assertEqual(
            normalize_url("https://s.weibo.com/weibo?q=test&t=31&band_rank=6&Refer=top", "weibo"),
            "https://s.weibo.com/weibo?q=test",
        )
        # Platform rules only apply to the matching platform.
        self.assertEqual(
            normalize_url("https://example.com/?q=test&band_rank=6", "other"),
            "https://example.com/?band_rank=6&q=test",
        )

    def test_sorts_params_so_variants_share_a_dedup_key(self):
        a = normalize_url("https://example.com/?b=2&a=1")
        b = normalize_url("https://example.com/?a=1&b=2&utm_medium=mail")
        self.assertEqual(a, b)
        self.assertEqual(a, "https://example.com/?a=1&b=2")

    def test_drops_query_entirely_when_only_tracking_params_remain(self):
        self.assertEqual(
            normalize_url("https://example.com/path?utm_source=a&utm_campaign=b#frag"),
            "https://example.com/path",
        )

    def test_malformed_url_falls_back_to_raw_url_and_logs_warning(self):
        malformed = "http://[::1/path?a=1"
        with self.assertLogs("trendradar.utils.url", level="WARNING") as captured:
            self.assertEqual(normalize_url(malformed, "weibo"), malformed)
        self.assertEqual(len(captured.records), 1)
        record = captured.records[0]
        self.assertIn("normalize_url failed", record.getMessage())
        self.assertIn(malformed, record.getMessage())
        self.assertIsNotNone(record.exc_info)
        self.assertIs(record.exc_info[0], ValueError)

    def test_parse_failure_inside_query_handling_is_logged(self):
        url = "https://example.com/?a=1"
        with patch("trendradar.utils.url.parse_qs", side_effect=ValueError("boom")):
            with self.assertLogs("trendradar.utils.url", level="WARNING") as captured:
                self.assertEqual(normalize_url(url), url)
        self.assertIsNotNone(captured.records[0].exc_info)

    def test_well_formed_url_emits_no_warning(self):
        with self.assertNoLogs("trendradar.utils.url", level="WARNING"):
            normalize_url("https://example.com/?a=1&utm_source=x")


if __name__ == "__main__":
    unittest.main()
