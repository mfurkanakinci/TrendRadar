"""Concurrent RSS fetch keeps config order and rate-limits a single host."""

import time
import unittest
from unittest.mock import patch

from trendradar.crawler.rss.fetcher import RSSFeedConfig, RSSFetcher


def _feed(feed_id, url):
    return RSSFeedConfig(id=feed_id, name=feed_id, url=url)


class RSSConcurrencyTests(unittest.TestCase):
    def test_results_follow_config_order(self):
        feeds = [
            _feed("a", "https://a.example/rss"),
            _feed("b", "https://b.example/rss"),
            _feed("c", "https://c.example/rss"),
        ]
        fetcher = RSSFetcher(feeds, request_interval=0, max_workers=3, freshness_enabled=True)

        def fake_fetch(feed):
            return ([], None if feed.id != "b" else "boom")

        with patch.object(fetcher, "fetch_feed", side_effect=fake_fetch):
            data = fetcher.fetch_all()

        self.assertEqual(list(data.id_to_name), ["a", "b", "c"])
        self.assertEqual(data.failed_ids, ["b"])
        self.assertTrue(fetcher.freshness_enabled)

    def test_same_host_requests_are_serialized(self):
        feeds = [
            _feed("a", "https://news.example/a"),
            _feed("b", "https://news.example/b"),
        ]
        fetcher = RSSFetcher(feeds, request_interval=200, max_workers=2)
        starts = []

        def fake_fetch(feed):
            starts.append(time.monotonic())
            return ([], None)

        with patch.object(fetcher, "fetch_feed", side_effect=fake_fetch):
            fetcher.fetch_all()

        self.assertEqual(len(starts), 2)
        self.assertGreaterEqual(starts[1] - starts[0], 0.12)


if __name__ == "__main__":
    unittest.main()
