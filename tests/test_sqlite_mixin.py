import sqlite3
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path

from trendradar.storage.base import NewsData, NewsItem
from trendradar.storage.sqlite_mixin import SQLiteStorageMixin


class FakeSQLiteBackend(SQLiteStorageMixin):
    """Minimal concrete backend: one on-disk SQLite file per db_type in a temp dir."""

    def __init__(self, root: Path):
        self.root = root
        self.connections: dict[str, sqlite3.Connection] = {}
        self.now = datetime(2026, 1, 15, 9, 0, 0, tzinfo=UTC)

    def _get_connection(self, date: str | None = None, db_type: str = "news") -> sqlite3.Connection:
        if db_type not in self.connections:
            conn = sqlite3.connect(self.root / f"{db_type}.db")
            self._init_tables(conn, db_type)
            self.connections[db_type] = conn
        return self.connections[db_type]

    def _get_configured_time(self) -> datetime:
        return self.now

    def _format_date_folder(self, date: str | None = None) -> str:
        return date or "2026-01-15"

    def _format_time_filename(self) -> str:
        return self.now.strftime("%H-%M")

    def close(self):
        for conn in self.connections.values():
            conn.close()


def make_data(crawl_time: str, items: dict[str, list[NewsItem]], failed_ids=None) -> NewsData:
    return NewsData(
        date="2026-01-15",
        crawl_time=crawl_time,
        items=items,
        id_to_name={source_id: source_id.upper() for source_id in items},
        failed_ids=list(failed_ids or []),
    )


def item(title: str, url: str, rank: int, source_id: str = "src") -> NewsItem:
    return NewsItem(title=title, source_id=source_id, rank=rank, url=url)


class SQLiteMixinTestCase(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.backend = FakeSQLiteBackend(Path(self.temp.name))
        self.addCleanup(self.backend.close)

    def items_by_title(self, data: NewsData, source_id: str = "src") -> dict[str, NewsItem]:
        return {news.title: news for news in data.items[source_id]}


class NewsRoundTripTests(SQLiteMixinTestCase):
    def test_save_then_read_round_trip(self):
        data = make_data(
            "09:00",
            {
                "src": [item("A", "https://x/a", 1), item("B", "https://x/b", 2)],
                "other": [item("C", "https://y/c", 1, "other")],
            },
            failed_ids=["broken"],
        )

        ok, new, updated, changed, off = self.backend._save_news_data_impl(data)

        self.assertEqual((ok, new, updated, changed, off), (True, 3, 0, 0, 0))

        stored = self.backend._get_today_all_data_impl("2026-01-15")
        self.assertIsNotNone(stored)
        assert stored is not None
        self.assertEqual(stored.date, "2026-01-15")
        self.assertEqual(stored.crawl_time, "09:00")
        self.assertEqual(stored.id_to_name, {"src": "SRC", "other": "OTHER"})
        self.assertEqual(stored.failed_ids, ["broken"])

        by_title = self.items_by_title(stored)
        self.assertEqual(set(by_title), {"A", "B"})
        self.assertEqual(by_title["A"].rank, 1)
        self.assertEqual(by_title["A"].url, "https://x/a")
        self.assertEqual(by_title["A"].first_time, "09:00")
        self.assertEqual(by_title["A"].last_time, "09:00")
        self.assertEqual(by_title["A"].count, 1)
        self.assertEqual(by_title["A"].ranks, [1])
        self.assertEqual(by_title["A"].rank_timeline, [{"time": "09:00", "rank": 1}])
        self.assertEqual(stored.items["other"][0].source_name, "OTHER")

        self.assertTrue(self.backend._is_first_crawl_today_impl("2026-01-15"))
        self.assertEqual(self.backend._get_crawl_times_impl("2026-01-15"), ["09:00"])

    def test_empty_db_reads_none(self):
        self.assertIsNone(self.backend._get_today_all_data_impl("2026-01-15"))
        self.assertIsNone(self.backend._get_latest_crawl_data_impl("2026-01-15"))
        self.assertEqual(self.backend._get_crawl_times_impl("2026-01-15"), [])
        self.assertTrue(self.backend._is_first_crawl_today_impl("2026-01-15"))

    def test_url_normalization_merges_tracking_variants(self):
        self.backend._save_news_data_impl(
            make_data("09:00", {"src": [item("A", "https://x/a?utm_source=tw", 1)]})
        )
        _, new, updated, _, _ = self.backend._save_news_data_impl(
            make_data("10:00", {"src": [item("A", "https://x/a", 3)]})
        )

        self.assertEqual((new, updated), (0, 1))
        stored = self.backend._get_today_all_data_impl("2026-01-15")
        assert stored is not None
        self.assertEqual(len(stored.items["src"]), 1)
        self.assertEqual(stored.items["src"][0].url, "https://x/a")
        self.assertEqual(stored.items["src"][0].count, 2)

    def test_items_without_url_are_never_merged(self):
        self.backend._save_news_data_impl(make_data("09:00", {"src": [item("A", "", 1)]}))
        _, new, updated, _, _ = self.backend._save_news_data_impl(
            make_data("10:00", {"src": [item("A", "", 2)]})
        )

        self.assertEqual((new, updated), (1, 0))
        stored = self.backend._get_today_all_data_impl("2026-01-15")
        assert stored is not None
        self.assertEqual(len(stored.items["src"]), 2)

    def test_title_change_is_recorded_and_url_like_title_is_rejected(self):
        self.backend._save_news_data_impl(make_data("09:00", {"src": [item("Old", "https://x/a", 1)]}))

        _, _, updated, changed, _ = self.backend._save_news_data_impl(
            make_data("10:00", {"src": [item("New", "https://x/a", 1)]})
        )
        self.assertEqual((updated, changed), (1, 1))

        conn = self.backend._get_connection()
        rows = conn.execute("SELECT old_title, new_title FROM title_changes").fetchall()
        self.assertEqual(rows, [("Old", "New")])

        _, _, updated, changed, _ = self.backend._save_news_data_impl(
            make_data("11:00", {"src": [item("https://x/a", "https://x/a", 1)]})
        )
        self.assertEqual((updated, changed), (1, 0))
        stored = self.backend._get_today_all_data_impl("2026-01-15")
        assert stored is not None
        self.assertEqual(stored.items["src"][0].title, "New")


class MergeAndOffListTests(SQLiteMixinTestCase):
    def test_second_crawl_updates_counts_and_detects_off_list(self):
        self.backend._save_news_data_impl(
            make_data("09:00", {"src": [item("A", "https://x/a", 1), item("B", "https://x/b", 2)]})
        )

        ok, new, updated, changed, off = self.backend._save_news_data_impl(
            make_data("10:00", {"src": [item("A", "https://x/a", 5), item("C", "https://x/c", 1)]})
        )

        self.assertEqual((ok, new, updated, changed, off), (True, 1, 1, 0, 1))
        self.assertFalse(self.backend._is_first_crawl_today_impl("2026-01-15"))
        self.assertEqual(self.backend._get_crawl_times_impl("2026-01-15"), ["09:00", "10:00"])

        stored = self.backend._get_today_all_data_impl("2026-01-15")
        assert stored is not None
        by_title = self.items_by_title(stored)
        self.assertEqual(by_title["A"].count, 2)
        self.assertEqual(by_title["A"].rank, 5)
        self.assertEqual(by_title["A"].ranks, [1, 5])
        self.assertEqual(by_title["A"].last_time, "10:00")
        self.assertEqual(by_title["B"].count, 1)
        self.assertEqual(by_title["B"].last_time, "09:00")

        off_rows = self.backend._get_connection().execute(
            "SELECT rh.crawl_time FROM rank_history rh JOIN news_items n ON n.id = rh.news_item_id "
            "WHERE n.title = 'B' AND rh.rank = 0"
        ).fetchall()
        self.assertEqual(off_rows, [("10:00",)])

    def test_off_list_is_recorded_only_once(self):
        self.backend._save_news_data_impl(make_data("09:00", {"src": [item("B", "https://x/b", 2)]}))
        _, _, _, _, off1 = self.backend._save_news_data_impl(make_data("10:00", {"src": [item("C", "https://x/c", 1)]}))
        _, _, _, _, off2 = self.backend._save_news_data_impl(make_data("11:00", {"src": [item("C", "https://x/c", 1)]}))

        self.assertEqual((off1, off2), (1, 0))

    def test_off_list_only_for_successfully_crawled_platforms(self):
        self.backend._save_news_data_impl(
            make_data("09:00", {"src": [item("A", "https://x/a", 1)], "other": [item("O", "https://y/o", 1, "other")]})
        )
        _, _, _, _, off = self.backend._save_news_data_impl(
            make_data("10:00", {"src": [item("Z", "https://x/z", 1)]}, failed_ids=["other"])
        )

        self.assertEqual(off, 1)
        latest = self.backend._get_latest_crawl_data_impl("2026-01-15")
        assert latest is not None
        self.assertEqual(latest.failed_ids, ["other"])

    def test_latest_crawl_returns_only_items_from_last_batch(self):
        self.backend._save_news_data_impl(
            make_data("09:00", {"src": [item("A", "https://x/a", 1), item("B", "https://x/b", 2)]})
        )
        self.backend._save_news_data_impl(make_data("10:00", {"src": [item("A", "https://x/a", 2)]}))

        latest = self.backend._get_latest_crawl_data_impl("2026-01-15")
        assert latest is not None
        self.assertEqual(latest.crawl_time, "10:00")
        self.assertEqual([n.title for n in latest.items["src"]], ["A"])
        self.assertEqual(latest.items["src"][0].ranks, [1, 2])

    def test_detect_new_titles(self):
        first = make_data("09:00", {"src": [item("A", "https://x/a", 1)]})
        self.assertEqual(self.backend._detect_new_titles_impl(first), {"src": {"A": first.items["src"][0]}})

        self.backend._save_news_data_impl(first)
        self.assertEqual(self.backend._detect_new_titles_impl(first), {})

        second = make_data("10:00", {"src": [item("A", "https://x/a", 2), item("B", "https://x/b", 1)]})
        new_titles = self.backend._detect_new_titles_impl(second)
        self.assertEqual(set(new_titles["src"]), {"B"})

        self.backend._save_news_data_impl(second)
        self.assertEqual(set(self.backend._detect_new_titles_impl(second)["src"]), {"B"})

        third = make_data("11:00", {"src": [item("B", "https://x/b", 1)]})
        self.assertEqual(self.backend._detect_new_titles_impl(third), {})


class RankHistoryTimelineTests(SQLiteMixinTestCase):
    def test_timeline_includes_off_list_then_return(self):
        self.backend._save_news_data_impl(make_data("09:00", {"src": [item("A", "https://x/a", 1), item("K", "https://x/k", 9)]}))
        self.backend._save_news_data_impl(make_data("10:00", {"src": [item("K", "https://x/k", 9)]}))
        self.backend._save_news_data_impl(make_data("11:00", {"src": [item("A", "https://x/a", 3), item("K", "https://x/k", 9)]}))

        stored = self.backend._get_today_all_data_impl("2026-01-15")
        assert stored is not None
        a = self.items_by_title(stored)["A"]
        self.assertEqual(a.ranks, [1, 3])
        self.assertEqual(a.count, 2)
        self.assertEqual(
            a.rank_timeline,
            [{"time": "09:00", "rank": 1}, {"time": "10:00", "rank": None}, {"time": "11:00", "rank": 3}],
        )

    def test_off_list_after_last_crawl_is_hidden_from_timeline(self):
        self.backend._save_news_data_impl(make_data("09:00", {"src": [item("A", "https://x/a", 1), item("K", "https://x/k", 9)]}))
        self.backend._save_news_data_impl(make_data("10:00", {"src": [item("K", "https://x/k", 9)]}))

        rows = self.backend._get_connection().execute(
            "SELECT rh.rank FROM rank_history rh JOIN news_items n ON n.id = rh.news_item_id WHERE n.title = 'A'"
        ).fetchall()
        self.assertEqual(rows, [(1,), (0,)])

        stored = self.backend._get_today_all_data_impl("2026-01-15")
        assert stored is not None
        a = self.items_by_title(stored)["A"]
        self.assertEqual(a.rank_timeline, [{"time": "09:00", "rank": 1}])
        self.assertEqual(a.ranks, [1])

    def test_ranks_are_deduplicated_but_timeline_is_complete(self):
        for crawl_time in ("09:00", "10:00", "11:00"):
            self.backend._save_news_data_impl(make_data(crawl_time, {"src": [item("A", "https://x/a", 2)]}))

        stored = self.backend._get_today_all_data_impl("2026-01-15")
        assert stored is not None
        a = stored.items["src"][0]
        self.assertEqual(a.ranks, [2])
        self.assertEqual([t["time"] for t in a.rank_timeline], ["09:00", "10:00", "11:00"])
        self.assertEqual(a.count, 3)

    def test_timeline_with_full_datetime_crawl_time(self):
        self.backend._save_news_data_impl(make_data("2026-01-15 09:30:00", {"src": [item("A", "https://x/a", 4)]}))

        stored = self.backend._get_today_all_data_impl("2026-01-15")
        assert stored is not None
        self.assertEqual(stored.items["src"][0].rank_timeline, [{"time": "09:30", "rank": 4}])

    def test_large_number_of_items_round_trips_with_history(self):
        count = 1200
        first = make_data("09:00", {"src": [item(f"T{i}", f"https://x/{i}", i + 1) for i in range(count)]})
        second = make_data("10:00", {"src": [item(f"T{i}", f"https://x/{i}", count - i) for i in range(count)]})
        ok, new, _, _, _ = self.backend._save_news_data_impl(first)
        self.assertEqual((ok, new), (True, count))
        ok, _, updated, _, _ = self.backend._save_news_data_impl(second)
        self.assertEqual((ok, updated), (True, count))

        max_id = self.backend._get_connection().execute("SELECT MAX(id) FROM news_items").fetchone()[0]
        self.assertGreaterEqual(max_id, count)

        stored = self.backend._get_today_all_data_impl("2026-01-15")
        self.assertIsNotNone(stored, "reading a large day must not fail")
        assert stored is not None
        self.assertEqual(len(stored.items["src"]), count)
        by_title = self.items_by_title(stored)
        self.assertEqual(by_title["T0"].ranks, [1, count])
        self.assertEqual(by_title[f"T{count - 1}"].ranks, [count, 1])
        self.assertTrue(all(len(n.rank_timeline) == 2 for n in stored.items["src"]))

        latest = self.backend._get_latest_crawl_data_impl("2026-01-15")
        assert latest is not None
        self.assertEqual(len(latest.items["src"]), count)


class PeriodExecutionTests(SQLiteMixinTestCase):
    def test_record_and_check_period_execution(self):
        self.assertFalse(self.backend._has_period_executed_impl("2026-01-15", "morning", "push"))
        self.assertTrue(self.backend._record_period_execution_impl("2026-01-15", "morning", "push"))
        self.assertTrue(self.backend._record_period_execution_impl("2026-01-15", "morning", "push"))
        self.assertTrue(self.backend._has_period_executed_impl("2026-01-15", "morning", "push"))
        self.assertFalse(self.backend._has_period_executed_impl("2026-01-15", "morning", "analyze"))


class AITagLifecycleTests(SQLiteMixinTestCase):
    def seed_news(self, count: int = 3) -> list[int]:
        self.backend._save_news_data_impl(
            make_data("09:00", {"src": [item(f"N{i}", f"https://x/{i}", i + 1) for i in range(count)]})
        )
        return [row["id"] for row in self.backend._get_all_news_ids_impl("2026-01-15")]

    def test_save_tags_and_query_active(self):
        tags = [
            {"tag": "Privacy", "description": "GDPR", "priority": 2},
            {"tag": "AI", "description": "LLM", "priority": 1},
            {"tag": "Misc", "priority": "bad"},
        ]
        self.assertEqual(self.backend._save_tags_impl("2026-01-15", tags, version=1, prompt_hash="h1"), 3)

        active = self.backend._get_active_tags_impl("2026-01-15")
        self.assertEqual([t["tag"] for t in active], ["AI", "Privacy", "Misc"])
        self.assertEqual([t["priority"] for t in active], [1, 2, 3])
        self.assertEqual(active[0]["description"], "LLM")
        self.assertEqual(self.backend._get_latest_prompt_hash_impl("2026-01-15"), "h1")
        self.assertEqual(self.backend._get_latest_tag_version_impl("2026-01-15"), 1)

        self.assertEqual(self.backend._get_active_tags_impl("2026-01-15", "other.txt"), [])
        self.assertIsNone(self.backend._get_latest_prompt_hash_impl("2026-01-15", "other.txt"))

    def test_deprecate_all_tags_cascades_to_results(self):
        news_ids = self.seed_news()
        self.backend._save_tags_impl("2026-01-15", [{"tag": "A"}], 1, "h1")
        self.backend._save_tags_impl("2026-01-15", [{"tag": "B"}], 1, "h-other", interests_file="other.txt")
        tag_a = self.backend._get_active_tags_impl("2026-01-15")[0]["id"]
        tag_b = self.backend._get_active_tags_impl("2026-01-15", "other.txt")[0]["id"]

        saved = self.backend._save_filter_results_impl(
            "2026-01-15",
            [
                {"news_item_id": news_ids[0], "tag_id": tag_a, "relevance_score": 0.9},
                {"news_item_id": news_ids[1], "tag_id": tag_a, "relevance_score": 0.5},
                {"news_item_id": news_ids[0], "tag_id": tag_b, "relevance_score": 0.7},
                {"news_item_id": news_ids[0], "tag_id": tag_a, "relevance_score": 0.1},
            ],
        )
        self.assertEqual(saved, 3)

        results = self.backend._get_active_filter_results_impl("2026-01-15")
        self.assertEqual([r["news_item_id"] for r in results], [news_ids[0], news_ids[1]])
        self.assertEqual(results[0]["tag"], "A")
        self.assertEqual(results[0]["title"], "N0")
        self.assertEqual(results[0]["source_name"], "SRC")
        self.assertEqual(results[0]["ranks"], [1])
        self.assertEqual(results[0]["rank_timeline"], [{"time": "09:00", "rank": 1}])

        self.assertEqual(self.backend._deprecate_all_tags_impl("2026-01-15"), 1)

        self.assertEqual(self.backend._get_active_tags_impl("2026-01-15"), [])
        self.assertEqual(self.backend._get_active_filter_results_impl("2026-01-15"), [])
        self.assertEqual(self.backend._get_latest_tag_version_impl("2026-01-15"), 1)

        conn = self.backend._get_connection()
        self.assertEqual(
            conn.execute("SELECT status, deprecated_at FROM ai_filter_tags WHERE id = ?", (tag_a,)).fetchone(),
            ("deprecated", "2026-01-15 09:00:00"),
        )
        self.assertEqual(
            conn.execute("SELECT COUNT(*) FROM ai_filter_results WHERE tag_id = ? AND status = 'deprecated'", (tag_a,)).fetchone()[0],
            2,
        )

        other = self.backend._get_active_filter_results_impl("2026-01-15", "other.txt")
        self.assertEqual([r["tag"] for r in other], ["B"])
        self.assertEqual(self.backend._deprecate_all_tags_impl("2026-01-15"), 0)

    def test_deprecate_specific_tags_and_update_hash(self):
        news_ids = self.seed_news()
        self.backend._save_tags_impl("2026-01-15", [{"tag": "A"}, {"tag": "B"}], 1, "h1")
        tag_a, tag_b = [t["id"] for t in self.backend._get_active_tags_impl("2026-01-15")]
        self.backend._save_filter_results_impl(
            "2026-01-15",
            [{"news_item_id": news_ids[0], "tag_id": tag_a}, {"news_item_id": news_ids[0], "tag_id": tag_b}],
        )

        self.assertEqual(self.backend._deprecate_specific_tags_impl("2026-01-15", []), 0)
        self.assertEqual(self.backend._deprecate_specific_tags_impl("2026-01-15", [tag_a]), 1)

        self.assertEqual([t["tag"] for t in self.backend._get_active_tags_impl("2026-01-15")], ["B"])
        results = self.backend._get_active_filter_results_impl("2026-01-15")
        self.assertEqual([r["tag_id"] for r in results], [tag_b])

        self.assertEqual(self.backend._update_tags_hash_impl("2026-01-15", "ai_interests.txt", "h2"), 1)
        self.assertEqual(self.backend._get_latest_prompt_hash_impl("2026-01-15"), "h2")
        conn = self.backend._get_connection()
        self.assertEqual(conn.execute("SELECT prompt_hash FROM ai_filter_tags WHERE id = ?", (tag_a,)).fetchone(), ("h1",))

    def test_update_descriptions_and_priorities_only_touch_active_tags(self):
        self.backend._save_tags_impl("2026-01-15", [{"tag": "A", "priority": 1}, {"tag": "B", "priority": 2}], 1, "h1")
        tag_a = self.backend._get_active_tags_impl("2026-01-15")[0]["id"]
        self.backend._deprecate_specific_tags_impl("2026-01-15", [tag_a])

        self.assertEqual(
            self.backend._update_tag_descriptions_impl("2026-01-15", [{"tag": "A", "description": "x"}, {"tag": "B", "description": "y"}, {"tag": ""}]),
            1,
        )
        self.assertEqual(
            self.backend._update_tag_priorities_impl("2026-01-15", [{"tag": "B", "priority": "7"}, {"tag": "B", "priority": None}]),
            1,
        )
        active = self.backend._get_active_tags_impl("2026-01-15")
        self.assertEqual(active, [{
            "id": active[0]["id"], "tag": "B", "description": "y", "version": 1, "prompt_hash": "h1", "priority": 7,
        }])

    def test_analyzed_news_tracking(self):
        news_ids = self.seed_news(4)
        saved = self.backend._save_analyzed_news_impl(
            "2026-01-15", news_ids, "hotlist", "ai_interests.txt", "h1", matched_ids={news_ids[0], news_ids[2]}
        )
        self.assertEqual(saved, 4)
        self.assertEqual(self.backend._get_analyzed_news_ids_impl("2026-01-15"), set(news_ids))
        self.assertEqual(self.backend._get_analyzed_news_ids_impl("2026-01-15", "rss"), set())
        self.assertEqual(self.backend._get_analyzed_news_ids_impl("2026-01-15", interests_file="other.txt"), set())

        self.assertEqual(self.backend._clear_unmatched_analyzed_news_impl("2026-01-15"), 2)
        self.assertEqual(self.backend._get_analyzed_news_ids_impl("2026-01-15"), {news_ids[0], news_ids[2]})

        self.assertEqual(self.backend._clear_analyzed_news_impl("2026-01-15"), 2)
        self.assertEqual(self.backend._get_analyzed_news_ids_impl("2026-01-15"), set())

    def test_get_all_news_ids(self):
        news_ids = self.seed_news(2)
        rows = self.backend._get_all_news_ids_impl("2026-01-15")
        self.assertEqual(len(rows), 2)
        self.assertEqual(sorted(r["id"] for r in rows), sorted(news_ids))
        self.assertEqual({r["title"] for r in rows}, {"N0", "N1"})


if __name__ == "__main__":
    unittest.main()
