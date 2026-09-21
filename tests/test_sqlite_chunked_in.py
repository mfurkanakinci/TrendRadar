import sqlite3
import unittest

from trendradar.utils.sqlite import execute_chunked_in


def _make_db(n: int) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE t (id INTEGER, v TEXT)")
    conn.executemany("INSERT INTO t VALUES (?, ?)", [(i, f"v{i}") for i in range(n)])
    return conn


class ExecuteChunkedInTests(unittest.TestCase):
    def test_more_ids_than_legacy_sqlite_variable_limit(self):
        conn = _make_db(3000)
        ids = list(range(2500))
        rows = execute_chunked_in(
            conn.cursor(),
            "SELECT id, v FROM t WHERE id IN ({placeholders})",
            ids,
            chunk_size=100,
        )
        self.assertEqual(len(rows), 2500)
        self.assertEqual({r[0] for r in rows}, set(ids))

    def test_dedupes_and_appends_extra_params(self):
        conn = _make_db(10)
        rows = execute_chunked_in(
            conn.cursor(),
            "SELECT id FROM t WHERE id IN ({placeholders}) AND v != ?",
            [1, 1, 2, 3, 3],
            extra_params=("v2",),
            chunk_size=2,
        )
        self.assertEqual(sorted(r[0] for r in rows), [1, 3])

    def test_empty_values(self):
        conn = _make_db(3)
        self.assertEqual(
            execute_chunked_in(
                conn.cursor(), "SELECT id FROM t WHERE id IN ({placeholders})", []
            ),
            [],
        )


if __name__ == "__main__":
    unittest.main()
