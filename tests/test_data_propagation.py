"""Storage read failures must propagate instead of looking like an empty day."""

import unittest

from trendradar.core.data import (
    detect_latest_new_titles_from_storage,
    read_all_today_titles_from_storage,
)


class _Boom:
    def get_today_all_data(self):
        raise RuntimeError("disk")

    def get_latest_crawl_data(self):
        raise RuntimeError("disk")


class DataPropagationTests(unittest.TestCase):
    def test_read_failure_propagates(self):
        with self.assertRaises(RuntimeError):
            read_all_today_titles_from_storage(_Boom())

    def test_new_title_detection_failure_propagates(self):
        with self.assertRaises(RuntimeError):
            detect_latest_new_titles_from_storage(_Boom())


if __name__ == "__main__":
    unittest.main()
