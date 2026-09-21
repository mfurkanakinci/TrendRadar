"""Hotlist fetcher retries only transient failures."""

import unittest
from unittest.mock import MagicMock, patch

import requests

from trendradar.crawler.fetcher import DataFetcher, NonRetryableFetchError, _is_transient


def _response(status=200, body='{"status":"success","items":[]}'):
    response = MagicMock()
    response.status_code = status
    response.text = body
    if status >= 400:
        error = requests.HTTPError(f"status {status}")
        error.response = response
        response.raise_for_status.side_effect = error
    else:
        response.raise_for_status.return_value = None
    return response


class FetcherRetryTests(unittest.TestCase):
    def test_client_error_is_not_retried(self):
        fetcher = DataFetcher(api_url="https://example.test/api")
        with patch("trendradar.crawler.fetcher.requests.get", return_value=_response(404)) as get:
            text, _, _ = fetcher.fetch_data("demo", max_retries=2)
        self.assertIsNone(text)
        self.assertEqual(get.call_count, 1)

    def test_server_error_is_retried(self):
        fetcher = DataFetcher(api_url="https://example.test/api")
        side = [_response(503), _response(200)]
        with patch("trendradar.crawler.fetcher.requests.get", side_effect=side) as get, patch(
            "trendradar.crawler.fetcher.time.sleep"
        ):
            text, _, _ = fetcher.fetch_data("demo", max_retries=2, min_retry_wait=0, max_retry_wait=0)
        self.assertIsNotNone(text)
        self.assertEqual(get.call_count, 2)

    def test_bad_json_is_not_retried(self):
        fetcher = DataFetcher(api_url="https://example.test/api")
        with patch("trendradar.crawler.fetcher.requests.get", return_value=_response(200, "not-json")) as get:
            text, _, _ = fetcher.fetch_data("demo", max_retries=2)
        self.assertIsNone(text)
        self.assertEqual(get.call_count, 1)

    def test_timeout_is_transient(self):
        self.assertTrue(_is_transient(requests.Timeout("slow")))
        self.assertIsInstance(NonRetryableFetchError("x"), Exception)


if __name__ == "__main__":
    unittest.main()
