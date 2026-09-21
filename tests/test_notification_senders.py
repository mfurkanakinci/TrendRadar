"""senders._post_batches: retry, structured logging, per-channel result handling."""

import logging
import unittest
from unittest.mock import MagicMock, patch

import requests

from trendradar.notification import senders


def _resp(status=200, body=None, text="ok"):
    r = MagicMock(spec=requests.Response)
    r.status_code = status
    r.text = text
    r.json.return_value = body if body is not None else {}
    return r


def _split(*_args, **_kwargs):
    return ["batch-1", "batch-2", "batch-3"]


class PostBatchesTests(unittest.TestCase):
    def setUp(self):
        for target, attr, value in (
            (senders.time, "sleep", MagicMock()),
            (senders, "RETRY_WAIT_MIN", 0),
            (senders, "RETRY_WAIT_MAX", 0),
        ):
            p = patch.object(target, attr, value)
            p.start()
            self.addCleanup(p.stop)

    def test_transient_error_is_retried_and_message_completes(self):
        ok = _resp(200, {"errcode": 0})
        side = [ok, requests.ConnectionError("boom"), _resp(429), ok, ok]
        with patch.object(senders.requests, "post", side_effect=side) as post:
            self.assertTrue(
                senders.send_to_dingtalk("https://hook", {}, "t", split_content_func=_split)
            )
        self.assertEqual(post.call_count, 5)
        for call in post.call_args_list:
            self.assertEqual(call.kwargs["timeout"], senders.REQUEST_TIMEOUT)

    def test_business_error_aborts_without_retry(self):
        side = [_resp(200, {"errcode": 0}), _resp(200, {"errcode": 1, "errmsg": "bad"})]
        with patch.object(senders.requests, "post", side_effect=side) as post, self.assertLogs(
            senders.logger, logging.WARNING
        ) as logs:
            self.assertFalse(
                senders.send_to_wework("https://hook", {}, "t", split_content_func=_split)
            )
        self.assertEqual(post.call_count, 2)
        self.assertIn("bad", "\n".join(logs.output))

    def test_exhausted_retries_fail_the_batch(self):
        with patch.object(
            senders.requests, "post", side_effect=requests.Timeout("slow")
        ) as post, self.assertLogs(senders.logger, logging.WARNING) as logs:
            self.assertFalse(
                senders.send_to_slack("https://hook", {}, "t", split_content_func=_split)
            )
        self.assertEqual(post.call_count, senders.RETRY_ATTEMPTS)
        self.assertIn("Timeout", "\n".join(logs.output))

    def test_ntfy_continues_after_failure_and_reports_partial(self):
        side = [_resp(200), _resp(413, text="too big"), _resp(200)]
        with patch.object(senders.requests, "post", side_effect=side) as post, self.assertLogs(
            senders.logger, logging.WARNING
        ) as logs:
            self.assertTrue(
                senders.send_to_ntfy(
                    "https://ntfy.sh", "topic", None, {}, "t", split_content_func=_split
                )
            )
        self.assertEqual(post.call_count, 3)
        titles = [c.kwargs["headers"]["Title"] for c in post.call_args_list]
        self.assertEqual(
            titles, ["News Report (3/3)", "News Report (2/3)", "News Report (1/3)"]
        )
        joined = "\n".join(logs.output)
        self.assertIn("413", joined)
        self.assertIn("2/3", joined)

    def test_unexpected_exception_logs_traceback(self):
        with patch.object(
            senders.requests, "post", return_value=_resp(200)
        ), patch.object(senders.json, "dumps", side_effect=TypeError("bug")), self.assertLogs(
            senders.logger, logging.ERROR
        ) as logs:
            self.assertFalse(
                senders.send_to_generic_webhook(
                    "https://hook", '{"c": "{content}"}', {}, "t", split_content_func=_split
                )
            )
        self.assertTrue(any(r.exc_info for r in logs.records))


if __name__ == "__main__":
    unittest.main()
