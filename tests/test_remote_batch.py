"""Remote batch nesting and head_object error classification."""

import unittest
from unittest.mock import MagicMock

from botocore.exceptions import ClientError

from trendradar.storage.remote import RemoteStorageBackend


def _client_error(code, status):
    return ClientError(
        {
            "Error": {"Code": code, "Message": "denied"},
            "ResponseMetadata": {"HTTPStatusCode": status},
        },
        "HeadObject",
    )


def _backend():
    backend = object.__new__(RemoteStorageBackend)
    backend._batch_depth = 0
    backend._batch_dirty = set()
    backend.s3_client = MagicMock()
    backend.bucket_name = "bucket"
    return backend


class RemoteBatchTests(unittest.TestCase):
    def test_nested_end_batch_uploads_only_at_outermost_level(self):
        backend = _backend()
        uploads = []
        backend._upload_sqlite = lambda date, db_type: uploads.append((date, db_type)) or True

        backend.begin_batch()
        backend.begin_batch()
        backend._batch_dirty.add(("2026-01-01", "news"))
        backend.end_batch()
        self.assertEqual(uploads, [])
        backend.end_batch()
        self.assertEqual(uploads, [("2026-01-01", "news")])

    def test_head_object_not_found_is_false(self):
        backend = _backend()
        backend.s3_client.head_object.side_effect = _client_error("NoSuchKey", 404)
        self.assertFalse(backend._check_object_exists("news/2026-01-01.db"))

    def test_head_object_permission_error_is_raised(self):
        backend = _backend()
        backend.s3_client.head_object.side_effect = _client_error("AccessDenied", 403)
        with self.assertRaises(ClientError):
            backend._check_object_exists("news/2026-01-01.db")

    def test_head_object_throttle_is_raised(self):
        backend = _backend()
        backend.s3_client.head_object.side_effect = _client_error("SlowDown", 503)
        with self.assertRaises(ClientError):
            backend._check_object_exists("news/2026-01-01.db")


if __name__ == "__main__":
    unittest.main()
