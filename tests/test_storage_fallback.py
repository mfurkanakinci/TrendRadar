"""Remote init failure must not silently fall back to local storage."""

import logging
import os
import unittest
from unittest.mock import patch

from trendradar.storage import manager as manager_mod
from trendradar.storage.manager import RemoteBackendInitError, StorageManager

REMOTE_CONFIG = {
    "bucket_name": "b",
    "access_key_id": "k",
    "secret_access_key": "s",
    "endpoint_url": "https://example.invalid",
}


class StorageFallbackTests(unittest.TestCase):
    def setUp(self):
        self._saved = os.environ.pop(manager_mod.ALLOW_LOCAL_FALLBACK_ENV, None)

    def tearDown(self):
        if self._saved is None:
            os.environ.pop(manager_mod.ALLOW_LOCAL_FALLBACK_ENV, None)
        else:
            os.environ[manager_mod.ALLOW_LOCAL_FALLBACK_ENV] = self._saved

    def _boom(self, exc):
        return patch("trendradar.storage.remote.RemoteStorageBackend", side_effect=exc)

    def test_remote_init_failure_raises_without_opt_in(self):
        with self._boom(ValueError("bad endpoint")):
            sm = StorageManager(backend_type="remote", data_dir="/tmp", remote_config=REMOTE_CONFIG)
            with self.assertLogs("trendradar.storage.manager", logging.ERROR):
                with self.assertRaises(RemoteBackendInitError) as info:
                    sm.get_backend()
        self.assertIsInstance(info.exception.__cause__, ValueError)
        self.assertIsNone(sm._backend)

    def test_remote_init_failure_falls_back_when_opted_in(self):
        os.environ[manager_mod.ALLOW_LOCAL_FALLBACK_ENV] = "true"
        with self._boom(ValueError("bad endpoint")):
            sm = StorageManager(backend_type="remote", data_dir="/tmp", remote_config=REMOTE_CONFIG)
            with self.assertLogs("trendradar.storage.manager", logging.WARNING):
                backend = sm.get_backend()
        self.assertEqual(backend.backend_name, "local")

    def test_unexpected_exception_is_not_swallowed(self):
        os.environ[manager_mod.ALLOW_LOCAL_FALLBACK_ENV] = "true"
        with self._boom(KeyError("unexpected")):
            sm = StorageManager(backend_type="remote", data_dir="/tmp", remote_config=REMOTE_CONFIG)
            with self.assertRaises(KeyError):
                sm.get_backend()

    def test_pull_from_remote_degrades_gracefully(self):
        with self._boom(ValueError("bad endpoint")):
            sm = StorageManager(
                backend_type="local",
                data_dir="/tmp",
                remote_config=REMOTE_CONFIG,
                pull_enabled=True,
                pull_days=1,
            )
            self.assertEqual(sm.pull_from_remote(), 0)


if __name__ == "__main__":
    unittest.main()
