import os
import sys
import tempfile
import types
import unittest
import db
from unittest.mock import MagicMock, patch


fake_psycopg = types.ModuleType("psycopg")
fake_psycopg.Connection = object
fake_psycopg.connect = MagicMock()
fake_psycopg_rows = types.ModuleType("psycopg.rows")
fake_psycopg_rows.dict_row = object()
sys.modules.setdefault("psycopg", fake_psycopg)
sys.modules.setdefault("psycopg.rows", fake_psycopg_rows)


class TestDbModule(unittest.TestCase):
    def test_get_database_url_prefers_argument(self):
        with patch.dict(os.environ, {"DATABASE_URL": "env-url"}, clear=True):
            self.assertEqual(db.get_database_url("arg-url"), "arg-url")

    def test_get_database_url_uses_env(self):
        with patch.dict(os.environ, {"DATABASE_URL": "env-url"}, clear=True):
            self.assertEqual(db.get_database_url(), "env-url")

    def test_get_database_url_uses_local_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            url_file = os.path.join(tmp, "neon_db_url.txt")
            with open(url_file, "w", encoding="utf-8") as file_handle:
                file_handle.write("file-url\n")

            with patch.dict(os.environ, {}, clear=True):
                self.assertEqual(
                    db._read_database_url_file(db.Path(url_file)), "file-url"
                )
                with patch("db._read_database_url_file", return_value="file-url"):
                    self.assertEqual(db.get_database_url(), "file-url")

    def test_get_database_url_raises_when_missing(self):
        with patch.dict(os.environ, {}, clear=True):
            with patch("db._read_database_url_file", return_value=""):
                with self.assertRaises(RuntimeError):
                    db.get_database_url()

    def test_database_enabled(self):
        with patch.dict(os.environ, {"DATABASE_URL": "x"}, clear=True):
            self.assertTrue(db.database_enabled())
        with patch.dict(os.environ, {}, clear=True):
            with patch("db._read_database_url_file", return_value="file-url"):
                self.assertTrue(db.database_enabled())
        with patch.dict(os.environ, {}, clear=True):
            with patch("db._read_database_url_file", return_value=""):
                self.assertFalse(db.database_enabled())

    def test_get_connection_commits_and_closes(self):
        fake_conn = MagicMock()
        with patch("db.connect", return_value=fake_conn) as connect_mock:
            with patch("db.get_database_url", return_value="url"):
                with db.get_connection():
                    pass

        connect_mock.assert_called_once()
        fake_conn.commit.assert_called_once()
        fake_conn.rollback.assert_not_called()
        fake_conn.close.assert_called_once()

    def test_get_connection_rolls_back_on_error(self):
        fake_conn = MagicMock()
        with patch("db.connect", return_value=fake_conn):
            with patch("db.get_database_url", return_value="url"):
                with self.assertRaises(ValueError):
                    with db.get_connection():
                        raise ValueError("boom")

        fake_conn.rollback.assert_called_once()
        fake_conn.close.assert_called_once()

    def test_get_connection_autocommit_skips_commit_rollback(self):
        fake_conn = MagicMock()
        with patch("db.connect", return_value=fake_conn):
            with patch("db.get_database_url", return_value="url"):
                with db.get_connection(autocommit=True):
                    pass

        fake_conn.commit.assert_not_called()
        fake_conn.rollback.assert_not_called()
        fake_conn.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
