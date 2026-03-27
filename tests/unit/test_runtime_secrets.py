import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import runtime_secrets


class TestRuntimeSecrets(unittest.TestCase):
    def test_load_runtime_secrets_reads_configured_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            secrets_file = Path(tmp) / "runtime_secrets.env"
            secrets_file.write_text(
                "SMTP_HOST=smtp.example.com\nSMTP_PASSWORD=secret\n",
                encoding="utf-8",
            )
            with patch.dict(
                os.environ,
                {"BARGAINFINDER_SECRETS_FILE": str(secrets_file)},
                clear=True,
            ):
                state = runtime_secrets.load_runtime_secrets()

                self.assertEqual(state["loaded_from"], secrets_file.resolve())
                self.assertEqual(os.getenv("SMTP_HOST"), "smtp.example.com")
                self.assertEqual(os.getenv("SMTP_PASSWORD"), "secret")

    def test_load_runtime_secrets_does_not_override_environment(self):
        with tempfile.TemporaryDirectory() as tmp:
            secrets_file = Path(tmp) / "runtime_secrets.env"
            secrets_file.write_text("SMTP_HOST=file-host\n", encoding="utf-8")
            with patch.dict(
                os.environ,
                {
                    "BARGAINFINDER_SECRETS_FILE": str(secrets_file),
                    "SMTP_HOST": "env-host",
                },
                clear=True,
            ):
                runtime_secrets.load_runtime_secrets()

                self.assertEqual(os.getenv("SMTP_HOST"), "env-host")

    def test_get_missing_email_secrets_includes_recipient(self):
        with tempfile.TemporaryDirectory() as tmp:
            missing_file = Path(tmp) / "missing.env"
            with patch.dict(
                os.environ,
                {"BARGAINFINDER_SECRETS_FILE": str(missing_file)},
                clear=True,
            ):
                with patch(
                    "runtime_secrets.get_secret_file_candidates",
                    return_value=(missing_file,),
                ):
                    missing = runtime_secrets.get_missing_email_secrets("")

                self.assertIn("REPORT_RECIPIENT", missing)
                self.assertIn("SMTP_HOST", missing)

    def test_loads_database_url_from_neon_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_file = Path(tmp) / "neon_db_url.txt"
            db_file.write_text("postgres://example", encoding="utf-8")
            missing_env_file = Path(tmp) / "missing.env"

            with patch.dict(
                os.environ,
                {"BARGAINFINDER_SECRETS_FILE": str(missing_env_file)},
                clear=True,
            ):
                with patch(
                    "runtime_secrets.get_secret_file_candidates",
                    return_value=(missing_env_file,),
                ):
                    with patch.object(
                        runtime_secrets,
                        "DATABASE_URL_FILES",
                        (db_file,),
                    ):
                        runtime_secrets.load_runtime_secrets()
                        self.assertEqual(
                            os.getenv("DATABASE_URL"),
                            "postgres://example",
                        )

    def test_loads_smtp_password_from_password_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            pw_file = Path(tmp) / "google_app_password.txt"
            pw_file.write_text("abc123", encoding="utf-8")
            missing_env_file = Path(tmp) / "missing.env"

            with patch.dict(
                os.environ,
                {"BARGAINFINDER_SECRETS_FILE": str(missing_env_file)},
                clear=True,
            ):
                with patch(
                    "runtime_secrets.get_secret_file_candidates",
                    return_value=(missing_env_file,),
                ):
                    with patch.object(
                        runtime_secrets,
                        "SMTP_PASSWORD_FILES",
                        (pw_file,),
                    ):
                        runtime_secrets.load_runtime_secrets()
                        self.assertEqual(
                            os.getenv("SMTP_PASSWORD"),
                            "abc123",
                        )
