import os
import unittest
from email.message import EmailMessage
from unittest.mock import MagicMock, patch

import send_email


class TestSendEmailModule(unittest.TestCase):
    def test_env_flag_parses_values(self):
        with patch.dict(os.environ, {"X": "yes"}, clear=True):
            self.assertTrue(send_email._env_flag("X", default=False))
        with patch.dict(os.environ, {"X": "off"}, clear=True):
            self.assertFalse(send_email._env_flag("X", default=True))
        with patch.dict(os.environ, {}, clear=True):
            self.assertTrue(send_email._env_flag("X", default=True))

    def test_smtp_settings_requires_host(self):
        with patch("send_email.load_runtime_secrets", return_value={}):
            with patch.dict(os.environ, {}, clear=True):
                with self.assertRaises(RuntimeError):
                    send_email._smtp_settings()

    def test_smtp_settings_requires_sender_or_username(self):
        with patch("send_email.load_runtime_secrets", return_value={}):
            with patch.dict(os.environ, {"SMTP_HOST": "smtp.example.com"}, clear=True):
                with self.assertRaises(RuntimeError):
                    send_email._smtp_settings()

    def test_smtp_settings_defaults(self):
        with patch("send_email.load_runtime_secrets", return_value={}):
            with patch.dict(
                os.environ,
                {
                    "SMTP_HOST": "smtp.example.com",
                    "SMTP_USERNAME": "user@example.com",
                    "SMTP_PASSWORD": "secret",
                },
                clear=True,
            ):
                settings = send_email._smtp_settings()
        self.assertEqual(settings["port"], 587)
        self.assertTrue(settings["use_tls"])
        self.assertEqual(settings["sender"], "user@example.com")

    def test_smtp_settings_requires_username_and_password(self):
        with patch("send_email.load_runtime_secrets", return_value={}):
            with patch.dict(
                os.environ,
                {"SMTP_HOST": "smtp.example.com", "SMTP_USERNAME": "user@example.com"},
                clear=True,
            ):
                with self.assertRaises(RuntimeError):
                    send_email._smtp_settings()

    def test_deliver_message_success(self):
        message = EmailMessage()
        message["Subject"] = "Hi"
        message["To"] = "to@example.com"
        message.set_content("hello")

        smtp_client = MagicMock()
        smtp_ctx = MagicMock()
        smtp_ctx.__enter__.return_value = smtp_client
        smtp_ctx.__exit__.return_value = None

        with patch(
            "send_email._smtp_settings",
            return_value={
                "host": "smtp.example.com",
                "port": 587,
                "username": "u",
                "password": "p",
                "sender": "u@example.com",
                "use_tls": True,
            },
        ):
            with patch("send_email.smtplib.SMTP", return_value=smtp_ctx):
                ok, message_id = send_email._deliver_message(message)

        self.assertTrue(ok)
        self.assertIsNotNone(message_id)
        smtp_client.starttls.assert_called_once()
        smtp_client.login.assert_called_once_with("u", "p")
        smtp_client.send_message.assert_called_once()

    def test_deliver_message_failure(self):
        message = EmailMessage()
        message["Subject"] = "Hi"
        message["To"] = "to@example.com"
        message.set_content("hello")

        with patch(
            "send_email._smtp_settings",
            return_value={
                "host": "smtp.example.com",
                "port": 587,
                "username": None,
                "password": None,
                "sender": "sender@example.com",
                "use_tls": False,
            },
        ):
            with patch(
                "send_email.smtplib.SMTP", side_effect=RuntimeError("smtp down")
            ):
                ok, message_id = send_email._deliver_message(message)

        self.assertFalse(ok)
        self.assertIsNone(message_id)

    def test_send_email_and_send_table_delegate_delivery(self):
        with patch(
            "send_email._smtp_settings", return_value={"sender": "from@example.com"}
        ):
            with patch(
                "send_email._deliver_message", return_value=(True, "id-1")
            ) as deliver:
                ok1, _ = send_email.send_email("s", "body", "to@example.com")
                ok2, _ = send_email.send_table("s", "<b>x</b>", "to@example.com")

        self.assertTrue(ok1)
        self.assertTrue(ok2)
        self.assertEqual(deliver.call_count, 2)

    def test_send_email_requires_recipient(self):
        with self.assertRaises(RuntimeError):
            send_email.send_email("s", "body", "")

    def test_aws_aliases(self):
        with patch("send_email.send_email", return_value=(True, "m1")) as send_plain:
            self.assertEqual(
                send_email.send_aws_email("s", "b", "to@example.com"),
                (True, "m1"),
            )
            send_plain.assert_called_once()

        with patch("send_email.send_table", return_value=(True, "m2")) as send_html:
            self.assertEqual(
                send_email.send_aws_table("s", "<b>x</b>", "to@example.com"),
                (True, "m2"),
            )
            send_html.assert_called_once()


if __name__ == "__main__":
    unittest.main()
