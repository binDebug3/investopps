import logging
import os
import smtplib
import ssl
from email.message import EmailMessage
from email.utils import make_msgid
from typing import Tuple

from runtime_secrets import load_runtime_secrets


LOGGER = logging.getLogger("bargain_finder.email")


def _env_flag(name: str, default: bool = True) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _smtp_settings() -> dict:
    load_runtime_secrets()

    host = os.getenv("SMTP_HOST")
    username = os.getenv("SMTP_USERNAME")
    password = os.getenv("SMTP_PASSWORD")
    sender = os.getenv("SMTP_FROM") or username

    if not host:
        raise RuntimeError("SMTP_HOST is not set.")
    if not username:
        raise RuntimeError("SMTP_USERNAME is not set.")
    if not password:
        raise RuntimeError("SMTP_PASSWORD is not set.")
    if not sender:
        raise RuntimeError("SMTP_FROM or SMTP_USERNAME must be set.")

    return {
        "host": host,
        "port": int(os.getenv("SMTP_PORT", "587")),
        "username": username,
        "password": password,
        "sender": sender,
        "use_tls": _env_flag("SMTP_USE_TLS", default=True),
    }


def _deliver_message(message: EmailMessage) -> Tuple[bool, str]:
    try:
        settings = _smtp_settings()
        message_id = make_msgid()
        message["Message-Id"] = message_id
        with smtplib.SMTP(settings["host"], settings["port"], timeout=30) as client:
            if settings["use_tls"]:
                client.starttls(context=ssl.create_default_context())
            if settings["username"] and settings["password"]:
                client.login(settings["username"], settings["password"])
            client.send_message(message)
        return True, message_id
    except Exception:
        LOGGER.exception("Error sending email")
        return False, None


def send_email(subject: str, message_text: str, recipient: str) -> Tuple[bool, str]:
    load_runtime_secrets()
    if not recipient:
        raise RuntimeError("REPORT_RECIPIENT is not set.")

    message = EmailMessage()
    message["To"] = recipient
    message["From"] = _smtp_settings()["sender"]
    message["Subject"] = subject
    message.set_content(message_text)
    return _deliver_message(message)


def send_table(subject: str, html_content: str, recipient: str) -> Tuple[bool, str]:
    load_runtime_secrets()
    if not recipient:
        raise RuntimeError("REPORT_RECIPIENT is not set.")

    message = EmailMessage()
    message["To"] = recipient
    message["From"] = _smtp_settings()["sender"]
    message["Subject"] = subject
    message.set_content("Open this message in an HTML-capable email client.")
    message.add_alternative(html_content, subtype="html")
    return _deliver_message(message)


def send_aws_email(subject: str, message_text: str, recipient: str) -> Tuple[bool, str]:
    return send_email(subject, message_text, recipient)


def send_aws_table(subject: str, html_content: str, recipient: str) -> Tuple[bool, str]:
    return send_table(subject, html_content, recipient)
