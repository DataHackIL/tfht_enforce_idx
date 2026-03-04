"""SMTP email output for unified items."""

import smtplib
from datetime import UTC, datetime
from email.message import EmailMessage

from denbust.data_models import UnifiedItem
from denbust.output.formatter import format_items


def default_subject(item_count: int) -> str:
    """Build a default email subject."""
    date_str = datetime.now(UTC).strftime("%Y-%m-%d")
    return f"denbust report {date_str} ({item_count} items)"


def send_email_report(
    *,
    items: list[UnifiedItem],
    smtp_host: str,
    smtp_port: int,
    sender: str,
    recipients: list[str],
    subject: str | None = None,
    username: str | None = None,
    password: str | None = None,
    use_tls: bool = True,
) -> None:
    """Send unified items as a plain-text email report."""
    if not recipients:
        raise ValueError("At least one recipient email is required")

    report_body = format_items(items)
    message = EmailMessage()
    message["Subject"] = subject or default_subject(len(items))
    message["From"] = sender
    message["To"] = ", ".join(recipients)
    message.set_content(report_body)

    with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as smtp:
        smtp.ehlo()
        if use_tls:
            smtp.starttls()
            smtp.ehlo()
        if username and password:
            smtp.login(username, password)
        smtp.send_message(message)
