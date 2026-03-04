"""Unit tests for email output module."""

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from pydantic import HttpUrl

from denbust.data_models import Category, SourceReference, SubCategory, UnifiedItem
from denbust.output.email import send_email_report


def build_item() -> UnifiedItem:
    """Create a sample unified item for output tests."""
    return UnifiedItem(
        headline="Test headline",
        summary="Test summary",
        sources=[
            SourceReference(
                source_name="ynet",
                url=HttpUrl("https://ynet.co.il/article/1"),
            )
        ],
        date=datetime(2026, 2, 15, tzinfo=UTC),
        category=Category.BROTHEL,
        sub_category=SubCategory.CLOSURE,
    )


class TestSendEmailReport:
    """Tests for send_email_report."""

    @patch("denbust.output.email.smtplib.SMTP")
    def test_send_email_with_tls_and_login(self, mock_smtp_cls: MagicMock) -> None:
        """Should send message via SMTP with STARTTLS and auth."""
        smtp = mock_smtp_cls.return_value.__enter__.return_value

        send_email_report(
            items=[build_item()],
            smtp_host="smtp.example.com",
            smtp_port=587,
            sender="alerts@example.com",
            recipients=["ops@example.com"],
            username="smtp-user",
            password="smtp-pass",
            use_tls=True,
        )

        mock_smtp_cls.assert_called_once_with("smtp.example.com", 587, timeout=30)
        smtp.starttls.assert_called_once()
        smtp.login.assert_called_once_with("smtp-user", "smtp-pass")
        smtp.send_message.assert_called_once()

        message = smtp.send_message.call_args.args[0]
        assert message["From"] == "alerts@example.com"
        assert message["To"] == "ops@example.com"
        assert "denbust report" in message["Subject"]
        assert "Test headline" in message.get_content()

    @patch("denbust.output.email.smtplib.SMTP")
    def test_send_email_without_tls_or_login(self, mock_smtp_cls: MagicMock) -> None:
        """Should skip STARTTLS and login when disabled/no credentials."""
        smtp = mock_smtp_cls.return_value.__enter__.return_value

        send_email_report(
            items=[build_item()],
            smtp_host="smtp.example.com",
            smtp_port=2525,
            sender="alerts@example.com",
            recipients=["ops@example.com"],
            use_tls=False,
        )

        smtp.starttls.assert_not_called()
        smtp.login.assert_not_called()
        smtp.send_message.assert_called_once()

    def test_send_email_requires_recipient(self) -> None:
        """Should fail fast when no recipients are provided."""
        with pytest.raises(ValueError, match="At least one recipient email is required"):
            send_email_report(
                items=[build_item()],
                smtp_host="smtp.example.com",
                smtp_port=587,
                sender="alerts@example.com",
                recipients=[],
            )
