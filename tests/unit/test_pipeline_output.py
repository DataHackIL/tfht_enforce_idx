"""Unit tests for pipeline output dispatch."""

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from pydantic import HttpUrl

from denbust.config import Config, OutputConfig, OutputFormat
from denbust.data_models import Category, SourceReference, SubCategory, UnifiedItem
from denbust.pipeline import output_items, send_output_email


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


class TestSendOutputEmail:
    """Tests for send_output_email helper."""

    @patch("denbust.pipeline.send_email_report")
    @patch.dict(
        "os.environ",
        {
            "DENBUST_EMAIL_SMTP_HOST": "smtp.example.com",
            "DENBUST_EMAIL_SMTP_PORT": "2525",
            "DENBUST_EMAIL_SMTP_USERNAME": "user",
            "DENBUST_EMAIL_SMTP_PASSWORD": "pass",
            "DENBUST_EMAIL_FROM": "alerts@example.com",
            "DENBUST_EMAIL_TO": "ops@example.com,admin@example.com",
            "DENBUST_EMAIL_USE_TLS": "false",
            "DENBUST_EMAIL_SUBJECT": "Custom Subject",
        },
        clear=True,
    )
    def test_send_output_email_uses_config_env(self, mock_send: MagicMock) -> None:
        """Should read SMTP settings from env-backed config properties."""
        config = Config(output=OutputConfig(format=OutputFormat.EMAIL))
        items = [build_item()]

        send_output_email(items, config)

        mock_send.assert_called_once_with(
            items=items,
            smtp_host="smtp.example.com",
            smtp_port=2525,
            sender="alerts@example.com",
            recipients=["ops@example.com", "admin@example.com"],
            subject="Custom Subject",
            username="user",
            password="pass",
            use_tls=False,
        )


class TestOutputItems:
    """Tests for output_items function."""

    @patch("denbust.pipeline.send_output_email")
    @patch("denbust.pipeline.print_items")
    def test_email_output_path(self, mock_print: MagicMock, mock_send: MagicMock) -> None:
        """Should send email and not print CLI output when successful."""
        config = Config(output=OutputConfig(format=OutputFormat.EMAIL))

        output_items([build_item()], config)

        mock_send.assert_called_once()
        mock_print.assert_not_called()

    @patch("denbust.pipeline.send_output_email")
    @patch("denbust.pipeline.print_items")
    def test_cli_and_email_output_path(
        self, mock_print: MagicMock, mock_send: MagicMock
    ) -> None:
        """Should emit both CLI and email output when both are configured."""
        config = Config(output=OutputConfig(formats=[OutputFormat.CLI, OutputFormat.EMAIL]))

        output_items([build_item()], config)

        mock_send.assert_called_once()
        mock_print.assert_called_once()

    @patch("denbust.pipeline.send_output_email", side_effect=RuntimeError("smtp down"))
    @patch("denbust.pipeline.print_items")
    def test_email_error_falls_back_to_cli(
        self, mock_print: MagicMock, mock_send: MagicMock
    ) -> None:
        """Should fall back to CLI output if sending email fails."""
        config = Config(output=OutputConfig(format=OutputFormat.EMAIL))

        output_items([build_item()], config)

        mock_send.assert_called_once()
        mock_print.assert_called_once()

    @patch("denbust.pipeline.send_output_email", side_effect=RuntimeError("smtp down"))
    @patch("denbust.pipeline.print_items")
    def test_cli_and_email_error_does_not_double_print(
        self, mock_print: MagicMock, mock_send: MagicMock
    ) -> None:
        """Should not print twice when CLI is already configured and email fails."""
        config = Config(output=OutputConfig(formats=[OutputFormat.CLI, OutputFormat.EMAIL]))

        output_items([build_item()], config)

        mock_send.assert_called_once()
        mock_print.assert_called_once()
