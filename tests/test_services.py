from unittest.mock import patch
import logging
from services.email import send_email


class TestSendEmail:
    @patch("services.email.time.sleep")
    def test_send_email_runs(self, mock_sleep, caplog):
        caplog.set_level(logging.INFO)
        send_email("test@example.com", "Subject", "Body")
        mock_sleep.assert_called_once_with(2)
        assert "test@example.com" in caplog.text
        assert "Subject" in caplog.text
