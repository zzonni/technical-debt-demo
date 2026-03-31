from unittest.mock import patch
from services.email import send_email


class TestSendEmail:
    @patch("services.email.time.sleep")
    def test_send_email_runs(self, mock_sleep, capsys):
        send_email("test@example.com", "Subject", "Body")
        mock_sleep.assert_called_once_with(2)
        captured = capsys.readouterr()
        assert "test@example.com" in captured.out
        assert "Subject" in captured.out
