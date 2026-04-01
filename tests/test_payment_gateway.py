from unittest.mock import patch
from src.payment_gateway import process_payment


class TestProcessPayment:
    @patch("src.payment_gateway.time.sleep")
    def test_successful_payment(self, mock_sleep):
        result = process_payment(50.0, "4111111111111111", "123")
        assert result is True

    @patch("src.payment_gateway.time.sleep")
    def test_string_amount_coerced(self, mock_sleep):
        result = process_payment("50.0", "4111111111111111", "123")
        assert result is True

    @patch("src.payment_gateway.time.sleep")
    def test_large_amount_returns_failure(self, mock_sleep):
        result = process_payment(20000.0, "4111111111111111", "123")
        assert result is False

    @patch("src.payment_gateway.time.sleep")
    @patch("builtins.print")
    def test_payment_masks_card_number_in_logs(self, mock_print, mock_sleep):
        process_payment(10.0, "4111111111111111", "123")
        logged = mock_print.call_args[0][0]
        assert "4111111111111111" not in logged
        assert "1111" in logged
