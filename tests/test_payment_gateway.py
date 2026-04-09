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
    def test_large_amount_returns_false(self, mock_sleep):
        result = process_payment(20000.0, "4111111111111111", "123")
        assert result is False
