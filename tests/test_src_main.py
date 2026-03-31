import pytest
from unittest.mock import patch, MagicMock
from src.main import calculate_discount, format_domestic_address, format_international_address, process_checkout


class TestCalculateDiscount:
    def test_vip_gets_15_percent_off(self):
        assert calculate_discount(100, True) == 85.0

    def test_non_vip_gets_no_discount(self):
        assert calculate_discount(100, False) == 100.0

    def test_returns_float(self):
        result = calculate_discount(50, False)
        assert isinstance(result, float)


class TestFormatDomesticAddress:
    def test_formats_correctly(self):
        info = {"street": "123 Main", "city": "Springfield", "state": "IL", "zip": "62701"}
        result = format_domestic_address(info)
        assert result == "123 MAIN, SPRINGFIELD, IL 62701"


class TestFormatInternationalAddress:
    def test_formats_correctly(self):
        info = {"street": "456 Oak", "city": "London", "state": "UK", "zip": "SW1A"}
        result = format_international_address(info)
        assert result == "456 OAK, LONDON, UK SW1A"


class TestProcessCheckout:
    @patch("src.main.process_payment", return_value=True)
    @patch("src.main.get_connection")
    def test_empty_cart_returns_error(self, mock_conn, mock_pay):
        result = process_checkout(1, [], "4111", "123")
        assert result["status"] == "error"
        assert "Cart empty" in result["msg"]

    @patch("src.main.process_payment", return_value=True)
    @patch("src.main.get_connection")
    def test_invalid_user_returns_error(self, mock_conn, mock_pay):
        result = process_checkout(999, [("item", 10, 0)], "4111", "123")
        assert result["status"] == "error"
        assert "User not found" in result["msg"]

    @patch("src.main.process_payment", return_value=True)
    @patch("src.main.get_connection")
    def test_clearance_item_gets_discount(self, mock_conn, mock_pay):
        mock_cursor = MagicMock()
        mock_conn.return_value.cursor.return_value = mock_cursor
        # item tuple: (name, price, category_code) — 99 = clearance
        process_checkout(1, [("widget", 100, 99)], "4111", "123")
        mock_pay.assert_called_once()
        # VIP discount: 100 * 0.85 = 85.0
        assert mock_pay.call_args[0][0] == 85.0
