import pytest
from unittest.mock import patch, MagicMock
import inventory_manager


class TestProcessStockAdjustment:
    def _make_product_row(self, product_id="p1", quantity=100, price=10.0):
        return (product_id, "Widget", "SKU1", "cat", price, quantity,
                "wh1", "sup1", 1.0, "10x10", "desc", "tag", 10, "2024-01-01")

    @patch("inventory_manager.get_db")
    def test_dry_run(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = self._make_product_row()
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = inventory_manager.process_stock_adjustment(
            [{"product_id": "p1", "quantity_change": -10, "type": "sale"}],
            reason="test", performed_by="admin", dry_run=True,
            validate_stock=True, log_changes=False, notify_warehouse=False,
            batch_id="b1", priority="normal", notes="",
        )
        assert result["processed"] == 1
        assert result["skipped"] == 0

    @patch("inventory_manager.get_db")
    def test_insufficient_stock_sale(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = self._make_product_row(quantity=5)
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = inventory_manager.process_stock_adjustment(
            [{"product_id": "p1", "quantity_change": -10, "type": "sale"}],
            reason="test", performed_by="admin", dry_run=False,
            validate_stock=True, log_changes=False, notify_warehouse=False,
            batch_id="b1", priority="normal", notes="",
        )
        assert result["skipped"] == 1
        assert len(result["errors"]) == 1

    @patch("inventory_manager.get_db")
    def test_product_not_found(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = inventory_manager.process_stock_adjustment(
            [{"product_id": "missing", "quantity_change": 10, "type": "manual"}],
            reason="test", performed_by="admin", dry_run=False,
            validate_stock=True, log_changes=False, notify_warehouse=False,
            batch_id="b1", priority="normal", notes="",
        )
        assert result["skipped"] == 1

    @patch("inventory_manager.get_db")
    def test_low_stock_alert_critical(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = self._make_product_row(quantity=5)
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = inventory_manager.process_stock_adjustment(
            [{"product_id": "p1", "quantity_change": -5, "type": "return"}],
            reason="test", performed_by="admin", dry_run=False,
            validate_stock=True, log_changes=False, notify_warehouse=False,
            batch_id="b1", priority="normal", notes="",
        )
        assert len(result["low_stock_alerts"]) == 1
        assert result["low_stock_alerts"][0]["level"] == "critical"

    @patch("inventory_manager.get_db")
    def test_low_stock_alert_warning(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = self._make_product_row(quantity=8)
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = inventory_manager.process_stock_adjustment(
            [{"product_id": "p1", "quantity_change": -5, "type": "return"}],
            reason="test", performed_by="admin", dry_run=False,
            validate_stock=True, log_changes=True, notify_warehouse=False,
            batch_id="b1", priority="normal", notes="",
        )
        assert result["low_stock_alerts"][0]["level"] == "warning"
        assert len(result["audit_entries"]) == 1

    @patch("inventory_manager.get_db")
    def test_low_stock_alert_info(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = self._make_product_row(quantity=16)
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = inventory_manager.process_stock_adjustment(
            [{"product_id": "p1", "quantity_change": -10, "type": "return"}],
            reason="test", performed_by="admin", dry_run=False,
            validate_stock=True, log_changes=False, notify_warehouse=False,
            batch_id="b1", priority="normal", notes="",
        )
        assert result["low_stock_alerts"][0]["level"] == "info"

    @patch("inventory_manager.get_db")
    def test_unknown_adjustment_type(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = self._make_product_row(quantity=5)
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = inventory_manager.process_stock_adjustment(
            [{"product_id": "p1", "quantity_change": -10, "type": "unknown"}],
            reason="test", performed_by="admin", dry_run=False,
            validate_stock=True, log_changes=False, notify_warehouse=False,
            batch_id="b1", priority="normal", notes="",
        )
        assert result["skipped"] == 1


class TestAddProduct:
    @patch("inventory_manager.get_db")
    def test_add(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_get_db.return_value.cursor.return_value = mock_cursor
        pid = inventory_manager.add_product(
            "Widget", "SKU1", "toys", 9.99, 100, "wh1",
            "sup1", 0.5, "5x5x5", "A widget", "toy,fun", 10,
        )
        assert isinstance(pid, str)
        assert len(pid) == 10
        mock_cursor.execute.assert_called_once()
        mock_get_db.return_value.commit.assert_called_once()


class TestUpdateProduct:
    @patch("inventory_manager.get_db")
    def test_update(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_get_db.return_value.cursor.return_value = mock_cursor
        inventory_manager.update_product(
            "p1", "Widget2", "SKU2", "tools", 19.99, 50, "wh2",
            "sup2", 1.0, "10x10", "Updated", "tool", 5,
        )
        mock_cursor.execute.assert_called_once()
        mock_get_db.return_value.commit.assert_called_once()


class TestSearchProductsAdvanced:
    @patch("inventory_manager.get_db")
    def test_search_with_all_filters(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [("p1", "Widget")]
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = inventory_manager.search_products_advanced(
            keyword="widget", category="toys", min_price=5, max_price=50,
            in_stock_only=True, warehouse_id="wh1", supplier_id="sup1",
            sort_by="price", sort_order="ASC", limit=10, offset=0,
        )
        assert len(result) == 1

    @patch("inventory_manager.get_db")
    def test_search_no_filters(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = inventory_manager.search_products_advanced(
            keyword=None, category=None, min_price=None, max_price=None,
            in_stock_only=False, warehouse_id=None, supplier_id=None,
            sort_by=None, sort_order="ASC", limit=100, offset=0,
        )
        assert result == []


class TestGenerateReorderList:
    @patch("inventory_manager.get_db")
    def test_reorder_list(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ("p1", "Widget", "SKU1", "toys", 10.0, 3, "wh1", "sup1",
             0.5, "5x5", "desc", "tag", 20, "2024-01-01"),
        ]
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = inventory_manager.generate_reorder_list(
            warehouse_id="wh1", category_filter=None, min_priority=1,
            include_discontinued=False, supplier_filter=None,
            max_items=100, format_type="json", dry_run=False,
            auto_approve=True, notification_list=[],
        )
        assert result["total_items"] == 1
        assert result["items"][0]["reorder_qty"] == int((20 - 3) * 2.5)
        assert result["auto_approved"] is True

    @patch("inventory_manager.get_db")
    def test_reorder_empty(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = inventory_manager.generate_reorder_list(
            warehouse_id="wh1", category_filter="electronics", min_priority=1,
            include_discontinued=True, supplier_filter="sup1",
            max_items=10, format_type="json", dry_run=True,
            auto_approve=False, notification_list=[],
        )
        assert result["total_items"] == 0
        assert result["total_cost"] == 0


class TestGenerateInventoryValuation:
    @patch("inventory_manager.get_db")
    def test_valuation(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ("p1", "Widget", "SKU1", "toys", 10.0, 50, "wh1"),
        ]
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = inventory_manager.generate_inventory_valuation(
            warehouse_id="wh1", category=None, valuation_method="fifo",
            include_zero_stock=False, group_by="category", output_format="json",
            currency="USD", exchange_rate=1.0, tax_rate=0.0, notes="test",
        )
        assert result["total_value"] == 500.0
        assert result["total_units"] == 50

    @patch("inventory_manager.get_db")
    def test_valuation_with_tax(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ("p1", "Widget", "SKU1", "toys", 10.0, 100, "wh1"),
        ]
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = inventory_manager.generate_inventory_valuation(
            warehouse_id="wh1", category="toys", valuation_method="lifo",
            include_zero_stock=True, group_by="category", output_format="json",
            currency="USD", exchange_rate=1.0, tax_rate=0.1, notes="",
        )
        assert result["total_value"] == round(1000 * 1.1, 2)


class TestReconcileInventory:
    def _make_product_row(self, product_id="p1", quantity=100, price=10.0):
        return (product_id, "Widget", "SKU1", "cat", price, quantity)

    @patch("inventory_manager.get_db")
    def test_matched(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = self._make_product_row(quantity=50)
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = inventory_manager.reconcile_inventory(
            warehouse_id="wh1",
            physical_counts=[{"product_id": "p1", "quantity": 50}],
            auto_adjust=False, tolerance_percent=5, log_discrepancies=True,
            notify_manager=False, batch_id="b1", auditor="admin",
            notes="", strict_mode=False,
        )
        assert result["matched"] == 1
        assert result["adjusted"] == 0

    @patch("inventory_manager.get_db")
    def test_product_not_found(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = inventory_manager.reconcile_inventory(
            warehouse_id="wh1",
            physical_counts=[{"product_id": "missing", "quantity": 10}],
            auto_adjust=False, tolerance_percent=5, log_discrepancies=True,
            notify_manager=False, batch_id="b1", auditor="admin",
            notes="", strict_mode=False,
        )
        assert result["flagged"] == 1
        assert result["discrepancies"][0]["type"] == "not_found"

    @patch("inventory_manager.get_db")
    def test_auto_adjust_within_tolerance(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = self._make_product_row(quantity=100)
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = inventory_manager.reconcile_inventory(
            warehouse_id="wh1",
            physical_counts=[{"product_id": "p1", "quantity": 102}],
            auto_adjust=True, tolerance_percent=5, log_discrepancies=True,
            notify_manager=False, batch_id="b1", auditor="admin",
            notes="", strict_mode=False,
        )
        assert result["adjusted"] == 1
        assert result["discrepancies"][0]["action"] == "auto_adjusted"

    @patch("inventory_manager.get_db")
    def test_flagged_strict_outside_tolerance(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = self._make_product_row(quantity=100)
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = inventory_manager.reconcile_inventory(
            warehouse_id="wh1",
            physical_counts=[{"product_id": "p1", "quantity": 50}],
            auto_adjust=True, tolerance_percent=5, log_discrepancies=True,
            notify_manager=False, batch_id="b1", auditor="admin",
            notes="", strict_mode=True,
        )
        assert result["flagged"] == 1
        assert result["discrepancies"][0]["action"] == "flagged_for_review"


class TestCalculateWarehouseCapacity:
    @patch("inventory_manager.get_db")
    def test_capacity(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ("p1", "A", "S1", "cat", 10.0, 1000, "wh1", "s1", 2.0),
        ]
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = inventory_manager.calculate_warehouse_capacity(
            warehouse_id="wh1", include_reserved=False, include_incoming=False,
            unit_type="units", buffer_pct=10, alert_threshold=50,
            forecast_days=30, growth_rate=1, detail_level="summary", notes="",
        )
        assert result["current_units"] == 1000
        assert result["max_capacity"] == 100000
        assert result["used_pct"] == 1.0
        assert result["capacity_alert"] is False

    @patch("inventory_manager.get_db")
    def test_capacity_alert(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ("p1", "A", "S1", "cat", 10.0, 90000, "wh1", "s1", 2.0),
        ]
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = inventory_manager.calculate_warehouse_capacity(
            warehouse_id="wh1", include_reserved=False, include_incoming=False,
            unit_type="units", buffer_pct=10, alert_threshold=50,
            forecast_days=30, growth_rate=1, detail_level="summary", notes="",
        )
        assert result["capacity_alert"] is True


class TestSyncInventoryWithSupplier:
    @patch("inventory_manager.get_db")
    def test_sync_supplier_wins(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (
            "p1", "Widget", "SKU1", "toys", 10.0, 50,
            "wh1", "sup1", 1.0, "5x5", "desc", "tag", 10, "2024-01-01"
        )
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = inventory_manager.sync_inventory_with_supplier(
            supplier_id="sup1", product_ids=["p1"], sync_prices=True,
            sync_quantities=True, sync_descriptions=False,
            conflict_resolution="supplier_wins", dry_run=False,
            log_changes=True, batch_id="b1", timeout=30,
        )
        assert result["synced"] == 1
        assert len(result["changes"]) >= 1

    @patch("inventory_manager.get_db")
    def test_sync_local_wins(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (
            "p1", "Widget", "SKU1", "toys", 10.0, 50,
            "wh1", "sup1", 1.0, "5x5", "desc", "tag", 10, "2024-01-01"
        )
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = inventory_manager.sync_inventory_with_supplier(
            supplier_id="sup1", product_ids=["p1"], sync_prices=True,
            sync_quantities=True, sync_descriptions=False,
            conflict_resolution="local_wins", dry_run=False,
            log_changes=True, batch_id="b1", timeout=30,
        )
        assert result["conflicts"] == 2
        assert result["synced"] == 0

    @patch("inventory_manager.get_db")
    def test_sync_product_not_found(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = inventory_manager.sync_inventory_with_supplier(
            supplier_id="sup1", product_ids=["missing"], sync_prices=True,
            sync_quantities=False, sync_descriptions=False,
            conflict_resolution="supplier_wins", dry_run=False,
            log_changes=False, batch_id="b1", timeout=30,
        )
        assert len(result["errors"]) == 1


class TestExportInventoryReport:
    @patch("inventory_manager.get_db")
    def test_export_json(self, mock_get_db, tmp_path):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ("p1", "Widget", "SKU1", "toys", 10.0, 50),
        ]
        mock_get_db.return_value.cursor.return_value = mock_cursor
        out = tmp_path / "report.json"
        result = inventory_manager.export_inventory_report(
            warehouse_id="wh1", categories=["toys"], date_range_start="2024-01-01",
            date_range_end="2024-12-31", include_zero_stock=False,
            format_type="json", output_path=str(out), group_by="category",
            sort_by="name", include_valuation=True,
        )
        assert result["total_records"] == 1
        assert out.exists()

    @patch("inventory_manager.get_db")
    def test_export_csv(self, mock_get_db, tmp_path):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ("p1", "Widget", "SKU1", "toys", 10.0, 50),
        ]
        mock_get_db.return_value.cursor.return_value = mock_cursor
        out = tmp_path / "report.csv"
        result = inventory_manager.export_inventory_report(
            warehouse_id=None, categories=None, date_range_start="2024-01-01",
            date_range_end="2024-12-31", include_zero_stock=True,
            format_type="csv", output_path=str(out), group_by=None,
            sort_by=None, include_valuation=True,
        )
        assert result["total_records"] == 1
        content = out.read_text()
        assert "valuation" in content


class TestGetDb:
    @patch("inventory_manager.sqlite3.connect")
    def test_returns_connection(self, mock_connect):
        mock_connect.return_value = MagicMock()
        conn = inventory_manager.get_db()
        mock_connect.assert_called_once_with("ecommerce.db")
        assert conn is not None


class TestStockAdjustmentNegativeWarning:
    def _make_product_row(self, product_id="p1", quantity=5, price=10.0):
        return (product_id, "Widget", "SKU1", "cat", price, quantity,
                "wh1", "sup1", 1.0, "10x10", "desc", "tag", 10, "2024-01-01")

    @patch("inventory_manager.get_db")
    def test_negative_stock_adjustment_type(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = self._make_product_row(quantity=5)
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = inventory_manager.process_stock_adjustment(
            [{"product_id": "p1", "quantity_change": -10, "type": "adjustment"}],
            reason="test", performed_by="admin", dry_run=False,
            validate_stock=True, log_changes=False, notify_warehouse=False,
            batch_id="b1", priority="normal", notes="",
        )
        assert result["processed"] == 1
        assert len(result["warnings"]) == 1
        assert "Negative stock" in result["warnings"][0]


class TestValuationMethods:
    @patch("inventory_manager.get_db")
    def test_weighted_avg(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ("p1", "Widget", "SKU1", "toys", 10.0, 50, "wh1"),
        ]
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = inventory_manager.generate_inventory_valuation(
            warehouse_id="wh1", category=None, valuation_method="weighted_avg",
            include_zero_stock=False, group_by="category", output_format="json",
            currency="USD", exchange_rate=1.0, tax_rate=0.0, notes="",
        )
        assert result["total_value"] == 500.0

    @patch("inventory_manager.get_db")
    def test_unknown_method(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ("p1", "Widget", "SKU1", "toys", 10.0, 50, "wh1"),
        ]
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = inventory_manager.generate_inventory_valuation(
            warehouse_id="wh1", category=None, valuation_method="other",
            include_zero_stock=False, group_by="category", output_format="json",
            currency="USD", exchange_rate=1.0, tax_rate=0.0, notes="",
        )
        assert result["total_value"] == 500.0

    @patch("inventory_manager.get_db")
    def test_group_by_warehouse(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ("p1", "Widget", "SKU1", "toys", 10.0, 50, "wh1"),
        ]
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = inventory_manager.generate_inventory_valuation(
            warehouse_id=None, category=None, valuation_method="fifo",
            include_zero_stock=False, group_by="warehouse", output_format="json",
            currency="USD", exchange_rate=1.0, tax_rate=0.0, notes="",
        )
        assert "wh1" in result["groups"]


class TestReconcileForceAdjust:
    def _make_product_row(self, product_id="p1", quantity=100, price=10.0):
        return (product_id, "Widget", "SKU1", "cat", price, quantity)

    @patch("inventory_manager.get_db")
    def test_force_adjust_outside_tolerance(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = self._make_product_row(quantity=100)
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = inventory_manager.reconcile_inventory(
            warehouse_id="wh1",
            physical_counts=[{"product_id": "p1", "quantity": 50}],
            auto_adjust=True, tolerance_percent=5, log_discrepancies=True,
            notify_manager=False, batch_id="b1", auditor="admin",
            notes="", strict_mode=False,
        )
        assert result["adjusted"] == 1
        assert result["discrepancies"][0]["action"] == "force_adjusted"

    @patch("inventory_manager.get_db")
    def test_flagged_non_strict_no_auto_adjust(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = self._make_product_row(quantity=100)
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = inventory_manager.reconcile_inventory(
            warehouse_id="wh1",
            physical_counts=[{"product_id": "p1", "quantity": 50}],
            auto_adjust=False, tolerance_percent=5, log_discrepancies=True,
            notify_manager=False, batch_id="b1", auditor="admin",
            notes="", strict_mode=False,
        )
        assert result["flagged"] == 1
        assert result["discrepancies"][0]["action"] == "flagged"

    @patch("inventory_manager.get_db")
    def test_within_tolerance_no_auto(self, mock_get_db):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = self._make_product_row(quantity=100)
        mock_get_db.return_value.cursor.return_value = mock_cursor
        result = inventory_manager.reconcile_inventory(
            warehouse_id="wh1",
            physical_counts=[{"product_id": "p1", "quantity": 102}],
            auto_adjust=False, tolerance_percent=5, log_discrepancies=True,
            notify_manager=False, batch_id="b1", auditor="admin",
            notes="", strict_mode=False,
        )
        assert result["discrepancies"][0]["action"] == "within_tolerance"

    @patch("inventory_manager.get_db")
    def test_export_csv(self, mock_get_db, tmp_path):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ("p1", "Widget", "SKU1", "toys", 10.0, 50),
        ]
        mock_get_db.return_value.cursor.return_value = mock_cursor
        out = tmp_path / "report.csv"
        result = inventory_manager.export_inventory_report(
            warehouse_id=None, categories=None, date_range_start="2024-01-01",
            date_range_end="2024-12-31", include_zero_stock=True,
            format_type="csv", output_path=str(out), group_by=None,
            sort_by=None, include_valuation=False,
        )
        assert result["total_records"] == 1
        content = out.read_text()
        assert "id,name,sku" in content
