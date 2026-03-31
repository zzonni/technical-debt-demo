import pytest
from unittest.mock import patch, MagicMock
import report_generator


class TestGenerateSalesReport:
    def test_basic_report(self):
        report = report_generator.generate_sales_report(
            "2024-01-01", "2024-12-31", "US", "json", include_tax=False
        )
        assert report["type"] == "sales"
        assert report["region"] == "US"
        assert len(report["items"]) == 10
        assert report["summary"]["total_tax"] == 0
        assert report["summary"]["total_items_sold"] > 0

    def test_report_with_tax(self):
        report = report_generator.generate_sales_report(
            "2024-01-01", "2024-12-31", "US", "json", include_tax=True
        )
        assert report["summary"]["total_tax"] > 0
        for item in report["items"]:
            assert "tax" in item

    def test_report_without_tax(self):
        report = report_generator.generate_sales_report(
            "2024-01-01", "2024-12-31", "EU", "csv", include_tax=False
        )
        for item in report["items"]:
            assert "tax" not in item

    def test_average_order_value(self):
        report = report_generator.generate_sales_report(
            "2024-01-01", "2024-12-31", "US", "json", include_tax=False
        )
        expected_avg = report["summary"]["total_revenue"] / 10
        assert report["summary"]["average_order_value"] == round(expected_avg, 2)


class TestGenerateInventoryReport:
    def test_basic_report(self):
        report = report_generator.generate_inventory_report(
            "2024-01-01", "2024-12-31", "WH-1", "json", include_tax=False
        )
        assert report["type"] == "inventory"
        assert report["warehouse"] == "WH-1"
        assert len(report["items"]) == 10

    def test_report_with_tax(self):
        report = report_generator.generate_inventory_report(
            "2024-01-01", "2024-12-31", "WH-1", "json", include_tax=True
        )
        assert report["summary"]["total_tax"] > 0


class TestGenerateCustomerReport:
    def test_basic_report(self):
        report = report_generator.generate_customer_report(
            "2024-01-01", "2024-12-31", "premium", "json", include_tax=False
        )
        assert report["type"] == "customer"
        assert report["segment"] == "premium"
        assert len(report["items"]) == 10

    def test_report_with_tax(self):
        report = report_generator.generate_customer_report(
            "2024-01-01", "2024-12-31", "all", "json", include_tax=True
        )
        assert report["summary"]["total_tax"] > 0


class TestSaveReportAsJson:
    def test_save_json(self, tmp_path):
        report = {"type": "test", "items": [{"a": 1}]}
        out = tmp_path / "report.json"
        result = report_generator.save_report_as_json(report, str(out))
        assert result == str(out)
        import json
        saved = json.loads(out.read_text())
        assert saved["type"] == "test"


class TestSaveReportAsCsv:
    def test_save_csv(self, tmp_path):
        report = {"items": [{"id": 1, "name": "A"}, {"id": 2, "name": "B"}]}
        out = tmp_path / "report.csv"
        result = report_generator.save_report_as_csv(report, str(out))
        assert result == str(out)
        content = out.read_text()
        assert "id,name" in content
        assert "1,A" in content

    def test_save_csv_empty_items(self, tmp_path):
        report = {"items": []}
        out = tmp_path / "report.csv"
        report_generator.save_report_as_csv(report, str(out))
        assert out.read_text() == ""


class TestFormatReportSummary:
    def test_format(self):
        report = {
            "type": "sales",
            "generated_at": "2024-01-01T00:00:00",
            "summary": {
                "total_revenue": 1000.0,
                "total_tax": 80.0,
                "total_items_sold": 50,
                "average_order_value": 200.0,
            },
        }
        result = report_generator.format_report_summary(report)
        assert "sales" in result
        assert "$1,000.00" in result
        assert "$80.00" in result
        assert "50" in result

    def test_format_empty_summary(self):
        report = {"type": "unknown"}
        result = report_generator.format_report_summary(report)
        assert "unknown" in result
        assert "$0.00" in result


class TestFormatReportSummaryHtml:
    def test_html_format(self):
        report = {
            "type": "sales",
            "generated_at": "2024-01-01",
            "summary": {
                "total_revenue": 500.0,
                "total_tax": 40.0,
                "total_items_sold": 10,
                "average_order_value": 50.0,
            },
        }
        result = report_generator.format_report_summary_html(report)
        assert "<h2>" in result
        assert "<p>" in result
        assert "sales" in result


class TestCompareReports:
    def test_identical_reports(self):
        report = report_generator.generate_sales_report(
            "2024-01-01", "2024-12-31", "US", "json", False
        )
        result = report_generator.compare_reports(
            report, report, "summary", tolerance=0, include_details=False,
            format_output="json", highlight_diffs=False, ignore_fields=[],
            output_path=None, verbose=False,
        )
        assert result["mismatches"] == 0
        assert result["matches"] > 0

    def test_different_reports(self):
        report_a = {
            "summary": {"total_revenue": 1000, "total_tax": 80},
            "items": [],
        }
        report_b = {
            "summary": {"total_revenue": 2000, "total_tax": 80},
            "items": [],
        }
        result = report_generator.compare_reports(
            report_a, report_b, "summary", tolerance=0, include_details=False,
            format_output="json", highlight_diffs=False, ignore_fields=[],
            output_path=None, verbose=False,
        )
        assert result["mismatches"] >= 1

    def test_compare_with_tolerance(self):
        report_a = {
            "summary": {"total_revenue": 1000},
            "items": [],
        }
        report_b = {
            "summary": {"total_revenue": 1005},
            "items": [],
        }
        result = report_generator.compare_reports(
            report_a, report_b, "summary", tolerance=1, include_details=False,
            format_output="json", highlight_diffs=False, ignore_fields=[],
            output_path=None, verbose=False,
        )
        assert result["matches"] == 1

    def test_compare_non_numeric_mismatch(self):
        report_a = {
            "summary": {"region": "US"},
            "items": [],
        }
        report_b = {
            "summary": {"region": "EU"},
            "items": [],
        }
        result = report_generator.compare_reports(
            report_a, report_b, "summary", tolerance=0, include_details=False,
            format_output="json", highlight_diffs=False, ignore_fields=[],
            output_path=None, verbose=False,
        )
        assert result["mismatches"] == 1

    def test_compare_with_ignore_fields(self):
        report_a = {
            "summary": {"total_revenue": 1000, "generated_at": "2024-01-01"},
            "items": [],
        }
        report_b = {
            "summary": {"total_revenue": 1000, "generated_at": "2024-02-01"},
            "items": [],
        }
        result = report_generator.compare_reports(
            report_a, report_b, "summary", tolerance=0, include_details=False,
            format_output="json", highlight_diffs=False, ignore_fields=["generated_at"],
            output_path=None, verbose=False,
        )
        assert result["skipped"] == 1
        assert result["mismatches"] == 0

    def test_compare_with_include_details(self):
        report_a = {
            "summary": {"total_revenue": 1000},
            "items": [{"id": 1, "name": "A"}],
        }
        report_b = {
            "summary": {"total_revenue": 1000},
            "items": [{"id": 1, "name": "B"}],
        }
        result = report_generator.compare_reports(
            report_a, report_b, "summary", tolerance=0, include_details=True,
            format_output="json", highlight_diffs=False, ignore_fields=[],
            output_path=None, verbose=False,
        )
        assert len(result["item_diffs"]) >= 1

    def test_compare_items_only_in_a(self):
        report_a = {
            "summary": {},
            "items": [{"id": 1}],
        }
        report_b = {
            "summary": {},
            "items": [],
        }
        result = report_generator.compare_reports(
            report_a, report_b, "summary", tolerance=0, include_details=True,
            format_output="json", highlight_diffs=False, ignore_fields=[],
            output_path=None, verbose=False,
        )
        assert any(d.get("type") == "only_in_a" for d in result["item_diffs"])

    def test_compare_items_only_in_b(self):
        report_a = {
            "summary": {},
            "items": [],
        }
        report_b = {
            "summary": {},
            "items": [{"id": 1}],
        }
        result = report_generator.compare_reports(
            report_a, report_b, "summary", tolerance=0, include_details=True,
            format_output="json", highlight_diffs=False, ignore_fields=[],
            output_path=None, verbose=False,
        )
        assert any(d.get("type") == "only_in_b" for d in result["item_diffs"])

    def test_compare_with_output_path(self, tmp_path):
        report_a = {"summary": {"total": 1}, "items": []}
        report_b = {"summary": {"total": 2}, "items": []}
        out = str(tmp_path / "diff.json")
        result = report_generator.compare_reports(
            report_a, report_b, "summary", tolerance=0, include_details=False,
            format_output="json", highlight_diffs=False, ignore_fields=[],
            output_path=out, verbose=False,
        )
        import json
        with open(out) as f:
            saved = json.load(f)
        assert saved["mismatches"] >= 1

    def test_numeric_over_tolerance(self):
        report_a = {"summary": {"revenue": 100}, "items": []}
        report_b = {"summary": {"revenue": 200}, "items": []}
        result = report_generator.compare_reports(
            report_a, report_b, "summary", tolerance=10, include_details=False,
            format_output="json", highlight_diffs=False, ignore_fields=[],
            output_path=None, verbose=False,
        )
        assert result["mismatches"] == 1
        assert result["summary_diffs"][0]["diff_pct"] > 10


class TestAggregateReportsV1:
    def test_average_aggregation(self):
        reports = [
            {"region": "US", "summary": {"total_revenue": 1000, "total_items_sold": 10}},
            {"region": "US", "summary": {"total_revenue": 2000, "total_items_sold": 20}},
            {"region": "EU", "summary": {"total_revenue": 500, "total_items_sold": 5}},
        ]
        result = report_generator.aggregate_reports(
            reports, "region", "average", None, True, "json", 2,
            False, None, "plain"
        )
        assert "US" in result["groups"]
        assert "EU" in result["groups"]
        assert result["total_revenue"] == 3500
        assert result["groups"]["US"]["avg_revenue"] == 1500.0

    def test_sum_aggregation(self):
        reports = [
            {"region": "US", "summary": {"total_revenue": 1000, "total_items_sold": 10}},
        ]
        result = report_generator.aggregate_reports(
            reports, "region", "sum", None, True, "json", 2,
            False, None, "plain"
        )
        assert result["groups"]["US"]["avg_revenue"] == 1000

    def test_max_aggregation(self):
        reports = [
            {"region": "US", "summary": {"total_revenue": 1000, "total_items_sold": 10}},
        ]
        result = report_generator.aggregate_reports(
            reports, "region", "max", None, True, "json", 2,
            False, None, "plain"
        )
        assert result["groups"]["US"]["avg_revenue"] == 1000

    def test_unknown_aggregation(self):
        reports = [
            {"region": "US", "summary": {"total_revenue": 1000, "total_items_sold": 10}},
        ]
        result = report_generator.aggregate_reports(
            reports, "region", "custom", None, True, "json", 2,
            False, None, "plain"
        )
        assert "US" in result["groups"]

    def test_normalize(self):
        reports = [
            {"region": "US", "summary": {"total_revenue": 750, "total_items_sold": 10}},
            {"region": "EU", "summary": {"total_revenue": 250, "total_items_sold": 5}},
        ]
        result = report_generator.aggregate_reports(
            reports, "region", "average", None, True, "json", 2,
            True, None, "plain"
        )
        assert result["groups"]["US"]["revenue_pct"] == 75.0
        assert result["groups"]["EU"]["revenue_pct"] == 25.0


class TestComputeReportTrendsV1:
    def _make_reports(self, values):
        return [
            {"summary": {"revenue": v}, "generated_at": f"2024-0{i+1}-01"}
            for i, v in enumerate(values)
        ]

    def test_insufficient_data(self):
        result = report_generator.compute_report_trends(
            self._make_reports([100]), "revenue", 3, "moving_average",
            min_data_points=2, confidence_level=0.95, output_format="json",
            include_raw_data=False, smoothing_factor=0.3, annotations=[]
        )
        assert "error" in result

    def test_moving_average(self):
        result = report_generator.compute_report_trends(
            self._make_reports([100, 200, 300]), "revenue", 2, "moving_average",
            min_data_points=2, confidence_level=0.95, output_format="json",
            include_raw_data=False, smoothing_factor=0.3, annotations=[]
        )
        assert result["direction"] == "up"
        assert result["data_points"] == 3

    def test_exponential_trend(self):
        result = report_generator.compute_report_trends(
            self._make_reports([100, 200, 300]), "revenue", 2, "exponential",
            min_data_points=2, confidence_level=0.95, output_format="json",
            include_raw_data=False, smoothing_factor=0.5, annotations=[]
        )
        assert result["direction"] == "up"

    def test_linear_trend(self):
        result = report_generator.compute_report_trends(
            self._make_reports([100, 200, 300]), "revenue", 2, "linear",
            min_data_points=2, confidence_level=0.95, output_format="json",
            include_raw_data=False, smoothing_factor=0.3, annotations=[]
        )
        assert result["direction"] == "up"

    def test_unknown_trend_type(self):
        result = report_generator.compute_report_trends(
            self._make_reports([100, 200, 300]), "revenue", 2, "unknown",
            min_data_points=2, confidence_level=0.95, output_format="json",
            include_raw_data=False, smoothing_factor=0.3, annotations=[]
        )
        assert len(result["smoothed_values"]) == 3

    def test_downward_trend(self):
        result = report_generator.compute_report_trends(
            self._make_reports([300, 200, 100]), "revenue", 2, "moving_average",
            min_data_points=2, confidence_level=0.95, output_format="json",
            include_raw_data=False, smoothing_factor=0.3, annotations=[]
        )
        assert result["direction"] == "down"

    def test_flat_trend(self):
        result = report_generator.compute_report_trends(
            self._make_reports([100, 100]), "revenue", 2, "moving_average",
            min_data_points=2, confidence_level=0.95, output_format="json",
            include_raw_data=False, smoothing_factor=0.3, annotations=[]
        )
        assert result["direction"] == "flat"

    def test_tolerance(self):
        report_a = {"summary": {"total_revenue": 100}, "items": []}
        report_b = {"summary": {"total_revenue": 105}, "items": []}
        result = report_generator.compare_reports(
            report_a, report_b, "summary", tolerance=10, include_details=False,
            format_output="json", highlight_diffs=False, ignore_fields=[],
            output_path=None, verbose=False,
        )
        assert result["matches"] == 1
        assert result["mismatches"] == 0

    def test_ignore_fields(self):
        report_a = {"summary": {"total_revenue": 100, "total_tax": 10}, "items": []}
        report_b = {"summary": {"total_revenue": 200, "total_tax": 10}, "items": []}
        result = report_generator.compare_reports(
            report_a, report_b, "summary", tolerance=0, include_details=False,
            format_output="json", highlight_diffs=False,
            ignore_fields=["total_revenue"], output_path=None, verbose=False,
        )
        assert result["skipped"] == 1
        assert result["mismatches"] == 0

    def test_include_details(self):
        report_a = {"summary": {}, "items": [{"id": 1, "val": 10}]}
        report_b = {"summary": {}, "items": [{"id": 1, "val": 20}]}
        result = report_generator.compare_reports(
            report_a, report_b, "summary", tolerance=0, include_details=True,
            format_output="json", highlight_diffs=False, ignore_fields=[],
            output_path=None, verbose=False,
        )
        assert len(result["item_diffs"]) >= 1

    def test_save_to_file(self, tmp_path):
        report_a = {"summary": {"x": 1}, "items": []}
        report_b = {"summary": {"x": 2}, "items": []}
        out = tmp_path / "diff.json"
        report_generator.compare_reports(
            report_a, report_b, "summary", tolerance=0, include_details=False,
            format_output="json", highlight_diffs=False, ignore_fields=[],
            output_path=str(out), verbose=False,
        )
        assert out.exists()


class TestAggregateReports:
    def test_basic_aggregation(self):
        reports = [
            {"type": "sales", "summary": {"total_revenue": 100, "total_items_sold": 10}},
            {"type": "sales", "summary": {"total_revenue": 200, "total_items_sold": 20}},
        ]
        result = report_generator.aggregate_reports(
            reports, "type", "sum", filters=None, include_empty=True,
            output_format="json", decimal_places=2, normalize=False,
            weight_field=None, label_format=None,
        )
        assert result["total_revenue"] == 300
        assert result["total_items"] == 30
        assert result["total_reports"] == 2

    def test_average_aggregation(self):
        reports = [
            {"type": "sales", "summary": {"total_revenue": 100, "total_items_sold": 10}},
            {"type": "sales", "summary": {"total_revenue": 200, "total_items_sold": 20}},
        ]
        result = report_generator.aggregate_reports(
            reports, "type", "average", filters=None, include_empty=True,
            output_format="json", decimal_places=2, normalize=False,
            weight_field=None, label_format=None,
        )
        assert result["groups"]["sales"]["avg_revenue"] == 150.0

    def test_normalize(self):
        reports = [
            {"type": "A", "summary": {"total_revenue": 100, "total_items_sold": 10}},
            {"type": "B", "summary": {"total_revenue": 100, "total_items_sold": 10}},
        ]
        result = report_generator.aggregate_reports(
            reports, "type", "sum", filters=None, include_empty=True,
            output_format="json", decimal_places=2, normalize=True,
            weight_field=None, label_format=None,
        )
        assert result["groups"]["A"]["revenue_pct"] == 50.0


class TestComputeReportTrends:
    def _make_reports(self, values):
        return [
            {"generated_at": f"2024-0{i+1}-01", "summary": {"total_revenue": v}}
            for i, v in enumerate(values)
        ]

    def test_insufficient_data(self):
        reports = self._make_reports([100])
        result = report_generator.compute_report_trends(
            reports, "total_revenue", window_size=3, trend_type="moving_average",
            min_data_points=3, confidence_level=0.95, output_format="json",
            include_raw_data=False, smoothing_factor=0.3, annotations=[],
        )
        assert "error" in result

    def test_moving_average(self):
        reports = self._make_reports([100, 200, 300, 400])
        result = report_generator.compute_report_trends(
            reports, "total_revenue", window_size=2, trend_type="moving_average",
            min_data_points=2, confidence_level=0.95, output_format="json",
            include_raw_data=True, smoothing_factor=0.3, annotations=[],
        )
        assert result["direction"] == "up"
        assert result["data_points"] == 4
        assert len(result["smoothed_values"]) == 4
        assert result["raw_values"] == [100, 200, 300, 400]

    def test_exponential_trend(self):
        reports = self._make_reports([100, 200, 300])
        result = report_generator.compute_report_trends(
            reports, "total_revenue", window_size=2, trend_type="exponential",
            min_data_points=2, confidence_level=0.95, output_format="json",
            include_raw_data=False, smoothing_factor=0.5, annotations=[],
        )
        assert result["direction"] == "up"
        assert len(result["smoothed_values"]) == 3

    def test_linear_trend(self):
        reports = self._make_reports([100, 200, 300])
        result = report_generator.compute_report_trends(
            reports, "total_revenue", window_size=2, trend_type="linear",
            min_data_points=2, confidence_level=0.95, output_format="json",
            include_raw_data=False, smoothing_factor=0.3, annotations=[],
        )
        assert result["direction"] == "up"

    def test_flat_trend(self):
        reports = self._make_reports([100, 100, 100])
        result = report_generator.compute_report_trends(
            reports, "total_revenue", window_size=2, trend_type="moving_average",
            min_data_points=2, confidence_level=0.95, output_format="json",
            include_raw_data=False, smoothing_factor=0.3, annotations=[],
        )
        assert result["direction"] == "flat"
        assert result["overall_change"] == 0

    def test_downward_trend(self):
        reports = self._make_reports([300, 200, 100])
        result = report_generator.compute_report_trends(
            reports, "total_revenue", window_size=2, trend_type="moving_average",
            min_data_points=2, confidence_level=0.95, output_format="json",
            include_raw_data=False, smoothing_factor=0.3, annotations=[],
        )
        assert result["direction"] == "down"
