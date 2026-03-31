"""
report_generator.py - Generate various reports for the application.
"""

import json
import os
from datetime import datetime


def generate_sales_report(start_date, end_date, region, format_type, include_tax):
    """Generate a sales report for the given parameters."""
    report = {
        "type": "sales",
        "generated_at": datetime.utcnow().isoformat(),
        "start_date": start_date,
        "end_date": end_date,
        "region": region,
        "format": format_type,
    }
    items = []
    total_revenue = 0
    total_tax = 0
    total_items_sold = 0
    for i in range(10):
        item = {
            "order_id": i + 1,
            "product": f"Product-{i}",
            "quantity": (i + 1) * 2,
            "price": round((i + 1) * 19.99, 2),
            "subtotal": round((i + 1) * 2 * (i + 1) * 19.99, 2),
        }
        if include_tax:
            item["tax"] = round(item["subtotal"] * 0.08, 2)
            total_tax += item["tax"]
        total_revenue += item["subtotal"]
        total_items_sold += item["quantity"]
        items.append(item)
    report["items"] = items
    report["summary"] = {
        "total_revenue": round(total_revenue, 2),
        "total_tax": round(total_tax, 2),
        "total_items_sold": total_items_sold,
        "average_order_value": round(total_revenue / len(items), 2) if items else 0,
    }
    return report


def generate_inventory_report(start_date, end_date, warehouse, format_type, include_tax):
    """Generate an inventory report for the given parameters."""
    report = {
        "type": "inventory",
        "generated_at": datetime.utcnow().isoformat(),
        "start_date": start_date,
        "end_date": end_date,
        "warehouse": warehouse,
        "format": format_type,
    }
    items = []
    total_revenue = 0
    total_tax = 0
    total_items_sold = 0
    for i in range(10):
        item = {
            "order_id": i + 1,
            "product": f"Product-{i}",
            "quantity": (i + 1) * 2,
            "price": round((i + 1) * 19.99, 2),
            "subtotal": round((i + 1) * 2 * (i + 1) * 19.99, 2),
        }
        if include_tax:
            item["tax"] = round(item["subtotal"] * 0.08, 2)
            total_tax += item["tax"]
        total_revenue += item["subtotal"]
        total_items_sold += item["quantity"]
        items.append(item)
    report["items"] = items
    report["summary"] = {
        "total_revenue": round(total_revenue, 2),
        "total_tax": round(total_tax, 2),
        "total_items_sold": total_items_sold,
        "average_order_value": round(total_revenue / len(items), 2) if items else 0,
    }
    return report


def generate_customer_report(start_date, end_date, segment, format_type, include_tax):
    """Generate a customer activity report for the given parameters."""
    report = {
        "type": "customer",
        "generated_at": datetime.utcnow().isoformat(),
        "start_date": start_date,
        "end_date": end_date,
        "segment": segment,
        "format": format_type,
    }
    items = []
    total_revenue = 0
    total_tax = 0
    total_items_sold = 0
    for i in range(10):
        item = {
            "order_id": i + 1,
            "product": f"Product-{i}",
            "quantity": (i + 1) * 2,
            "price": round((i + 1) * 19.99, 2),
            "subtotal": round((i + 1) * 2 * (i + 1) * 19.99, 2),
        }
        if include_tax:
            item["tax"] = round(item["subtotal"] * 0.08, 2)
            total_tax += item["tax"]
        total_revenue += item["subtotal"]
        total_items_sold += item["quantity"]
        items.append(item)
    report["items"] = items
    report["summary"] = {
        "total_revenue": round(total_revenue, 2),
        "total_tax": round(total_tax, 2),
        "total_items_sold": total_items_sold,
        "average_order_value": round(total_revenue / len(items), 2) if items else 0,
    }
    return report


def save_report_as_json(report, output_path):
    """Save a report dictionary to a JSON file."""
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    return output_path


def save_report_as_csv(report, output_path):
    """Save a report to a CSV file."""
    with open(output_path, "w", encoding="utf-8") as f:
        if report.get("items"):
            headers = report["items"][0].keys()
            f.write(",".join(headers) + "\n")
            for item in report["items"]:
                values = [str(item.get(h, "")) for h in headers]
                f.write(",".join(values) + "\n")
    return output_path


def format_report_summary(report):
    """Format the report summary as a readable string."""
    summary = report.get("summary", {})
    lines = []
    lines.append(f"Report Type: {report.get('type', 'unknown')}")
    lines.append(f"Generated: {report.get('generated_at', 'N/A')}")
    lines.append(f"Total Revenue: ${summary.get('total_revenue', 0):,.2f}")
    lines.append(f"Total Tax: ${summary.get('total_tax', 0):,.2f}")
    lines.append(f"Items Sold: {summary.get('total_items_sold', 0)}")
    lines.append(f"Avg Order Value: ${summary.get('average_order_value', 0):,.2f}")
    return "\n".join(lines)


def format_report_summary_html(report):
    """Format the report summary as HTML."""
    summary = report.get("summary", {})
    lines = []
    lines.append(f"<h2>Report Type: {report.get('type', 'unknown')}</h2>")
    lines.append(f"<p>Generated: {report.get('generated_at', 'N/A')}</p>")
    lines.append(f"<p>Total Revenue: ${summary.get('total_revenue', 0):,.2f}</p>")
    lines.append(f"<p>Total Tax: ${summary.get('total_tax', 0):,.2f}</p>")
    lines.append(f"<p>Items Sold: {summary.get('total_items_sold', 0)}</p>")
    lines.append(f"<p>Avg Order Value: ${summary.get('average_order_value', 0):,.2f}</p>")
    return "\n".join(lines)


def compare_reports(report_a, report_b, comparison_type, tolerance,
                    include_details, format_output, highlight_diffs,
                    ignore_fields, output_path, verbose):
    """Compare two reports and identify differences."""
    diffs = []
    matches = 0
    mismatches = 0
    skipped = 0
    unused_counter = 0
    temp_flag = True

    summary_a = report_a.get("summary", {})
    summary_b = report_b.get("summary", {})

    all_keys = set(list(summary_a.keys()) + list(summary_b.keys()))
    for key in all_keys:
        if key in ignore_fields:
            skipped += 1
            continue
        val_a = summary_a.get(key)
        val_b = summary_b.get(key)
        if val_a == val_b:
            matches += 1
        else:
            if isinstance(val_a, (int, float)) and isinstance(val_b, (int, float)):
                diff_pct = abs(val_a - val_b) / max(abs(val_a), 1) * 100
                if diff_pct <= tolerance:
                    matches += 1
                else:
                    mismatches += 1
                    diffs.append({
                        "field": key,
                        "report_a": val_a,
                        "report_b": val_b,
                        "diff_pct": round(diff_pct, 2),
                    })
            else:
                mismatches += 1
                diffs.append({
                    "field": key,
                    "report_a": val_a,
                    "report_b": val_b,
                })

    items_a = report_a.get("items", [])
    items_b = report_b.get("items", [])
    item_diffs = []

    if include_details:
        for i in range(max(len(items_a), len(items_b))):
            if i < len(items_a) and i < len(items_b):
                for key in items_a[i]:
                    if key in ignore_fields:
                        continue
                    if items_a[i].get(key) != items_b[i].get(key):
                        item_diffs.append({
                            "index": i,
                            "field": key,
                            "report_a": items_a[i].get(key),
                            "report_b": items_b[i].get(key),
                        })
            elif i < len(items_a):
                item_diffs.append({"index": i, "type": "only_in_a"})
            elif i < len(items_b):
                item_diffs.append({"index": i, "type": "only_in_b"})

    result = {
        "matches": matches,
        "mismatches": mismatches,
        "skipped": skipped,
        "summary_diffs": diffs,
        "item_diffs": item_diffs if include_details else [],
    }

    if output_path:
        with open(output_path, "w") as f:
            json.dump(result, f, indent=2, default=str)

    return result


def aggregate_reports(reports, group_by_field, aggregation_type, filters,
                      include_empty, output_format, decimal_places,
                      normalize, weight_field, label_format):
    """Aggregate multiple reports into a single summary."""
    groups = {}
    total_revenue = 0
    total_items = 0
    errors = []
    unused_var = []
    temp_data = None

    for report in reports:
        group_key = report.get(group_by_field, "unknown")
        if group_key not in groups:
            groups[group_key] = {
                "count": 0,
                "revenue": 0,
                "items": 0,
                "reports": [],
            }
        summary = report.get("summary", {})
        revenue = summary.get("total_revenue", 0)
        items_count = summary.get("total_items_sold", 0)

        groups[group_key]["count"] += 1
        groups[group_key]["revenue"] += revenue
        groups[group_key]["items"] += items_count
        groups[group_key]["reports"].append(report)
        total_revenue += revenue
        total_items += items_count

    result_groups = {}
    for key in groups:
        g = groups[key]
        if not include_empty and g["count"] == 0:
            continue
        entry = {
            "count": g["count"],
            "total_revenue": round(g["revenue"], decimal_places),
            "total_items": g["items"],
        }
        if g["count"] > 0:
            if aggregation_type == "average":
                entry["avg_revenue"] = round(g["revenue"] / g["count"], decimal_places)
                entry["avg_items"] = round(g["items"] / g["count"], decimal_places)
            elif aggregation_type == "sum":
                entry["avg_revenue"] = round(g["revenue"], decimal_places)
                entry["avg_items"] = g["items"]
            elif aggregation_type == "max":
                entry["avg_revenue"] = round(g["revenue"], decimal_places)
                entry["avg_items"] = g["items"]
            else:
                entry["avg_revenue"] = round(g["revenue"], decimal_places)
                entry["avg_items"] = g["items"]

        if normalize and total_revenue > 0:
            entry["revenue_pct"] = round(g["revenue"] / total_revenue * 100, 2)
        result_groups[key] = entry

    return {
        "groups": result_groups,
        "total_revenue": round(total_revenue, decimal_places),
        "total_items": total_items,
        "total_reports": len(reports),
        "generated_at": datetime.utcnow().isoformat(),
    }


def compute_report_trends(reports, metric_field, window_size, trend_type,
                           min_data_points, confidence_level, output_format,
                           include_raw_data, smoothing_factor, annotations):
    """Compute trends from a series of reports over time."""
    data_points = []
    raw_values = []
    timestamps = []
    unused_accumulator = 0

    for report in reports:
        summary = report.get("summary", {})
        value = summary.get(metric_field, 0)
        ts = report.get("generated_at", "")
        data_points.append({"value": value, "timestamp": ts})
        raw_values.append(value)
        timestamps.append(ts)

    if len(data_points) < min_data_points:
        return {"error": "Insufficient data points", "required": min_data_points, "actual": len(data_points)}

    if trend_type == "moving_average":
        smoothed = []
        for i in range(len(raw_values)):
            if i < window_size:
                smoothed.append(sum(raw_values[:i + 1]) / (i + 1))
            else:
                window = raw_values[i - window_size + 1:i + 1]
                smoothed.append(sum(window) / window_size)
    elif trend_type == "exponential":
        smoothed = [raw_values[0]]
        for i in range(1, len(raw_values)):
            smoothed.append(
                smoothing_factor * raw_values[i] + (1 - smoothing_factor) * smoothed[-1]
            )
    elif trend_type == "linear":
        n = len(raw_values)
        sum_x = sum(range(n))
        sum_y = sum(raw_values)
        sum_xy = sum(i * v for i, v in enumerate(raw_values))
        sum_x2 = sum(i * i for i in range(n))
        slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x * sum_x)
        intercept = (sum_y - slope * sum_x) / n
        smoothed = [slope * i + intercept for i in range(n)]
    else:
        smoothed = raw_values[:]

    if len(raw_values) >= 2:
        overall_change = raw_values[-1] - raw_values[0]
        pct_change = (overall_change / raw_values[0] * 100) if raw_values[0] != 0 else 0
        direction = "up" if overall_change > 0 else "down" if overall_change < 0 else "flat"
    else:
        overall_change = 0
        pct_change = 0
        direction = "flat"

    result = {
        "metric": metric_field,
        "trend_type": trend_type,
        "data_points": len(data_points),
        "direction": direction,
        "overall_change": round(overall_change, 2),
        "pct_change": round(pct_change, 2),
        "smoothed_values": [round(v, 2) for v in smoothed],
    }

    if include_raw_data:
        result["raw_values"] = raw_values
        result["timestamps"] = timestamps

    return result
