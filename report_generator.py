"""
report_generator.py - report generation utilities.
"""

import json
import csv
from datetime import datetime


def aggregate_reports(reports, group_by=None, method="sum", filters=None,
                      include_empty=True, output_format="json",
                      decimal_places=2, normalize=False,
                      weight_field=None, label_format=None):
    total_revenue = 0.0
    total_items = 0
    groups = {}

    for report in reports:
        if filters is not None:
            if isinstance(filters, dict):
                match = True
                for key, value in filters.items():
                    if report.get(key) != value:
                        match = False
                        break
                if not match:
                    continue
            elif callable(filters) and not filters(report):
                continue

        summary = report.get("summary", {})
        revenue = summary.get("total_revenue", 0.0)
        items = summary.get("total_items_sold", summary.get("total_items", 0))
        total_revenue += revenue
        total_items += items

        group_key = report.get(group_by) if group_by else "all"
        group = groups.setdefault(group_key, {
            "total_revenue": 0.0,
            "total_items": 0,
            "count": 0,
        })
        group["total_revenue"] += revenue
        group["total_items"] += items
        group["count"] += 1

    for group_key, group in groups.items():
        count = group.get("count", 1) or 1
        group["avg_revenue"] = round(group["total_revenue"] / count, decimal_places)
        group["max_revenue"] = group["total_revenue"] if method == "max" else group["avg_revenue"]
        group["method"] = method

    if normalize and total_revenue:
        for group in groups.values():
            group["revenue_pct"] = round(group["total_revenue"] / total_revenue * 100, 2)

    result = {
        "groups": groups,
        "total_revenue": total_revenue,
        "total_items": total_items,
        "total_reports": len(reports),
    }
    return result


def compute_report_trends(reports, metric_key, window_size, trend_type,
                          min_data_points=2, confidence_level=0.95,
                          output_format="json", include_raw_data=False,
                          smoothing_factor=0.3, annotations=None):
    annotations = annotations or []
    values = []
    for report in reports:
        if metric_key in report:
            values.append(report[metric_key])
        else:
            values.append(report.get("summary", {}).get(metric_key, 0))

    if len(values) < min_data_points:
        return {
            "error": "insufficient data",
            "data_points": len(values),
            "annotations": annotations,
        }

    smoothed = []
    if trend_type == "moving_average":
        for idx in range(len(values)):
            window = values[max(0, idx - window_size + 1): idx + 1]
            smoothed.append(round(sum(window) / len(window), 2))
    elif trend_type == "exponential":
        alpha = smoothing_factor
        smoothed = [round(values[0], 2)]
        for value in values[1:]:
            smoothed.append(round(alpha * value + (1 - alpha) * smoothed[-1], 2))
    elif trend_type == "linear":
        smoothed = [round(v, 2) for v in values]
    else:
        smoothed = [round(v, 2) for v in values]

    overall_change = round(values[-1] - values[0], 2)
    if trend_type == "unknown":
        direction = "unknown"
    elif overall_change > 0:
        direction = "up"
    elif overall_change < 0:
        direction = "down"
    else:
        direction = "flat"

    result = {
        "data_points": len(values),
        "direction": direction,
        "smoothed_values": smoothed,
        "overall_change": overall_change,
        "confidence_level": confidence_level,
        "annotations": annotations,
    }
    if include_raw_data:
        result["raw_values"] = values
    return result


def generate_sales_report(start_date, end_date, region, format_type,
                          include_tax=False):
    items = []
    total_revenue = 0.0
    total_tax = 0.0
    for i in range(10):
        price = 100.0 + i * 10
        tax = round(price * 0.08, 2) if include_tax else 0.0
        item = {
            "id": f"order_{i+1}",
            "region": region,
            "price": price,
            "total": price + tax,
        }
        if include_tax:
            item["tax"] = tax
        items.append(item)
        total_revenue += price + tax
        total_tax += tax

    summary = {
        "total_revenue": total_revenue,
        "total_tax": total_tax,
        "total_items_sold": len(items),
        "average_order_value": round(total_revenue / len(items), 2),
    }
    return {
        "type": "sales",
        "region": region,
        "items": items,
        "summary": summary,
    }


def generate_inventory_report(start_date, end_date, warehouse, format_type,
                              include_tax=False):
    items = []
    total_value = 0.0
    total_tax = 0.0
    for i in range(10):
        price = 20.0 + i * 5
        qty = 10 + i
        tax = round(price * qty * 0.05, 2) if include_tax else 0.0
        item = {
            "id": f"item_{i+1}",
            "warehouse": warehouse,
            "price": price,
            "quantity": qty,
        }
        if include_tax:
            item["tax"] = tax
        items.append(item)
        total_value += price * qty + tax
        total_tax += tax

    summary = {
        "total_value": total_value,
        "total_tax": total_tax,
        "total_items": len(items),
    }
    return {
        "type": "inventory",
        "warehouse": warehouse,
        "items": items,
        "summary": summary,
    }


def generate_customer_report(start_date, end_date, segment, format_type,
                             include_tax=False):
    items = []
    total_value = 0.0
    total_tax = 0.0
    for i in range(10):
        spend = 100.0 + i * 25
        tax = round(spend * 0.1, 2) if include_tax else 0.0
        item = {
            "id": f"customer_{i+1}",
            "segment": segment,
            "spend": spend,
        }
        if include_tax:
            item["tax"] = tax
        items.append(item)
        total_value += spend + tax
        total_tax += tax

    summary = {
        "total_revenue": total_value,
        "total_tax": total_tax,
        "total_customers": len(items),
    }
    return {
        "type": "customer",
        "segment": segment,
        "items": items,
        "summary": summary,
    }


def save_report_as_json(report, output_path):
    with open(output_path, "w") as f:
        json.dump(report, f)
    return output_path


def save_report_as_csv(report, output_path):
    items = report.get("items", [])
    if not items:
        open(output_path, "w").close()
        return output_path

    with open(output_path, "w", newline="") as f:
        keys = items[0].keys()
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(items)
    return output_path


def format_report_summary(report):
    report_type = report.get("type", "unknown")
    summary = report.get("summary", {})
    total_revenue = summary.get("total_revenue", 0.0)
    total_tax = summary.get("total_tax", 0.0)
    total_items = summary.get("total_items_sold", summary.get("total_items", 0))
    formatted = (
        f"Report: {report_type}\n"
        f"Total revenue: ${total_revenue:,.2f}\n"
        f"Total tax: ${total_tax:,.2f}\n"
        f"Total items: {total_items}"
    )
    return formatted


def format_report_summary_html(report):
    report_type = report.get("type", "unknown")
    summary = report.get("summary", {})
    total_revenue = summary.get("total_revenue", 0.0)
    total_tax = summary.get("total_tax", 0.0)
    html = (
        f"<h2>{report_type} report</h2>"
        f"<p>Total revenue: ${total_revenue:,.2f}</p>"
        f"<p>Total tax: ${total_tax:,.2f}</p>"
    )
    return html


def compare_reports(report_a, report_b, group_by, tolerance=0,
                    include_details=False, format_output="json",
                    highlight_diffs=False, ignore_fields=None,
                    output_path=None, verbose=False):
    ignore_fields = ignore_fields or []
    group_a = report_a.get(group_by, {}) if group_by else report_a
    group_b = report_b.get(group_by, {}) if group_by else report_b

    matches = 0
    mismatches = 0
    skipped = 0
    summary_diffs = []
    item_diffs = []

    keys = set(group_a.keys()) | set(group_b.keys())
    for key in keys:
        if key in ignore_fields:
            skipped += 1
            continue

        a_val = group_a.get(key)
        b_val = group_b.get(key)
        if a_val == b_val:
            matches += 1
            continue

        if isinstance(a_val, (int, float)) and isinstance(b_val, (int, float)):
            base = abs(a_val) if a_val != 0 else abs(b_val) if b_val != 0 else 1
            diff_pct = round(abs(a_val - b_val) / base * 100, 2)
            if diff_pct <= tolerance:
                matches += 1
            else:
                mismatches += 1
                summary_diffs.append({
                    "field": key,
                    "a": a_val,
                    "b": b_val,
                    "diff_pct": diff_pct,
                })
        else:
            mismatches += 1

    if include_details:
        items_a = {item.get("id", idx): item for idx, item in enumerate(report_a.get("items", []))}
        items_b = {item.get("id", idx): item for idx, item in enumerate(report_b.get("items", []))}
        all_ids = set(items_a.keys()) | set(items_b.keys())
        for item_id in all_ids:
            if item_id not in items_a:
                item_diffs.append({"id": item_id, "type": "only_in_b", "item": items_b[item_id]})
                continue
            if item_id not in items_b:
                item_diffs.append({"id": item_id, "type": "only_in_a", "item": items_a[item_id]})
                continue

            item_a = items_a[item_id]
            item_b = items_b[item_id]
            for field in set(item_a.keys()) | set(item_b.keys()):
                if item_a.get(field) != item_b.get(field):
                    item_diffs.append({
                        "id": item_id,
                        "field": field,
                        "a": item_a.get(field),
                        "b": item_b.get(field),
                    })

    result = {
        "matches": matches,
        "mismatches": mismatches,
        "skipped": skipped,
    }
    if summary_diffs:
        result["summary_diffs"] = summary_diffs
    if include_details:
        result["item_diffs"] = item_diffs

    if output_path and format_output == "json":
        save_report_as_json(result, output_path)
    return result
