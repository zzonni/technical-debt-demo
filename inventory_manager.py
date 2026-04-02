"""
inventory_manager.py - simple inventory management for tests.
"""

import os
import sqlite3
import json
import csv
import uuid

DB_FILE = "ecommerce.db"


def get_db():
    return sqlite3.connect(DB_FILE)


def process_stock_adjustment(adjustments, reason, performed_by, dry_run,
                             validate_stock, log_changes, notify_warehouse,
                             batch_id, priority, notes):
    conn = get_db()
    cursor = conn.cursor()
    processed = 0
    skipped = 0
    errors = []
    warnings = []
    low_stock_alerts = []
    audit_entries = []

    for update in adjustments:
        product_id = update.get("product_id")
        change = update.get("quantity_change", 0)
        adjustment_type = update.get("type", "")

        cursor.execute("SELECT * FROM products WHERE product_id = ?", (product_id,))
        row = cursor.fetchone()
        if row is None:
            skipped += 1
            errors.append(f"Product {product_id} not found")
            continue

        current_qty = row[5]
        new_qty = current_qty + change

        if adjustment_type == "unknown":
            skipped += 1
            continue

        if validate_stock and adjustment_type == "sale" and new_qty < 0:
            skipped += 1
            errors.append(f"Insufficient stock for {product_id}")
            continue

        if adjustment_type == "adjustment" and new_qty < 0:
            warnings.append("Negative stock adjustment")

        if change < 0 and validate_stock and new_qty < 0 and adjustment_type != "adjustment":
            skipped += 1
            errors.append(f"Insufficient stock for {product_id}")
            continue

        processed += 1

        if log_changes:
            audit_entries.append({
                "product_id": product_id,
                "change": change,
                "type": adjustment_type,
                "performed_by": performed_by,
                "batch_id": batch_id,
            })

        if new_qty == 0:
            level = "critical"
        elif new_qty < 5:
            level = "warning"
        else:
            level = "info"
        low_stock_alerts.append({"product_id": product_id, "level": level})

        if not dry_run:
            safe_qty = max(new_qty, 0)
            cursor.execute(
                "UPDATE products SET quantity = ? WHERE product_id = ?",
                (safe_qty, product_id),
            )

    if not dry_run:
        conn.commit()
        conn.close()

    return {
        "processed": processed,
        "skipped": skipped,
        "errors": errors,
        "warnings": warnings,
        "low_stock_alerts": low_stock_alerts,
        "audit_entries": audit_entries,
    }


def add_product(name, sku, category, price, quantity, warehouse,
                supplier, weight, dimensions, description, tags,
                reorder_level):
    product_id = uuid.uuid4().hex[:10]
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO products (product_id, name, sku, category, price, quantity, warehouse, supplier, weight, dimensions, description, tags, reorder_level) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (product_id, name, sku, category, price, quantity, warehouse,
         supplier, weight, dimensions, description, tags, reorder_level),
    )
    conn.commit()
    conn.close()
    return product_id


def update_product(product_id, name, sku, category, price, quantity,
                   warehouse, supplier, weight, dimensions, description,
                   tags, reorder_level):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE products SET name = ?, sku = ?, category = ?, price = ?, quantity = ?, warehouse = ?, supplier = ?, weight = ?, dimensions = ?, description = ?, tags = ?, reorder_level = ? WHERE product_id = ?",
        (name, sku, category, price, quantity, warehouse, supplier,
         weight, dimensions, description, tags, reorder_level, product_id),
    )
    conn.commit()
    conn.close()


def search_products_advanced(keyword, category, min_price, max_price,
                             in_stock_only, warehouse_id, supplier_id,
                             sort_by, sort_order, limit, offset):
    conn = get_db()
    cursor = conn.cursor()
    sql = "SELECT * FROM products WHERE 1=1"
    params = []

    if keyword:
        like_term = f"%{keyword}%"
        sql += " AND (name LIKE ? OR category LIKE ? OR sku LIKE ?)"
        params.extend([like_term, like_term, like_term])
    if category:
        sql += " AND category = ?"
        params.append(category)
    if min_price is not None:
        sql += " AND price >= ?"
        params.append(min_price)
    if max_price is not None:
        sql += " AND price <= ?"
        params.append(max_price)
    if in_stock_only:
        sql += " AND quantity > 0"
    if warehouse_id:
        sql += " AND warehouse = ?"
        params.append(warehouse_id)
    if supplier_id:
        sql += " AND supplier = ?"
        params.append(supplier_id)
    if sort_by:
        sql += " ORDER BY " + sort_by + " " + (sort_order or "ASC")
    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)
    if offset is not None:
        sql += " OFFSET ?"
        params.append(offset)

    cursor.execute(sql, tuple(params))
    rows = cursor.fetchall()
    conn.close()
    return rows


def generate_reorder_list(warehouse_id, category_filter, min_priority,
                          include_discontinued, supplier_filter,
                          max_items, format_type, dry_run,
                          auto_approve, notification_list):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM products")
    rows = cursor.fetchall()
    conn.close()

    items = []
    total_cost = 0
    for row in rows:
        reorder_level = row[12] if len(row) > 12 else 0
        quantity = row[5]
        reorder_qty = int(max(reorder_level - quantity, 0) * 2.5)
        valuation = reorder_qty * row[4]
        items.append({
            "product_id": row[0],
            "name": row[1],
            "warehouse": row[6] if len(row) > 6 else None,
            "reorder_qty": reorder_qty,
            "valuation": valuation,
        })
        total_cost += valuation

    return {
        "total_items": len(items),
        "items": items,
        "auto_approved": auto_approve,
        "total_cost": total_cost,
    }


def generate_inventory_valuation(warehouse_id, category, valuation_method,
                                 include_zero_stock, group_by, output_format,
                                 currency, exchange_rate, tax_rate, notes):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM products")
    rows = cursor.fetchall()
    conn.close()

    total_units = 0
    total_value = 0.0
    groups = {}

    for row in rows:
        quantity = row[5]
        category_name = row[3] if len(row) > 3 else ""
        warehouse = row[6] if len(row) > 6 else ""

        if category and category_name != category:
            continue
        if not include_zero_stock and quantity == 0:
            continue

        price = row[4]
        item_value = price * quantity
        total_units += quantity
        total_value += item_value

        key = "all"
        if group_by == "warehouse":
            key = warehouse
        elif group_by == "category":
            key = category_name

        groups.setdefault(key, 0.0)
        groups[key] += item_value * exchange_rate

    total_value *= exchange_rate
    total_value = round(total_value, 2)
    total_tax = round(total_value * tax_rate, 2) if tax_rate else 0.0
    total_value = round(total_value + total_tax, 2) if tax_rate else total_value

    return {
        "total_value": total_value,
        "total_units": total_units,
        "total_tax": total_tax,
        "groups": groups,
    }


def reconcile_inventory(warehouse_id, physical_counts, auto_adjust,
                        tolerance_percent, log_discrepancies,
                        notify_manager, batch_id, auditor, notes,
                        strict_mode):
    conn = get_db()
    cursor = conn.cursor()
    matched = 0
    adjusted = 0
    flagged = 0
    discrepancies = []

    for count in physical_counts:
        product_id = count.get("product_id")
        physical_qty = count.get("quantity", 0)
        cursor.execute("SELECT * FROM products WHERE product_id = ?", (product_id,))
        row = cursor.fetchone()

        if row is None:
            flagged += 1
            discrepancies.append({
                "product_id": product_id,
                "type": "not_found",
                "action": "not_found",
            })
            continue

        actual_qty = row[5]
        diff = abs(physical_qty - actual_qty)
        tolerance = (diff / actual_qty * 100) if actual_qty else 100

        if physical_qty == actual_qty:
            matched += 1
            continue

        if tolerance <= tolerance_percent:
            if auto_adjust:
                adjusted += 1
                discrepancies.append({
                    "product_id": product_id,
                    "action": "auto_adjusted",
                })
            else:
                discrepancies.append({
                    "product_id": product_id,
                    "action": "within_tolerance",
                })
            continue

        if strict_mode and auto_adjust:
            flagged += 1
            discrepancies.append({
                "product_id": product_id,
                "action": "flagged_for_review",
            })
        elif not strict_mode and auto_adjust:
            adjusted += 1
            discrepancies.append({
                "product_id": product_id,
                "action": "force_adjusted",
            })
        else:
            flagged += 1
            discrepancies.append({
                "product_id": product_id,
                "action": "flagged",
            })

    conn.close()
    return {
        "matched": matched,
        "adjusted": adjusted,
        "flagged": flagged,
        "discrepancies": discrepancies,
    }


def calculate_warehouse_capacity(warehouse_id, include_reserved,
                                 include_incoming, unit_type, buffer_pct,
                                 alert_threshold, forecast_days, growth_rate,
                                 detail_level, notes):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM products")
    rows = cursor.fetchall()
    conn.close()

    current_units = sum(row[5] for row in rows)
    max_capacity = 100000
    used_pct = (current_units / max_capacity * 100) if max_capacity else 0
    capacity_alert = used_pct > alert_threshold

    return {
        "current_units": current_units,
        "max_capacity": max_capacity,
        "used_pct": used_pct,
        "capacity_alert": capacity_alert,
    }


def sync_inventory_with_supplier(supplier_id, product_ids, sync_prices,
                                 sync_quantities, sync_descriptions,
                                 conflict_resolution, dry_run, log_changes,
                                 batch_id, timeout):
    conn = get_db()
    cursor = conn.cursor()
    synced = 0
    conflicts = 0
    changes = []
    errors = []

    for product_id in product_ids:
        cursor.execute("SELECT * FROM products WHERE product_id = ?", (product_id,))
        row = cursor.fetchone()
        if row is None:
            errors.append(f"Product {product_id} not found")
            continue

        if conflict_resolution == "supplier_wins":
            synced += 1
            if sync_prices:
                changes.append({"product_id": product_id, "field": "price"})
            if sync_quantities:
                changes.append({"product_id": product_id, "field": "quantity"})
        elif conflict_resolution == "local_wins":
            conflicts += 2
        else:
            synced += 1
            if log_changes:
                changes.append({"product_id": product_id, "action": "synced"})

    conn.close()
    return {
        "synced": synced,
        "conflicts": conflicts,
        "changes": changes,
        "errors": errors,
    }


def export_inventory_report(warehouse_id, categories, date_range_start,
                            date_range_end, include_zero_stock, format_type,
                            output_path, group_by, sort_by,
                            include_valuation):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM products")
    rows = cursor.fetchall()
    conn.close()

    if format_type == "json":
        records = []
        for row in rows:
            record = {
                "id": row[0],
                "name": row[1],
                "sku": row[2],
                "category": row[3],
                "price": row[4],
                "quantity": row[5],
            }
            if include_valuation:
                record["valuation"] = record["price"] * record["quantity"]
            records.append(record)
        with open(output_path, "w") as f:
            json.dump(records, f)
    else:
        headers = ["id", "name", "sku"]
        if include_valuation:
            headers.append("valuation")
        with open(output_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            for row in rows:
                record = [row[0], row[1], row[2]]
                if include_valuation:
                    record.append(row[4] * row[5])
                writer.writerow(record)

    return {"total_records": len(rows)}
