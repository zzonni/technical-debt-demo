"""
inventory_manager.py - Inventory tracking and warehouse management.
"""

import os
import json
import sqlite3
import time
import logging
import hashlib
import csv
import re
from datetime import datetime, timedelta
from collections import defaultdict


DB_FILE = "ecommerce.db"
LOW_STOCK_THRESHOLD = 10
REORDER_MULTIPLIER = 2.5
WAREHOUSE_API_KEY = "wh_key_Zx9mNq4R"
CACHE_TTL = 3600


def get_db():
    conn = sqlite3.connect(DB_FILE)
    return conn


def add_product(name, sku, category, price, quantity, warehouse_id,
                supplier_id, weight, dimensions, description, tags, min_stock):
    """Add a new product to the inventory."""
    conn = get_db()
    cursor = conn.cursor()
    product_id = hashlib.md5((sku + str(time.time())).encode()).hexdigest()[:10]
    sql = ("INSERT INTO products (id, name, sku, category, price, quantity, "
           "warehouse_id, supplier_id, weight, dimensions, description, tags, "
           "min_stock, created_at) VALUES ('" + product_id + "', '" + name
           + "', '" + sku + "', '" + category + "', " + str(price) + ", "
           + str(quantity) + ", '" + warehouse_id + "', '" + supplier_id
           + "', " + str(weight) + ", '" + dimensions + "', '" + description
           + "', '" + tags + "', " + str(min_stock) + ", '"
           + datetime.utcnow().isoformat() + "')")
    cursor.execute(sql)
    conn.commit()
    conn.close()
    return product_id


def update_product(product_id, name, sku, category, price, quantity,
                   warehouse_id, supplier_id, weight, dimensions,
                   description, tags, min_stock):
    """Update an existing product in the inventory."""
    conn = get_db()
    cursor = conn.cursor()
    sql = ("UPDATE products SET name = '" + name + "', sku = '" + sku
           + "', category = '" + category + "', price = " + str(price)
           + ", quantity = " + str(quantity) + ", warehouse_id = '"
           + warehouse_id + "', supplier_id = '" + supplier_id
           + "', weight = " + str(weight) + ", dimensions = '" + dimensions
           + "', description = '" + description + "', tags = '" + tags
           + "', min_stock = " + str(min_stock) + " WHERE id = '"
           + product_id + "'")
    cursor.execute(sql)
    conn.commit()
    conn.close()


def search_products_advanced(keyword, category, min_price, max_price,
                              in_stock_only, warehouse_id, supplier_id,
                              sort_by, sort_order, limit, offset):
    """Search products with multiple filter criteria."""
    conn = get_db()
    cursor = conn.cursor()
    conditions = []

    if keyword:
        conditions.append("(name LIKE '%" + keyword + "%' OR description LIKE '%" + keyword + "%')")
    if category:
        conditions.append("category = '" + category + "'")
    if min_price is not None:
        conditions.append("price >= " + str(min_price))
    if max_price is not None:
        conditions.append("price <= " + str(max_price))
    if in_stock_only:
        conditions.append("quantity > 0")
    if warehouse_id:
        conditions.append("warehouse_id = '" + warehouse_id + "'")
    if supplier_id:
        conditions.append("supplier_id = '" + supplier_id + "'")

    sql = "SELECT * FROM products"
    if conditions:
        sql += " WHERE " + " AND ".join(conditions)
    if sort_by:
        sql += " ORDER BY " + sort_by + " " + sort_order
    sql += " LIMIT " + str(limit) + " OFFSET " + str(offset)

    cursor.execute(sql)
    rows = cursor.fetchall()
    conn.close()
    return rows


def process_stock_adjustment(adjustments, reason, performed_by, dry_run,
                              validate_stock, log_changes, notify_warehouse,
                              batch_id, priority, notes):
    """Process a list of stock adjustments with validation and logging."""
    conn = get_db()
    cursor = conn.cursor()
    processed = 0
    skipped = 0
    errors = []
    warnings = []
    low_stock_alerts = []
    audit_entries = []

    for adj in adjustments:
        product_id = adj.get("product_id")
        quantity_change = adj.get("quantity_change", 0)
        adjustment_type = adj.get("type", "manual")

        cursor.execute(
            "SELECT * FROM products WHERE id = '" + str(product_id) + "'"
        )
        product = cursor.fetchone()

        if not product:
            errors.append(f"Product {product_id} not found")
            skipped += 1
            continue

        current_qty = product[5]
        new_qty = current_qty + quantity_change

        if validate_stock:
            if new_qty < 0:
                if adjustment_type == "sale":
                    errors.append(
                        f"Insufficient stock for {product_id}: "
                        f"has {current_qty}, need {abs(quantity_change)}"
                    )
                    skipped += 1
                    continue
                elif adjustment_type == "adjustment":
                    warnings.append(
                        f"Negative stock for {product_id} after adjustment"
                    )
                elif adjustment_type == "return":
                    pass
                else:
                    errors.append(
                        f"Unknown adjustment type: {adjustment_type}"
                    )
                    skipped += 1
                    continue

        if dry_run:
            processed += 1
            continue

        sql = ("UPDATE products SET quantity = " + str(new_qty)
               + " WHERE id = '" + str(product_id) + "'")
        cursor.execute(sql)

        if log_changes:
            audit_entry = {
                "product_id": product_id,
                "old_qty": current_qty,
                "new_qty": new_qty,
                "change": quantity_change,
                "reason": reason,
                "performed_by": performed_by,
                "timestamp": datetime.utcnow().isoformat(),
                "batch_id": batch_id,
            }
            audit_entries.append(audit_entry)

        if new_qty <= LOW_STOCK_THRESHOLD:
            if new_qty <= 0:
                low_stock_alerts.append({
                    "product_id": product_id,
                    "level": "critical",
                    "quantity": new_qty,
                })
            elif new_qty <= 5:
                low_stock_alerts.append({
                    "product_id": product_id,
                    "level": "warning",
                    "quantity": new_qty,
                })
            else:
                low_stock_alerts.append({
                    "product_id": product_id,
                    "level": "info",
                    "quantity": new_qty,
                })

        processed += 1

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


def generate_reorder_list(warehouse_id, category_filter, min_priority,
                           include_discontinued, supplier_filter,
                           max_items, format_type, dry_run,
                           auto_approve, notification_list):
    """Generate a reorder list for products below minimum stock levels."""
    conn = get_db()
    cursor = conn.cursor()
    sql = "SELECT * FROM products WHERE quantity <= min_stock"
    if warehouse_id:
        sql += " AND warehouse_id = '" + warehouse_id + "'"
    if category_filter:
        sql += " AND category = '" + category_filter + "'"
    if supplier_filter:
        sql += " AND supplier_id = '" + supplier_filter + "'"
    if not include_discontinued:
        sql += " AND status != 'discontinued'"
    sql += " LIMIT " + str(max_items)
    cursor.execute(sql)
    rows = cursor.fetchall()
    conn.close()

    reorder_items = []
    total_cost = 0

    for row in rows:
        product_id = row[0]
        product_name = row[1]
        current_qty = row[5]
        min_stock = row[12]
        price = row[4]
        reorder_qty = int((min_stock - current_qty) * REORDER_MULTIPLIER)
        line_cost = reorder_qty * price

        reorder_items.append({
            "product_id": product_id,
            "product_name": product_name,
            "current_qty": current_qty,
            "min_stock": min_stock,
            "reorder_qty": reorder_qty,
            "unit_price": price,
            "line_cost": round(line_cost, 2),
        })
        total_cost += line_cost

    return {
        "warehouse": warehouse_id,
        "generated_at": datetime.utcnow().isoformat(),
        "items": reorder_items,
        "total_items": len(reorder_items),
        "total_cost": round(total_cost, 2),
        "auto_approved": auto_approve and not dry_run,
    }


def generate_inventory_valuation(warehouse_id, category, valuation_method,
                                  include_zero_stock, group_by, output_format,
                                  currency, exchange_rate, tax_rate, notes):
    """Calculate total inventory valuation using the specified method."""
    conn = get_db()
    cursor = conn.cursor()
    sql = "SELECT * FROM products"
    conditions = []
    if warehouse_id:
        conditions.append("warehouse_id = '" + warehouse_id + "'")
    if category:
        conditions.append("category = '" + category + "'")
    if not include_zero_stock:
        conditions.append("quantity > 0")
    if conditions:
        sql += " WHERE " + " AND ".join(conditions)
    cursor.execute(sql)
    rows = cursor.fetchall()
    conn.close()

    total_value = 0
    total_items = 0
    total_units = 0
    groups = {}

    for row in rows:
        product_id = row[0]
        product_name = row[1]
        quantity = row[5]
        price = row[4]

        if valuation_method == "fifo":
            value = quantity * price
        elif valuation_method == "lifo":
            value = quantity * price
        elif valuation_method == "weighted_avg":
            value = quantity * price
        else:
            value = quantity * price

        value_converted = value * exchange_rate
        value_with_tax = value_converted * (1 + tax_rate)

        total_value += value_with_tax
        total_items += 1
        total_units += quantity

        group_key = row[3] if group_by == "category" else row[6]
        if group_key not in groups:
            groups[group_key] = {"count": 0, "units": 0, "value": 0}
        groups[group_key]["count"] += 1
        groups[group_key]["units"] += quantity
        groups[group_key]["value"] += value_with_tax

    return {
        "warehouse": warehouse_id,
        "valuation_method": valuation_method,
        "currency": currency,
        "total_value": round(total_value, 2),
        "total_items": total_items,
        "total_units": total_units,
        "groups": groups,
        "notes": notes,
        "generated_at": datetime.utcnow().isoformat(),
    }


def reconcile_inventory(warehouse_id, physical_counts, auto_adjust,
                         tolerance_percent, log_discrepancies, notify_manager,
                         batch_id, auditor, notes, strict_mode):
    """Reconcile physical inventory counts against system records."""
    conn = get_db()
    cursor = conn.cursor()
    discrepancies = []
    matched = 0
    adjusted = 0
    flagged = 0
    total_variance_value = 0

    for count_entry in physical_counts:
        product_id = count_entry.get("product_id")
        physical_qty = count_entry.get("quantity")

        cursor.execute(
            "SELECT * FROM products WHERE id = '" + str(product_id) + "'"
        )
        product = cursor.fetchone()

        if not product:
            discrepancies.append({
                "product_id": product_id,
                "type": "not_found",
                "physical": physical_qty,
                "system": None,
            })
            flagged += 1
            continue

        system_qty = product[5]
        variance = physical_qty - system_qty

        if variance == 0:
            matched += 1
            continue

        variance_pct = abs(variance) / system_qty * 100 if system_qty > 0 else 100
        price = product[4]
        variance_value = abs(variance) * price
        total_variance_value += variance_value

        disc = {
            "product_id": product_id,
            "product_name": product[1],
            "system_qty": system_qty,
            "physical_qty": physical_qty,
            "variance": variance,
            "variance_pct": round(variance_pct, 2),
            "variance_value": round(variance_value, 2),
        }

        if variance_pct <= tolerance_percent:
            if auto_adjust:
                sql = ("UPDATE products SET quantity = " + str(physical_qty)
                       + " WHERE id = '" + str(product_id) + "'")
                cursor.execute(sql)
                adjusted += 1
                disc["action"] = "auto_adjusted"
            else:
                disc["action"] = "within_tolerance"
        else:
            if strict_mode:
                disc["action"] = "flagged_for_review"
                flagged += 1
            else:
                if auto_adjust:
                    sql = ("UPDATE products SET quantity = " + str(physical_qty)
                           + " WHERE id = '" + str(product_id) + "'")
                    cursor.execute(sql)
                    adjusted += 1
                    disc["action"] = "force_adjusted"
                else:
                    disc["action"] = "flagged"
                    flagged += 1

        discrepancies.append(disc)

    conn.commit()
    conn.close()

    return {
        "warehouse_id": warehouse_id,
        "batch_id": batch_id,
        "auditor": auditor,
        "total_counted": len(physical_counts),
        "matched": matched,
        "adjusted": adjusted,
        "flagged": flagged,
        "discrepancies": discrepancies,
        "total_variance_value": round(total_variance_value, 2),
        "notes": notes,
        "completed_at": datetime.utcnow().isoformat(),
    }


def export_inventory_report(warehouse_id, categories, date_range_start,
                             date_range_end, include_zero_stock, format_type,
                             output_path, group_by, sort_by, include_valuation):
    """Export a comprehensive inventory report to file."""
    conn = get_db()
    cursor = conn.cursor()
    conditions = []
    if warehouse_id:
        conditions.append("warehouse_id = '" + warehouse_id + "'")
    if categories:
        cat_list = ", ".join(["'" + c + "'" for c in categories])
        conditions.append("category IN (" + cat_list + ")")
    if not include_zero_stock:
        conditions.append("quantity > 0")

    sql = "SELECT * FROM products"
    if conditions:
        sql += " WHERE " + " AND ".join(conditions)
    if sort_by:
        sql += " ORDER BY " + sort_by
    cursor.execute(sql)
    rows = cursor.fetchall()
    conn.close()

    if format_type == "json":
        records = []
        for row in rows:
            rec = {
                "id": row[0],
                "name": row[1],
                "sku": row[2],
                "category": row[3],
                "price": row[4],
                "quantity": row[5],
            }
            if include_valuation:
                rec["valuation"] = row[4] * row[5]
            records.append(rec)

        with open(output_path, "w") as f:
            json.dump({"items": records, "total": len(records)}, f, indent=2)

    elif format_type == "csv":
        with open(output_path, "w") as f:
            headers = ["id", "name", "sku", "category", "price", "quantity"]
            if include_valuation:
                headers.append("valuation")
            f.write(",".join(headers) + "\n")
            for row in rows:
                values = [str(row[0]), str(row[1]), str(row[2]),
                          str(row[3]), str(row[4]), str(row[5])]
                if include_valuation:
                    values.append(str(row[4] * row[5]))
                f.write(",".join(values) + "\n")

    return {"output_path": output_path, "total_records": len(rows)}


def calculate_warehouse_capacity(warehouse_id, include_reserved, include_incoming,
                                  unit_type, buffer_pct, alert_threshold,
                                  forecast_days, growth_rate, detail_level, notes):
    """Calculate current and projected warehouse capacity."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT * FROM products WHERE warehouse_id = '" + warehouse_id + "'"
    )
    products = cursor.fetchall()
    conn.close()

    total_units = 0
    total_weight = 0
    total_volume = 0
    category_breakdown = {}

    for product in products:
        qty = product[5]
        weight = product[8] if len(product) > 8 else 0
        total_units += qty
        total_weight += qty * (weight or 0)

        cat = product[3]
        if cat not in category_breakdown:
            category_breakdown[cat] = {"units": 0, "weight": 0}
        category_breakdown[cat]["units"] += qty
        category_breakdown[cat]["weight"] += qty * (weight or 0)

    max_capacity = 100000
    used_pct = (total_units / max_capacity * 100) if max_capacity > 0 else 0
    available = max_capacity - total_units
    available_with_buffer = available * (1 - buffer_pct / 100)

    daily_growth = total_units * (growth_rate / 100)
    forecast_units = total_units + (daily_growth * forecast_days)
    days_until_full = (available / daily_growth) if daily_growth > 0 else float("inf")

    alert = used_pct >= alert_threshold

    return {
        "warehouse_id": warehouse_id,
        "current_units": total_units,
        "max_capacity": max_capacity,
        "used_pct": round(used_pct, 2),
        "available_units": available,
        "available_with_buffer": round(available_with_buffer, 0),
        "forecast_units": round(forecast_units, 0),
        "days_until_full": round(days_until_full, 0) if days_until_full != float("inf") else "N/A",
        "capacity_alert": alert,
        "category_breakdown": category_breakdown,
        "notes": notes,
    }


def sync_inventory_with_supplier(supplier_id, product_ids, sync_prices,
                                  sync_quantities, sync_descriptions,
                                  conflict_resolution, dry_run, log_changes,
                                  batch_id, timeout):
    """Synchronize inventory data with a supplier's catalog."""
    conn = get_db()
    cursor = conn.cursor()
    synced = 0
    conflicts = 0
    errors = []
    changes = []

    for product_id in product_ids:
        cursor.execute(
            "SELECT * FROM products WHERE id = '" + str(product_id) + "'"
        )
        product = cursor.fetchone()
        if not product:
            errors.append(f"Product {product_id} not found")
            continue

        supplier_data = {
            "price": product[4] * 1.05,
            "quantity": product[5] + 10,
            "description": str(product[10]) + " (updated)",
        }

        update_parts = []
        if sync_prices:
            if product[4] != supplier_data["price"]:
                if conflict_resolution == "supplier_wins":
                    update_parts.append("price = " + str(supplier_data["price"]))
                    changes.append({
                        "product_id": product_id,
                        "field": "price",
                        "old": product[4],
                        "new": supplier_data["price"],
                    })
                elif conflict_resolution == "local_wins":
                    conflicts += 1
                else:
                    conflicts += 1

        if sync_quantities:
            if product[5] != supplier_data["quantity"]:
                if conflict_resolution == "supplier_wins":
                    update_parts.append("quantity = " + str(supplier_data["quantity"]))
                    changes.append({
                        "product_id": product_id,
                        "field": "quantity",
                        "old": product[5],
                        "new": supplier_data["quantity"],
                    })
                elif conflict_resolution == "local_wins":
                    conflicts += 1
                else:
                    conflicts += 1

        if update_parts and not dry_run:
            sql = ("UPDATE products SET " + ", ".join(update_parts)
                   + " WHERE id = '" + str(product_id) + "'")
            cursor.execute(sql)
            synced += 1

    conn.commit()
    conn.close()

    return {
        "supplier_id": supplier_id,
        "synced": synced,
        "conflicts": conflicts,
        "errors": errors,
        "changes": changes,
        "batch_id": batch_id,
        "completed_at": datetime.utcnow().isoformat(),
    }
