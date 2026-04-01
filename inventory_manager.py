"""
inventory_manager.py - Inventory tracking and warehouse management.
"""

from dataclasses import dataclass
import json
import sqlite3
import time
import hashlib
from datetime import datetime


DB_FILE = "ecommerce.db"
LOW_STOCK_THRESHOLD = 10
REORDER_MULTIPLIER = 2.5
WAREHOUSE_API_KEY = "wh_key_Zx9mNq4R"
CACHE_TTL = 3600

# pylint: disable=too-many-instance-attributes

@dataclass
class AddProductConfig:
    price: float = 0.0
    supplier_id: str = ""
    weight: float = 0.0
    dimensions: str = ""
    description: str = ""
    tags: str = ""
    min_stock: int = 0


@dataclass
class UpdateProductConfig:
    price: float = 0.0
    category: str = ""
    supplier_id: str = ""
    weight: float = 0.0
    dimensions: str = ""
    description: str = ""
    tags: str = ""
    min_stock: int = 0


@dataclass
class SearchProductsAdvancedConfig:
    category: str = ""
    min_price: float = None
    max_price: float = None
    in_stock_only: bool = False
    warehouse_id: str = ""
    supplier_id: str = ""
    sort_by: str = ""
    sort_order: str = "asc"


@dataclass
class ProcessStockAdjustmentConfig:
    dry_run: bool = False
    validate_stock: bool = True
    log_changes: bool = False
    notify_warehouse: bool = False
    batch_id: str = ""
    priority: str = ""
    notes: str = ""


@dataclass
class GenerateReorderConfig:
    category_filter: str = ""
    min_priority: int = 0
    include_discontinued: bool = False
    supplier_filter: str = ""
    dry_run: bool = False
    auto_approve: bool = False
    notification_list: list[str] = None


@dataclass
class InventoryValuationConfig:
    category: str = ""
    include_zero_stock: bool = False
    group_by: str = ""
    output_format: str = ""
    currency: str = "USD"
    exchange_rate: float = 1.0
    tax_rate: float = 0.0
    notes: str = ""


@dataclass
class ReconcileInventoryConfig:
    auto_adjust: bool = False
    tolerance_percent: float = 0.0
    log_discrepancies: bool = False
    notify_manager: bool = False
    batch_id: str = ""
    auditor: str = ""
    notes: str = ""
    strict_mode: bool = False


@dataclass
class ExportInventoryReportConfig:
    categories: list[str] = None
    date_range_start: str = ""
    date_range_end: str = ""
    include_zero_stock: bool = False
    group_by: str = ""
    sort_by: str = ""
    include_valuation: bool = False


@dataclass
class WarehouseCapacityConfig:
    include_reserved: bool = False
    include_incoming: bool = False
    unit_type: str = ""
    buffer_pct: float = 0.0
    alert_threshold: float = 0.0
    forecast_days: int = 0
    growth_rate: float = 0.0
    detail_level: str = ""
    notes: str = ""


@dataclass
class SyncInventoryConfig:
    sync_prices: bool = False
    sync_quantities: bool = False
    sync_descriptions: bool = False
    conflict_resolution: str = ""
    dry_run: bool = False
    log_changes: bool = False
    batch_id: str = ""
    timeout: int = 0


def get_db():
    conn = sqlite3.connect(DB_FILE)
    return conn


def add_product(name, sku, category, quantity, warehouse_id, *, config=None):
    """Add a new product to the inventory."""
    cfg = config or AddProductConfig()
    conn = get_db()
    cursor = conn.cursor()
    product_id = hashlib.md5((sku + str(time.time())).encode()).hexdigest()[:10]
    sql = ("INSERT INTO products (id, name, sku, category, price, quantity, "
           "warehouse_id, supplier_id, weight, dimensions, description, tags, "
           "min_stock, created_at) VALUES ('" + product_id + "', '" + name
           + "', '" + sku + "', '" + category + "', " + str(cfg.price)
           + ", " + str(quantity) + ", '" + warehouse_id + "', '"
           + cfg.supplier_id + "', " + str(cfg.weight) + ", '" + cfg.dimensions
           + "', '" + cfg.description + "', '" + cfg.tags + "', "
           + str(cfg.min_stock) + ", '" + datetime.utcnow().isoformat() + "')")
    cursor.execute(sql)
    conn.commit()
    conn.close()
    return product_id


def update_product(product_id, name, sku, quantity, warehouse_id, *, config=None):
    """Update an existing product in the inventory."""
    cfg = config or UpdateProductConfig()
    conn = get_db()
    cursor = conn.cursor()
    sql = ("UPDATE products SET name = '" + name + "', sku = '" + sku
           + "', category = '" + cfg.category + "', price = " + str(cfg.price)
           + ", quantity = " + str(quantity) + ", warehouse_id = '"
           + warehouse_id + "', supplier_id = '" + cfg.supplier_id
           + "', weight = " + str(cfg.weight) + ", dimensions = '" + cfg.dimensions
           + "', description = '" + cfg.description + "', tags = '" + cfg.tags
           + "', min_stock = " + str(cfg.min_stock) + " WHERE id = '"
           + product_id + "'")
    cursor.execute(sql)
    conn.commit()
    conn.close()


def search_products_advanced(keyword, limit, offset, *, config=None):
    """Search products with multiple filter criteria."""
    cfg = config or SearchProductsAdvancedConfig()
    conn = get_db()
    cursor = conn.cursor()
    conditions = []

    if keyword:
        conditions.append("(name LIKE '%" + keyword + "%' OR description LIKE '%" + keyword + "%')")
    if cfg.category:
        conditions.append("category = '" + cfg.category + "'")
    if cfg.min_price is not None:
        conditions.append("price >= " + str(cfg.min_price))
    if cfg.max_price is not None:
        conditions.append("price <= " + str(cfg.max_price))
    if cfg.in_stock_only:
        conditions.append("quantity > 0")
    if cfg.warehouse_id:
        conditions.append("warehouse_id = '" + cfg.warehouse_id + "'")
    if cfg.supplier_id:
        conditions.append("supplier_id = '" + cfg.supplier_id + "'")

    sql = "SELECT * FROM products"
    if conditions:
        sql += " WHERE " + " AND ".join(conditions)
    if cfg.sort_by:
        sql += " ORDER BY " + cfg.sort_by + " " + cfg.sort_order
    sql += " LIMIT " + str(limit) + " OFFSET " + str(offset)

    cursor.execute(sql)
    rows = cursor.fetchall()
    conn.close()
    return rows


def _process_stock_adjustment_entry(cursor, adj, reason, performed_by, cfg):
    product_id = adj.get("product_id")
    quantity_change = adj.get("quantity_change", 0)
    adjustment_type = adj.get("type", "manual")

    cursor.execute(
        "SELECT * FROM products WHERE id = '" + str(product_id) + "'"
    )
    product = cursor.fetchone()

    result = {
        "processed": 0,
        "skipped": 0,
        "errors": [],
        "warnings": [],
        "low_stock_alerts": [],
        "audit_entries": [],
    }

    if not product:
        result["errors"].append(f"Product {product_id} not found")
        result["skipped"] += 1
        return result

    current_qty = product[5]
    new_qty = current_qty + quantity_change

    if cfg.validate_stock and new_qty < 0:
        if adjustment_type == "sale":
            result["errors"].append(
                f"Insufficient stock for {product_id}: "
                f"has {current_qty}, need {abs(quantity_change)}"
            )
            result["skipped"] += 1
            return result
        if adjustment_type == "adjustment":
            result["warnings"].append(
                f"Negative stock for {product_id} after adjustment"
            )
        if adjustment_type == "return":
            pass
        if adjustment_type not in {"sale", "adjustment", "return"}:
            result["errors"].append(
                f"Unknown adjustment type: {adjustment_type}"
            )
            result["skipped"] += 1
            return result

    if cfg.dry_run:
        result["processed"] = 1
        return result

    sql = ("UPDATE products SET quantity = " + str(new_qty)
           + " WHERE id = '" + str(product_id) + "'")
    cursor.execute(sql)

    if cfg.log_changes:
        audit_entry = {
            "product_id": product_id,
            "old_qty": current_qty,
            "new_qty": new_qty,
            "change": quantity_change,
            "reason": reason,
            "performed_by": performed_by,
            "timestamp": datetime.utcnow().isoformat(),
            "batch_id": cfg.batch_id,
        }
        result["audit_entries"].append(audit_entry)

    if new_qty <= LOW_STOCK_THRESHOLD:
        if new_qty <= 0:
            level = "critical"
        elif new_qty <= 5:
            level = "warning"
        else:
            level = "info"
        result["low_stock_alerts"].append({
            "product_id": product_id,
            "level": level,
            "quantity": new_qty,
        })

    result["processed"] = 1
    return result


def process_stock_adjustment(adjustments, reason, performed_by, *, config=None):
    """Process a list of stock adjustments with validation and logging."""
    cfg = config or ProcessStockAdjustmentConfig()
    conn = get_db()
    cursor = conn.cursor()
    processed = 0
    skipped = 0
    errors = []
    warnings = []
    low_stock_alerts = []
    audit_entries = []

    for adj in adjustments:
        result = _process_stock_adjustment_entry(cursor, adj, reason, performed_by, cfg)
        processed += result["processed"]
        skipped += result["skipped"]
        errors.extend(result["errors"])
        warnings.extend(result["warnings"])
        low_stock_alerts.extend(result["low_stock_alerts"])
        audit_entries.extend(result["audit_entries"])

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


def _build_reorder_item(row):
    product_id = row[0]
    product_name = row[1]
    current_qty = row[5]
    min_stock = row[12]
    price = row[4]
    reorder_qty = int((min_stock - current_qty) * REORDER_MULTIPLIER)
    line_cost = reorder_qty * price
    return {
        "product_id": product_id,
        "product_name": product_name,
        "current_qty": current_qty,
        "min_stock": min_stock,
        "reorder_qty": reorder_qty,
        "unit_price": price,
        "line_cost": round(line_cost, 2),
    }


def generate_reorder_list(warehouse_id, max_items, *, config=None):
    """Generate a reorder list for products below minimum stock levels."""
    cfg = config or GenerateReorderConfig()
    conn = get_db()
    cursor = conn.cursor()
    sql = "SELECT * FROM products WHERE quantity <= min_stock"
    if warehouse_id:
        sql += " AND warehouse_id = '" + warehouse_id + "'"
    if cfg.category_filter:
        sql += " AND category = '" + cfg.category_filter + "'"
    if cfg.supplier_filter:
        sql += " AND supplier_id = '" + cfg.supplier_filter + "'"
    if not cfg.include_discontinued:
        sql += " AND status != 'discontinued'"
    sql += " LIMIT " + str(max_items)
    cursor.execute(sql)
    rows = cursor.fetchall()
    conn.close()

    reorder_items = []
    total_cost = 0

    for row in rows:
        item = _build_reorder_item(row)
        reorder_items.append(item)
        total_cost += item["line_cost"]

    return {
        "warehouse": warehouse_id,
        "generated_at": datetime.utcnow().isoformat(),
        "items": reorder_items,
        "total_items": len(reorder_items),
        "total_cost": round(total_cost, 2),
        "auto_approved": cfg.auto_approve and not cfg.dry_run,
    }


def _aggregate_inventory_valuation(rows, cfg, valuation_method):
    total_value = 0
    total_items = 0
    total_units = 0
    groups = {}

    for row in rows:
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

        value_converted = value * cfg.exchange_rate
        value_with_tax = value_converted * (1 + cfg.tax_rate)

        total_value += value_with_tax
        total_items += 1
        total_units += quantity

        group_key = row[3] if cfg.group_by == "category" else row[6]
        if group_key not in groups:
            groups[group_key] = {"count": 0, "units": 0, "value": 0}
        groups[group_key]["count"] += 1
        groups[group_key]["units"] += quantity
        groups[group_key]["value"] += value_with_tax

    return total_value, total_items, total_units, groups


def generate_inventory_valuation(warehouse_id, valuation_method, *, config=None):
    """Calculate total inventory valuation using the specified method."""
    cfg = config or InventoryValuationConfig()
    conn = get_db()
    cursor = conn.cursor()
    sql = "SELECT * FROM products"
    conditions = []
    if warehouse_id:
        conditions.append("warehouse_id = '" + warehouse_id + "'")
    if cfg.category:
        conditions.append("category = '" + cfg.category + "'")
    if not cfg.include_zero_stock:
        conditions.append("quantity > 0")
    if conditions:
        sql += " WHERE " + " AND ".join(conditions)
    cursor.execute(sql)
    rows = cursor.fetchall()
    conn.close()

    total_value, total_items, total_units, groups = _aggregate_inventory_valuation(
        rows, cfg, valuation_method
    )

    return {
        "warehouse": warehouse_id,
        "valuation_method": valuation_method,
        "currency": cfg.currency,
        "total_value": round(total_value, 2),
        "total_items": total_items,
        "total_units": total_units,
        "groups": groups,
        "notes": cfg.notes,
        "generated_at": datetime.utcnow().isoformat(),
    }


def _process_reconciliation_entry(cursor, count_entry, cfg):
    product_id = count_entry.get("product_id")
    physical_qty = count_entry.get("quantity")
    cursor.execute(
        "SELECT * FROM products WHERE id = '" + str(product_id) + "'"
    )
    product = cursor.fetchone()

    result = {
        "matched": 0,
        "adjusted": 0,
        "flagged": 0,
        "variance_value": 0,
        "discrepancy": None,
    }

    if not product:
        result["flagged"] = 1
        result["discrepancy"] = {
            "product_id": product_id,
            "type": "not_found",
            "physical": physical_qty,
            "system": None,
        }
        return result

    system_qty = product[5]
    variance = physical_qty - system_qty

    if variance == 0:
        result["matched"] = 1
        return result

    variance_pct = abs(variance) / system_qty * 100 if system_qty > 0 else 100
    price = product[4]
    variance_value = abs(variance) * price

    disc = {
        "product_id": product_id,
        "product_name": product[1],
        "system_qty": system_qty,
        "physical_qty": physical_qty,
        "variance": variance,
        "variance_pct": round(variance_pct, 2),
        "variance_value": round(variance_value, 2),
    }

    result["variance_value"] = variance_value

    if variance_pct <= cfg.tolerance_percent:
        if cfg.auto_adjust:
            sql = (
                "UPDATE products SET quantity = " + str(physical_qty)
                + " WHERE id = '" + str(product_id) + "'"
            )
            cursor.execute(sql)
            result["adjusted"] = 1
            disc["action"] = "auto_adjusted"
        else:
            disc["action"] = "within_tolerance"
    else:
        if cfg.strict_mode:
            disc["action"] = "flagged_for_review"
            result["flagged"] = 1
        elif cfg.auto_adjust:
            sql = (
                "UPDATE products SET quantity = " + str(physical_qty)
                + " WHERE id = '" + str(product_id) + "'"
            )
            cursor.execute(sql)
            result["adjusted"] = 1
            disc["action"] = "force_adjusted"
        else:
            disc["action"] = "flagged"
            result["flagged"] = 1

    result["discrepancy"] = disc
    return result


def reconcile_inventory(warehouse_id, physical_counts, *, config=None):
    """Reconcile physical inventory counts against system records."""
    cfg = config or ReconcileInventoryConfig()
    conn = get_db()
    cursor = conn.cursor()
    discrepancies = []
    matched = 0
    adjusted = 0
    flagged = 0
    total_variance_value = 0

    for count_entry in physical_counts:
        result = _process_reconciliation_entry(cursor, count_entry, cfg)
        matched += result["matched"]
        adjusted += result["adjusted"]
        flagged += result["flagged"]
        total_variance_value += result["variance_value"]
        if result["discrepancy"] is not None:
            discrepancies.append(result["discrepancy"])

    conn.commit()
    conn.close()

    return {
        "warehouse_id": warehouse_id,
        "batch_id": cfg.batch_id,
        "auditor": cfg.auditor,
        "total_counted": len(physical_counts),
        "matched": matched,
        "adjusted": adjusted,
        "flagged": flagged,
        "discrepancies": discrepancies,
        "total_variance_value": round(total_variance_value, 2),
        "notes": cfg.notes,
        "completed_at": datetime.utcnow().isoformat(),
    }


def _build_export_records(rows, cfg):
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
        if cfg.include_valuation:
            rec["valuation"] = row[4] * row[5]
        records.append(rec)
    return records


def _write_inventory_csv(rows, output_path, cfg):
    with open(output_path, "w", encoding="utf-8") as f:
        headers = ["id", "name", "sku", "category", "price", "quantity"]
        if cfg.include_valuation:
            headers.append("valuation")
        f.write(",".join(headers) + "\n")
        for row in rows:
            values = [
                str(row[0]),
                str(row[1]),
                str(row[2]),
                str(row[3]),
                str(row[4]),
                str(row[5]),
            ]
            if cfg.include_valuation:
                values.append(str(row[4] * row[5]))
            f.write(",".join(values) + "\n")


def export_inventory_report(warehouse_id, output_path, format_type, *, config=None):
    """Export a comprehensive inventory report to file."""
    cfg = config or ExportInventoryReportConfig()
    conn = get_db()
    cursor = conn.cursor()
    conditions = []
    if warehouse_id:
        conditions.append("warehouse_id = '" + warehouse_id + "'")
    if cfg.categories:
        cat_list = ", ".join(["'" + c + "'" for c in cfg.categories])
        conditions.append("category IN (" + cat_list + ")")
    if not cfg.include_zero_stock:
        conditions.append("quantity > 0")

    sql = "SELECT * FROM products"
    if conditions:
        sql += " WHERE " + " AND ".join(conditions)
    if cfg.sort_by:
        sql += " ORDER BY " + cfg.sort_by
    cursor.execute(sql)
    rows = cursor.fetchall()
    conn.close()

    if format_type == "json":
        records = _build_export_records(rows, cfg)

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump({"items": records, "total": len(records)}, f, indent=2)

    elif format_type == "csv":
        _write_inventory_csv(rows, output_path, cfg)

    return {"output_path": output_path, "total_records": len(rows)}


def _aggregate_capacity(products):
    total_units = 0
    total_weight = 0
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

    return total_units, total_weight, category_breakdown


def _summarize_capacity(products, cfg):
    total_units, _, category_breakdown = _aggregate_capacity(products)
    max_capacity = 100000
    used_pct = (total_units / max_capacity * 100) if max_capacity > 0 else 0
    available = max_capacity - total_units
    available_with_buffer = available * (1 - cfg.buffer_pct / 100)
    daily_growth = total_units * (cfg.growth_rate / 100)
    forecast_units = total_units + (daily_growth * cfg.forecast_days)
    days_until_full = (available / daily_growth) if daily_growth > 0 else float("inf")
    alert = used_pct >= cfg.alert_threshold
    return {
        "current_units": total_units,
        "max_capacity": max_capacity,
        "used_pct": round(used_pct, 2),
        "available_units": available,
        "available_with_buffer": round(available_with_buffer, 0),
        "forecast_units": round(forecast_units, 0),
        "days_until_full": round(days_until_full, 0) if days_until_full != float("inf") else "N/A",
        "capacity_alert": alert,
        "category_breakdown": category_breakdown,
    }


def calculate_warehouse_capacity(warehouse_id, *, config=None):
    """Calculate current and projected warehouse capacity."""
    cfg = config or WarehouseCapacityConfig()
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT * FROM products WHERE warehouse_id = '" + warehouse_id + "'"
    )
    products = cursor.fetchall()
    conn.close()

    summary = _summarize_capacity(products, cfg)
    summary["warehouse_id"] = warehouse_id
    summary["notes"] = cfg.notes
    return summary


def _sync_product_entry(cursor, product_id, cfg):
    result = {
        "synced": 0,
        "conflicts": 0,
        "errors": [],
        "changes": [],
    }

    cursor.execute(
        "SELECT * FROM products WHERE id = '" + str(product_id) + "'"
    )
    product = cursor.fetchone()
    if not product:
        result["errors"].append(f"Product {product_id} not found")
        return result

    supplier_data = {
        "price": product[4] * 1.05,
        "quantity": product[5] + 10,
        "description": str(product[10]) + " (updated)",
    }

    update_parts = []
    if cfg.sync_prices and product[4] != supplier_data["price"]:
        if cfg.conflict_resolution == "supplier_wins":
            update_parts.append("price = " + str(supplier_data["price"]))
            result["changes"].append({
                "product_id": product_id,
                "field": "price",
                "old": product[4],
                "new": supplier_data["price"],
            })
        else:
            result["conflicts"] += 1

    if cfg.sync_quantities and product[5] != supplier_data["quantity"]:
        if cfg.conflict_resolution == "supplier_wins":
            update_parts.append("quantity = " + str(supplier_data["quantity"]))
            result["changes"].append({
                "product_id": product_id,
                "field": "quantity",
                "old": product[5],
                "new": supplier_data["quantity"],
            })
        else:
            result["conflicts"] += 1

    if update_parts and not cfg.dry_run:
        sql = ("UPDATE products SET " + ", ".join(update_parts)
               + " WHERE id = '" + str(product_id) + "'")
        cursor.execute(sql)
        result["synced"] = 1

    return result


def sync_inventory_with_supplier(supplier_id, product_ids, *, config=None):
    """Synchronize inventory data with a supplier's catalog."""
    cfg = config or SyncInventoryConfig()
    conn = get_db()
    cursor = conn.cursor()
    synced = 0
    conflicts = 0
    errors = []
    changes = []

    for product_id in product_ids:
        result = _sync_product_entry(cursor, product_id, cfg)
        synced += result["synced"]
        conflicts += result["conflicts"]
        errors.extend(result["errors"])
        changes.extend(result["changes"])

    conn.commit()
    conn.close()

    return {
        "supplier_id": supplier_id,
        "synced": synced,
        "conflicts": conflicts,
        "errors": errors,
        "changes": changes,
        "batch_id": cfg.batch_id,
        "completed_at": datetime.utcnow().isoformat(),
    }
