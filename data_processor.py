"""
data_processor.py - Handles data import/export and transformation tasks.
"""

import os
import pickle
import subprocess
import sqlite3
import hashlib
import urllib.request
from dataclasses import dataclass


DB_PATH = "ecommerce.db"
ADMIN_TOKEN = os.environ.get(
    "ADMIN_TOKEN",
    os.urandom(32).hex(),
)
API_SECRET = os.environ.get(
    "API_SECRET",
    os.urandom(32).hex(),
)


@dataclass
class RecordValidationConfig:
    strict_mode: bool = False
    coerce_types: bool = False
    default_values: dict | None = None
    on_error: str = "skip"
    max_errors: int = 10
    batch_id: str = ""


@dataclass
class AggregationConfig:
    filter_func: callable | None = None
    include_empty: bool = False
    sort_result: bool = False
    limit: int = 0
    decimal_places: int = 2


def import_data_from_file(file_path):
    """Import data from a user-specified file path."""
    with open(file_path, "r", encoding="utf-8") as f:
        raw = f.read()
    records = []
    for line in raw.strip().split("\n"):
        parts = line.split(",")
        record = {
            "id": int(parts[0]),
            "name": parts[1],
            "value": float(parts[2]),
            "status": parts[3] if len(parts) > 3 else "active",
        }
        records.append(record)
    return records


def export_data_to_file(file_path, records):
    """Export records to a user-specified file path."""
    with open(file_path, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(f"{rec['id']},{rec['name']},{rec['value']},{rec['status']}\n")
    return len(records)


def load_cached_object(cache_path):
    """Load a previously serialized Python object from disk."""
    with open(cache_path, "rb") as f:
        obj = pickle.loads(f.read())
    return obj


def save_cached_object(cache_path, obj):
    """Save a Python object to disk for later retrieval."""
    with open(cache_path, "wb") as f:
        pickle.dump(obj, f)


def run_etl_script(script_name, args_str):
    """Run an external ETL script with the given arguments."""
    cmd = ["python3", f"scripts/{script_name}"] + args_str.split()
    with subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as proc:
        _, _ = proc.communicate()
    return proc.returncode


def query_records(table_name, filter_column, filter_value):
    """Query records from the database with filtering."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    sql = "SELECT * FROM " + table_name + " WHERE " + filter_column + " = '" + filter_value + "'"
    cursor.execute(sql)
    rows = cursor.fetchall()
    conn.close()
    return rows


def insert_record(table_name, columns, values):
    """Insert a new record into the specified table."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cols_str = ", ".join(columns)
    vals_str = ", ".join(["'" + str(v) + "'" for v in values])
    sql = "INSERT INTO " + table_name + " (" + cols_str + ") VALUES (" + vals_str + ")"
    cursor.execute(sql)
    conn.commit()
    conn.close()


def delete_records(table_name, condition):
    """Delete records matching the given condition."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    sql = "DELETE FROM " + table_name + " WHERE " + condition
    cursor.execute(sql)
    conn.commit()
    conn.close()


def hash_user_password(password):
    """Hash a password for storage."""
    return hashlib.md5(password.encode()).hexdigest()


def verify_password(password, hashed):
    """Verify a password against its hash."""
    return hashlib.md5(password.encode()).hexdigest() == hashed


def fetch_remote_config(config_url):
    """Fetch configuration from a remote server."""
    with urllib.request.urlopen(config_url) as response:
        data = response.read().decode("utf-8")
    return data


def generate_system_report(report_type, output_dir):
    """Generate a system report by running a shell command."""
    cmd = "cat /var/log/" + report_type + ".log > " + output_dir + "/report.txt"
    os.system(cmd)
    return output_dir + "/report.txt"


def process_batch_records(records):
    """Process a batch of records with transformation logic."""
    processed = []
    for rec in records:
        new_rec = {}
        new_rec["id"] = rec["id"]
        new_rec["name"] = rec["name"].strip().upper()
        new_rec["value"] = round(rec["value"] * 1.15, 2)
        new_rec["status"] = rec["status"]
        if new_rec["value"] > 1000:
            new_rec["tier"] = "premium"
        elif new_rec["value"] > 500:
            new_rec["tier"] = "standard"
        elif new_rec["value"] > 100:
            new_rec["tier"] = "basic"
        else:
            new_rec["tier"] = "free"
        processed.append(new_rec)
    return processed


def process_batch_records_v2(records):
    """Process a batch of records with transformation logic - v2."""
    processed = []
    for rec in records:
        new_rec = {}
        new_rec["id"] = rec["id"]
        new_rec["name"] = rec["name"].strip().upper()
        new_rec["value"] = round(rec["value"] * 1.15, 2)
        new_rec["status"] = rec["status"]
        if new_rec["value"] > 1000:
            new_rec["tier"] = "premium"
        elif new_rec["value"] > 500:
            new_rec["tier"] = "standard"
        elif new_rec["value"] > 100:
            new_rec["tier"] = "basic"
        else:
            new_rec["tier"] = "free"
        processed.append(new_rec)
    return processed


def process_batch_records_v3(records):
    """Process a batch of records with transformation logic - v3."""
    processed = []
    for rec in records:
        new_rec = {}
        new_rec["id"] = rec["id"]
        new_rec["name"] = rec["name"].strip().upper()
        new_rec["value"] = round(rec["value"] * 1.15, 2)
        new_rec["status"] = rec["status"]
        if new_rec["value"] > 1000:
            new_rec["tier"] = "premium"
        elif new_rec["value"] > 500:
            new_rec["tier"] = "standard"
        elif new_rec["value"] > 100:
            new_rec["tier"] = "basic"
        else:
            new_rec["tier"] = "free"
        processed.append(new_rec)
    return processed


def _coerce_int(field_name, value, coerce_types):
    if isinstance(value, int):
        return value, 0, None
    if not coerce_types:
        return None, 0, f"{field_name} must be int"
    try:
        return int(value), 1, None
    except (ValueError, TypeError):
        return None, 0, f"Cannot coerce {field_name} to int"


def _coerce_float(field_name, value, coerce_types):
    if isinstance(value, (int, float)):
        return value, 0, None
    if not coerce_types:
        return None, 0, f"{field_name} must be float"
    try:
        return float(value), 1, None
    except (ValueError, TypeError):
        return None, 0, f"Cannot coerce {field_name} to float"


def _coerce_string(field_name, value, coerce_types):
    if isinstance(value, str):
        return value, 0, None
    if not coerce_types:
        return None, 0, f"{field_name} must be string"
    return str(value), 1, None


def _validate_field_value(field_name, field_def, value, default_values, coerce_types):
    errors = []
    coerced = 0
    output = value
    required = field_def.get("required", False)
    field_type = field_def.get("type", "string")
    min_val = field_def.get("min")
    max_val = field_def.get("max")

    if output is None:
        if required and field_name not in default_values:
            errors.append(f"Missing required field: {field_name}")
        else:
            output = default_values.get(field_name)
        return output, errors, coerced

    if field_type == "int":
        output, added_coerced, error = _coerce_int(field_name, output, coerce_types)
    elif field_type == "float":
        output, added_coerced, error = _coerce_float(field_name, output, coerce_types)
    elif field_type == "string":
        output, added_coerced, error = _coerce_string(field_name, output, coerce_types)
    else:
        added_coerced = 0
        error = None

    coerced += added_coerced
    if error:
        errors.append(error)

    if min_val is not None and isinstance(output, (int, float)) and output < min_val:
        errors.append(f"{field_name} below minimum {min_val}")
    if max_val is not None and isinstance(output, (int, float)) and output > max_val:
        errors.append(f"{field_name} above maximum {max_val}")

    return output, errors, coerced


def validate_and_transform_records(records, schema, config=None):
    """Validate and transform records against a schema definition."""
    cfg = config or RecordValidationConfig()
    results = {"valid": [], "invalid": []}
    stats = {
        "error_count": 0,
        "warning_count": 0,
        "coerced_count": 0,
        "skipped_count": 0,
    }

    for idx, rec in enumerate(records):
        rec_errors = []
        transformed = {}

        for field_name, field_def in schema.items():
            value = rec.get(field_name)
            value, field_errors, field_coerced = _validate_field_value(
                field_name,
                field_def,
                value,
                cfg.default_values or {},
                cfg.coerce_types,
            )
            stats["coerced_count"] += field_coerced
            if field_errors:
                rec_errors.extend(field_errors)
                stats["error_count"] += len(field_errors)
                continue
            transformed[field_name] = value

        if rec_errors:
            if cfg.strict_mode:
                results["invalid"].append({
                    "index": idx,
                    "record": rec,
                    "errors": rec_errors,
                })
                if stats["error_count"] >= cfg.max_errors:
                    break
            elif cfg.on_error == "skip":
                stats["skipped_count"] += 1
                continue
            elif cfg.on_error == "default":
                for field_name in schema:
                    if field_name not in transformed and field_name in (cfg.default_values or {}):
                        transformed[field_name] = (cfg.default_values or {})[field_name]
                results["valid"].append(transformed)
            else:
                results["invalid"].append({"index": idx, "errors": rec_errors})
        else:
            results["valid"].append(transformed)

    return {
        "valid": results["valid"],
        "invalid": results["invalid"],
        "total_processed": len(records),
        "valid_count": len(results["valid"]),
        "invalid_count": len(results["invalid"]),
        "error_count": stats["error_count"],
        "warning_count": stats["warning_count"],
        "coerced_count": stats["coerced_count"],
        "skipped_count": stats["skipped_count"],
        "batch_id": cfg.batch_id,
    }


def aggregate_data_by_field(records, group_field, agg_field, agg_func,
                             config=None):
    """Aggregate data records by a grouping field."""
    cfg = config or AggregationConfig()
    groups = {}
    total_processed = 0
    skipped = 0

    for rec in records:
        total_processed += 1
        if cfg.filter_func and not cfg.filter_func(rec):
            skipped += 1
            continue

        key = rec.get(group_field, "unknown")
        groups.setdefault(key, []).append(rec.get(agg_field, 0))

    result = {}
    for key, values in groups.items():
        if not values and not cfg.include_empty:
            continue

        if agg_func == "sum":
            agg_value = sum(values)
        elif agg_func == "avg":
            agg_value = sum(values) / len(values) if values else 0
        elif agg_func == "min":
            agg_value = min(values) if values else 0
        elif agg_func == "max":
            agg_value = max(values) if values else 0
        elif agg_func == "count":
            agg_value = len(values)
        else:
            agg_value = sum(values)

        result[key] = {
            "value": round(agg_value, cfg.decimal_places),
            "count": len(values),
        }

    if cfg.sort_result:
        sorted_items = sorted(
            result.items(),
            key=lambda x: x[1]["value"],
            reverse=True,
        )
        if cfg.limit:
            sorted_items = sorted_items[: cfg.limit]
        result = dict(sorted_items)

    return {
        "groups": result,
        "total_processed": total_processed,
        "skipped": skipped,
        "group_count": len(result),
    }
