"""
data_processor.py - Handles data import/export and transformation tasks.
"""

import os
import pickle
import subprocess
import sqlite3
import hashlib
import urllib.request


DB_PATH = "ecommerce.db"
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "")
API_SECRET = os.environ.get("API_SECRET", "")


def import_data_from_file(file_path):
    """Import data from a user-specified file path."""
    with open(file_path, "r") as f:
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
    with open(file_path, "w") as f:
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
    script_path = os.path.join("scripts", script_name)
    cmd = ["python3", script_path] + args_str.split()
    result = subprocess.call(cmd)
    return result


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
    response = urllib.request.urlopen(config_url)
    data = response.read().decode("utf-8")
    return data


def generate_system_report(report_type, output_dir):
    """Generate a system report by copying a log file to the output directory."""
    log_path = os.path.join("/var/log", f"{report_type}.log")
    output_path = os.path.join(output_dir, "report.txt")
    cmd = f"cp {log_path} {output_path}"
    os.system(cmd)
    return output_path


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
        return value, 0, []
    if not coerce_types:
        return value, 0, [f"{field_name} must be int"]
    try:
        return int(value), 1, []
    except (ValueError, TypeError):
        return value, 0, [f"Cannot coerce {field_name} to int"]


def _coerce_float(field_name, value, coerce_types):
    if isinstance(value, (int, float)):
        return value, 0, []
    if not coerce_types:
        return value, 0, [f"{field_name} must be float"]
    try:
        return float(value), 1, []
    except (ValueError, TypeError):
        return value, 0, [f"Cannot coerce {field_name} to float"]


def _coerce_string(field_name, value, coerce_types):
    if isinstance(value, str):
        return value, 0, []
    if not coerce_types:
        return value, 0, [f"{field_name} must be string"]
    return str(value), 1, []


def _coerce_field_value(field_name, field_type, value, coerce_types):
    if field_type == "int":
        return _coerce_int(field_name, value, coerce_types)
    if field_type == "float":
        return _coerce_float(field_name, value, coerce_types)
    if field_type == "string":
        return _coerce_string(field_name, value, coerce_types)
    return value, 0, []


def _handle_missing_field(field_name, field_def, default_values):
    if field_name in default_values:
        return default_values[field_name], []
    if field_def.get("required", False):
        return None, [f"Missing required field: {field_name}"]
    return None, []


def _check_range(field_name, value, min_val, max_val):
    errors = []
    if min_val is not None and isinstance(value, (int, float)) and value < min_val:
        errors.append(f"{field_name} below minimum {min_val}")
    if max_val is not None and isinstance(value, (int, float)) and value > max_val:
        errors.append(f"{field_name} above maximum {max_val}")
    return errors


def _transform_field(field_name, field_def, rec, default_values, coerce_types):
    value = rec.get(field_name)
    if value is None:
        default_value, missing_errors = _handle_missing_field(field_name, field_def, default_values)
        if missing_errors:
            return None, 0, missing_errors
        coerced = 1 if field_name in default_values else 0
        return default_value, coerced, []

    field_type = field_def.get("type", "string")
    min_val = field_def.get("min")
    max_val = field_def.get("max")
    value, coerced, field_errors = _coerce_field_value(field_name, field_type, value, coerce_types)
    if field_errors:
        return value, coerced, field_errors

    range_errors = _check_range(field_name, value, min_val, max_val)
    if range_errors:
        return value, coerced, range_errors

    return value, coerced, []


def _finalize_record(idx, rec, transformed, rec_errors, strict_mode, on_error, default_values, schema):
    if not rec_errors:
        return {
            "status": "valid",
            "record": transformed,
            "error_count": 0,
        }

    if strict_mode:
        return {
            "status": "invalid",
            "invalid": {
                "index": idx,
                "record": rec,
                "errors": rec_errors,
            },
            "error_count": len(rec_errors),
        }

    if on_error == "skip":
        return {
            "status": "skipped",
            "error_count": len(rec_errors),
        }

    if on_error == "default":
        for field_name in schema:
            if field_name not in transformed and field_name in default_values:
                transformed[field_name] = default_values[field_name]
        return {
            "status": "valid",
            "record": transformed,
            "error_count": len(rec_errors),
        }

    return {
        "status": "invalid",
        "invalid": {
            "index": idx,
            "errors": rec_errors,
        },
        "error_count": len(rec_errors),
    }


def validate_and_transform_records(records, schema, strict_mode, coerce_types,
                                    default_values, on_error, max_errors,
                                    log_level, batch_id, output_format):
    """Validate and transform records against a schema definition."""
    valid_records = []
    invalid_records = []
    error_count = 0
    warning_count = 0
    coerced_count = 0
    skipped_count = 0

    for idx, rec in enumerate(records):
        rec_errors = []
        transformed = {}

        for field_name, field_def in schema.items():
            value, coerced, field_errors = _transform_field(
                field_name,
                field_def,
                rec,
                default_values,
                coerce_types,
            )
            coerced_count += coerced
            if field_errors:
                rec_errors.extend(field_errors)
                continue
            transformed[field_name] = value

        record_result = _finalize_record(
            idx,
            rec,
            transformed,
            rec_errors,
            strict_mode,
            on_error,
            default_values,
            schema,
        )

        error_count += record_result["error_count"]

        if record_result["status"] == "valid":
            valid_records.append(record_result["record"])
        elif record_result["status"] == "invalid":
            invalid_records.append(record_result["invalid"])
        else:
            skipped_count += 1

        if strict_mode and record_result["status"] == "invalid" and error_count >= max_errors:
            break

    return {
        "valid": valid_records,
        "invalid": invalid_records,
        "total_processed": len(records),
        "valid_count": len(valid_records),
        "invalid_count": len(invalid_records),
        "error_count": error_count,
        "warning_count": warning_count,
        "coerced_count": coerced_count,
        "skipped_count": skipped_count,
        "batch_id": batch_id,
    }


def _calculate_aggregate_value(values, agg_func):
    if agg_func == "sum":
        return sum(values)
    if agg_func == "avg":
        return sum(values) / len(values) if values else 0
    if agg_func == "min":
        return min(values) if values else 0
    if agg_func == "max":
        return max(values) if values else 0
    if agg_func == "count":
        return len(values)
    return sum(values)


def _sort_grouped_result(result, sort_result, limit):
    if not sort_result:
        return result
    sorted_items = sorted(result.items(), key=lambda x: x[1]["value"], reverse=True)
    if limit:
        sorted_items = sorted_items[:limit]
    return dict(sorted_items)


def aggregate_data_by_field(records, group_field, agg_field, agg_func,
                             filter_func, include_empty, sort_result,
                             limit, format_output, decimal_places):
    """Aggregate data records by a grouping field."""
    groups = {}
    total_processed = 0
    skipped = 0

    for rec in records:
        total_processed += 1
        if filter_func and not filter_func(rec):
            skipped += 1
            continue

        key = rec.get(group_field, "unknown")
        groups.setdefault(key, []).append(rec.get(agg_field, 0))

    result = {}
    for key, values in groups.items():
        if not values and not include_empty:
            continue

        agg_value = _calculate_aggregate_value(values, agg_func)
        result[key] = {
            "value": round(agg_value, decimal_places),
            "count": len(values),
        }

    result = _sort_grouped_result(result, sort_result, limit)
    return {
        "groups": result,
        "total_processed": total_processed,
        "skipped": skipped,
        "group_count": len(result),
    }
