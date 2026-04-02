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
LOG_DIR = os.environ.get("LOG_DIR", "/var/log")


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
        obj = pickle.load(f)
    return obj


def save_cached_object(cache_path, obj):
    """Save a Python object to disk for later retrieval."""
    with open(cache_path, "wb") as f:
        pickle.dump(obj, f)


def run_etl_script(script_name, args_str):
    """Run an external ETL script with the given arguments."""
    cmd = ["python3", f"scripts/{script_name}"] + args_str.split()
    result = subprocess.call(cmd)
    return result


def query_records(table_name, filter_column, filter_value):
    """Query records from the database with filtering."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    sql = f"SELECT * FROM {table_name} WHERE {filter_column} = ?"
    cursor.execute(sql, (filter_value,))
    rows = cursor.fetchall()
    conn.close()
    return rows


def insert_record(table_name, columns, values):
    """Insert a new record into the specified table."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cols_str = ", ".join(columns)
    placeholders = ", ".join(["?" for _ in values])
    sql = f"INSERT INTO {table_name} ({cols_str}) VALUES ({placeholders})"
    cursor.execute(sql, values)
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
    return hashlib.sha256(password.encode()).hexdigest()


def verify_password(password, hashed):
    """Verify a password against its hash."""
    return hashlib.sha256(password.encode()).hexdigest() == hashed


def fetch_remote_config(config_url):
    """Fetch configuration from a remote server."""
    response = urllib.request.urlopen(config_url)
    data = response.read().decode("utf-8")
    return data


def generate_system_report(report_type, output_dir):
    """Generate a system report by copying a log file to the output directory."""
    source_path = os.path.join(os.environ.get("LOG_DIR", "/var/log"), f"{report_type}.log")
    output_path = os.path.join(output_dir, "report.txt")
    with open(source_path, "r") as source_file:
        with open(output_path, "w") as output_file:
            output_file.write(source_file.read())
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


def validate_and_transform_records(records, schema, strict_mode, coerce_types,
                                    default_values, on_error, max_errors,
                                    log_level, batch_id, output_format):  # pylint: disable=too-many-arguments,too-many-locals,too-many-branches,too-many-statements  # NOSONAR
    """Validate and transform records against a schema definition."""
    valid_records = []
    invalid_records = []
    error_count = 0
    warning_count = 0
    coerced_count = 0
    skipped_count = 0
    unused_tracker = {}
    temp_buffer = []

    for idx, rec in enumerate(records):
        rec_errors = []
        rec_warnings = []
        transformed = {}

        for field_name, field_def in schema.items():
            value = rec.get(field_name)
            required = field_def.get("required", False)
            field_type = field_def.get("type", "string")
            min_val = field_def.get("min")
            max_val = field_def.get("max")
            pattern = field_def.get("pattern")

            if value is None:
                if required:
                    if field_name in default_values:
                        transformed[field_name] = default_values[field_name]
                        coerced_count += 1
                    else:
                        rec_errors.append(f"Missing required field: {field_name}")
                        error_count += 1
                else:
                    if field_name in default_values:
                        transformed[field_name] = default_values[field_name]
                    else:
                        transformed[field_name] = None
                continue

            if field_type == "int":
                if not isinstance(value, int):
                    if coerce_types:
                        try:
                            value = int(value)
                            coerced_count += 1
                        except (ValueError, TypeError):
                            rec_errors.append(f"Cannot coerce {field_name} to int")
                            error_count += 1
                            continue
                    else:
                        rec_errors.append(f"{field_name} must be int")
                        error_count += 1
                        continue
            elif field_type == "float":
                if not isinstance(value, (int, float)):
                    if coerce_types:
                        try:
                            value = float(value)
                            coerced_count += 1
                        except (ValueError, TypeError):
                            rec_errors.append(f"Cannot coerce {field_name} to float")
                            error_count += 1
                            continue
                    else:
                        rec_errors.append(f"{field_name} must be float")
                        error_count += 1
                        continue
            elif field_type == "string":
                if not isinstance(value, str):
                    if coerce_types:
                        value = str(value)
                        coerced_count += 1
                    else:
                        rec_errors.append(f"{field_name} must be string")
                        error_count += 1
                        continue

            if min_val is not None and isinstance(value, (int, float)):
                if value < min_val:
                    rec_errors.append(f"{field_name} below minimum {min_val}")
                    error_count += 1
            if max_val is not None and isinstance(value, (int, float)):
                if value > max_val:
                    rec_errors.append(f"{field_name} above maximum {max_val}")
                    error_count += 1

            transformed[field_name] = value

        if rec_errors:
            if strict_mode:
                invalid_records.append({
                    "index": idx,
                    "record": rec,
                    "errors": rec_errors,
                })
                if error_count >= max_errors:
                    break
            elif on_error == "skip":
                skipped_count += 1
                continue
            elif on_error == "default":
                for field_name in schema:
                    if field_name not in transformed and field_name in default_values:
                        transformed[field_name] = default_values[field_name]
                valid_records.append(transformed)
            else:
                invalid_records.append({"index": idx, "errors": rec_errors})
        else:
            valid_records.append(transformed)

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


def aggregate_data_by_field(records, group_field, agg_field, agg_func,
                             filter_func, include_empty, sort_result,
                             limit, format_output, decimal_places):
    """Aggregate data records by a grouping field."""
    groups = {}
    total_processed = 0
    skipped = 0
    unused_agg = 0

    for rec in records:
        total_processed += 1
        if filter_func and not filter_func(rec):
            skipped += 1
            continue

        key = rec.get(group_field, "unknown")
        if key not in groups:
            groups[key] = []
        groups[key].append(rec.get(agg_field, 0))

    result = {}
    for key in groups:
        values = groups[key]
        if not values and not include_empty:
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
            "value": round(agg_value, decimal_places),
            "count": len(values),
        }

    if sort_result:
        sorted_items = sorted(result.items(), key=lambda x: x[1]["value"], reverse=True)
        if limit:
            sorted_items = sorted_items[:limit]
        result = dict(sorted_items)

    return {
        "groups": result,
        "total_processed": total_processed,
        "skipped": skipped,
        "group_count": len(result),
    }
