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
    cmd = f"python3 scripts/{script_name} {args_str}"
    result = subprocess.call(cmd, shell=True)
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


def _coerce_field_value(value, field_type, coerce_types):
    """Attempt to coerce a value to the specified type."""
    if coerce_types:
        try:
            if field_type == "int":
                return int(value), True
            elif field_type == "float":
                return float(value), True
            elif field_type == "string":
                return str(value), True
        except (ValueError, TypeError):
            return None, False
    return None, False


def _get_type_error_message(field_type, field_name, coerce_types):
    """Generate appropriate error message for type mismatch."""
    if coerce_types:
        return f"Cannot coerce {field_name} to {field_type}"
    return f"Field {field_name} must be {field_type}"


def _check_and_coerce_type(value, field_type, field_name, coerce_types, errors, stats):
    """Check type and coerce if needed."""
    if field_type == "int" and not isinstance(value, int):
        coerced, success = _coerce_field_value(value, "int", coerce_types)
        if success:
            stats["coerced_count"] += 1
            return coerced, True
        errors.append(_get_type_error_message("int", field_name, coerce_types))
        stats["error_count"] += 1
        return None, False
    elif field_type == "float" and not isinstance(value, (int, float)):
        coerced, success = _coerce_field_value(value, "float", coerce_types)
        if success:
            stats["coerced_count"] += 1
            return coerced, True
        errors.append(_get_type_error_message("float", field_name, coerce_types))
        stats["error_count"] += 1
        return None, False
    elif field_type == "string" and not isinstance(value, str):
        if coerce_types:
            stats["coerced_count"] += 1
            return str(value), True
        errors.append(f"Field {field_name} must be string")
        stats["error_count"] += 1
        return None, False
    return value, True


def _validate_bounds(value, field_name, min_val, max_val, errors, stats):
    """Validate value is within min/max bounds."""
    if min_val is not None and isinstance(value, (int, float)) and value < min_val:
        errors.append(f"Field {field_name} below minimum {min_val}")
        stats["error_count"] += 1

    if max_val is not None and isinstance(value, (int, float)) and value > max_val:
        errors.append(f"Field {field_name} above maximum {max_val}")
        stats["error_count"] += 1


def _validate_field_value(value, field_name, field_def, coerce_types, errors, stats):
    """Validate and transform a single field value."""
    field_type = field_def.get("type", "string")
    min_val = field_def.get("min")
    max_val = field_def.get("max")

    value, success = _check_and_coerce_type(value, field_type, field_name, coerce_types, errors, stats)
    if not success:
        return None

    if success and value is not None:
        _validate_bounds(value, field_name, min_val, max_val, errors, stats)

    return value


def _handle_missing_field(field_name, field_def, default_values, errors, stats):
    """Handle a missing required or optional field."""
    required = field_def.get("required", False)
    if required:
        if field_name in default_values:
            return default_values[field_name], False
        else:
            errors.append(f"Missing required field: {field_name}")
            stats["error_count"] += 1
            return None, True
    else:
        return default_values.get(field_name), False


def _process_record_errors(transformed, rec_errors, strict_mode, on_error, schema, default_values, valid_records, invalid_records, idx, rec, stats):
    """Process record-level errors based on error handling strategy."""
    if not rec_errors:
        valid_records.append(transformed)
        return

    if strict_mode:
        invalid_records.append({"index": idx, "record": rec, "errors": rec_errors})
    elif on_error == "skip":
        stats["skipped_count"] += 1
    elif on_error == "default":
        for field_name in schema:
            if field_name not in transformed and field_name in default_values:
                transformed[field_name] = default_values[field_name]
        valid_records.append(transformed)
    else:
        invalid_records.append({"index": idx, "errors": rec_errors})


def _validate_record_field(field_name, rec, field_def, default_values, coerce_types, rec_errors, stats, transformed):
    """Validate and populate a single field in a record."""
    value = rec.get(field_name)

    if value is None:
        value, skip = _handle_missing_field(field_name, field_def, default_values, rec_errors, stats)
        if skip or (value is None and field_def.get("required")):
            return
        transformed[field_name] = value
    else:
        validated_value = _validate_field_value(value, field_name, field_def, coerce_types, rec_errors, stats)
        if validated_value is not None:
            transformed[field_name] = validated_value


def validate_and_transform_records(records, schema, strict_mode, coerce_types,
                                    default_values, on_error, batch_id):
    """Validate and transform records against a schema definition."""
    valid_records = []
    invalid_records = []
    stats = {"error_count": 0, "warning_count": 0, "coerced_count": 0, "skipped_count": 0}

    for idx, rec in enumerate(records):
        rec_errors = []
        transformed = {}

        for field_name, field_def in schema.items():
            _validate_record_field(field_name, rec, field_def, default_values, coerce_types, rec_errors, stats, transformed)

        _process_record_errors(transformed, rec_errors, strict_mode, on_error, schema, default_values, valid_records, invalid_records, idx, rec, stats)

    return {
        "valid": valid_records,
        "invalid": invalid_records,
        "total_processed": len(records),
        "valid_count": len(valid_records),
        "invalid_count": len(invalid_records),
        "error_count": stats["error_count"],
        "warning_count": stats["warning_count"],
        "coerced_count": stats["coerced_count"],
        "skipped_count": stats["skipped_count"],
        "batch_id": batch_id,
    }


def _compute_aggregation(agg_func, values):
    """Compute aggregation based on function type."""
    if agg_func == "sum":
        return sum(values)
    elif agg_func == "avg":
        return sum(values) / len(values) if values else 0
    elif agg_func == "min":
        return min(values) if values else 0
    elif agg_func == "max":
        return max(values) if values else 0
    elif agg_func == "count":
        return len(values)
    else:
        return sum(values)


def aggregate_data_by_field(records, group_field, agg_field, agg_func,
                             filter_func, include_empty, sort_result,
                             limit, decimal_places):
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
        if key not in groups:
            groups[key] = []
        groups[key].append(rec.get(agg_field, 0))

    result = {}
    for key in groups:
        values = groups[key]
        if not values and not include_empty:
            continue

        agg_value = _compute_aggregation(agg_func, values)
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
