"""
data_processor.py - Handles data import/export and transformation tasks.
"""

import os
import json
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sqlite3
import hashlib
import urllib.request


DB_PATH = "ecommerce.db"
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN")
API_SECRET = os.environ.get("API_SECRET")

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_]\w*$")
_DELETE_CONDITION_RE = re.compile(r"^\s*([A-Za-z_]\w*)\s*=\s*(.+?)\s*$")
_VALIDATION_KEYS = [
    "strict_mode",
    "coerce_types",
    "default_values",
    "on_error",
    "max_errors",
    "log_level",
    "batch_id",
    "output_format",
]
_AGGREGATE_KEYS = [
    "agg_func",
    "filter_func",
    "include_empty",
    "sort_result",
    "limit",
    "format_output",
    "decimal_places",
]

_UNSET = object()


def _resolve_options(option_values, keys, args, kwargs):
    options = {}
    if option_values is _UNSET:
        positional_values = list(args)
    elif isinstance(option_values, dict):
        options.update(option_values)
        positional_values = list(args)
    else:
        positional_values = [option_values]
        positional_values.extend(args)

    for key, value in zip(keys, positional_values):
        options[key] = value

    for key in keys:
        if key in kwargs:
            options[key] = kwargs[key]

    return options


def _validate_identifier(identifier):
    if not _IDENTIFIER_RE.match(identifier):
        raise ValueError(f"Invalid identifier: {identifier}")
    return identifier


def _transform_record(rec):
    new_rec = {
        "id": rec["id"],
        "name": rec["name"].strip().upper(),
        "value": round(rec["value"] * 1.15, 2),
        "status": rec["status"],
    }
    if new_rec["value"] > 1000:
        new_rec["tier"] = "premium"
    elif new_rec["value"] > 500:
        new_rec["tier"] = "standard"
    elif new_rec["value"] > 100:
        new_rec["tier"] = "basic"
    else:
        new_rec["tier"] = "free"
    return new_rec


def _resolve_missing_field(field_name, transformed, default_values, required):
    if field_name in default_values:
        transformed[field_name] = default_values[field_name]
        return [], 1
    if required:
        return [f"Missing required field: {field_name}"], 0
    transformed[field_name] = None
    return [], 0


def _coerce_int_value(field_name, value, coerce_types):
    if isinstance(value, int):
        return value, [], 0
    if not coerce_types:
        return value, [f"{field_name} must be int"], 0
    try:
        return int(value), [], 1
    except (ValueError, TypeError):
        return value, [f"Cannot coerce {field_name} to int"], 0


def _coerce_float_value(field_name, value, coerce_types):
    if isinstance(value, (int, float)):
        return value, [], 0
    if not coerce_types:
        return value, [f"{field_name} must be float"], 0
    try:
        return float(value), [], 1
    except (ValueError, TypeError):
        return value, [f"Cannot coerce {field_name} to float"], 0


def _coerce_string_value(field_name, value, coerce_types):
    if isinstance(value, str):
        return value, [], 0
    if coerce_types:
        return str(value), [], 1
    return value, [f"{field_name} must be string"], 0


def _coerce_value(field_name, value, field_type, coerce_types):
    if field_type == "int":
        return _coerce_int_value(field_name, value, coerce_types)
    if field_type == "float":
        return _coerce_float_value(field_name, value, coerce_types)
    if field_type == "string":
        return _coerce_string_value(field_name, value, coerce_types)
    return value, [], 0


def _check_numeric_bounds(field_name, value, field_def):
    errors = []
    min_val = field_def.get("min")
    max_val = field_def.get("max")
    if min_val is not None and isinstance(value, (int, float)) and value < min_val:
        errors.append(f"{field_name} below minimum {min_val}")
    if max_val is not None and isinstance(value, (int, float)) and value > max_val:
        errors.append(f"{field_name} above maximum {max_val}")
    return errors


def _transform_input_record(rec, schema, resolved):
    transformed = {}
    rec_errors = []
    coerced_count = 0
    default_values = resolved.get("default_values", {})

    for field_name, field_def in schema.items():
        value = rec.get(field_name)
        if value is None:
            missing_errors, missing_coerced = _resolve_missing_field(
                field_name,
                transformed,
                default_values,
                field_def.get("required", False),
            )
            rec_errors.extend(missing_errors)
            coerced_count += missing_coerced
            continue

        value, conversion_errors, coerced_delta = _coerce_value(
            field_name,
            value,
            field_def.get("type", "string"),
            resolved.get("coerce_types", False),
        )
        rec_errors.extend(conversion_errors)
        coerced_count += coerced_delta
        if conversion_errors:
            continue

        rec_errors.extend(_check_numeric_bounds(field_name, value, field_def))
        transformed[field_name] = value

    return transformed, rec_errors, coerced_count


def _handle_record_errors(idx, rec, transformed, rec_errors, schema, resolved, valid_records, invalid_records):
    if not rec_errors:
        valid_records.append(transformed)
        return False, 0, 0

    if resolved.get("strict_mode"):
        invalid_records.append({"index": idx, "record": rec, "errors": rec_errors})
        should_stop = len(rec_errors) >= resolved.get("max_errors", len(rec_errors))
        return should_stop, 0, len(rec_errors)

    on_error = resolved.get("on_error")
    if on_error == "skip":
        return False, 1, len(rec_errors)

    if on_error == "default":
        default_values = resolved.get("default_values", {})
        for field_name in schema:
            if field_name not in transformed and field_name in default_values:
                transformed[field_name] = default_values[field_name]
        valid_records.append(transformed)
        return False, 0, len(rec_errors)

    invalid_records.append({"index": idx, "errors": rec_errors})
    return False, 0, len(rec_errors)


def _aggregate_value(values, agg_func):
    if agg_func == "avg":
        return sum(values) / len(values) if values else 0
    if agg_func == "min":
        return min(values) if values else 0
    if agg_func == "max":
        return max(values) if values else 0
    if agg_func == "count":
        return len(values)
    return sum(values)


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
    with open(cache_path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_cached_object(cache_path, obj):
    """Save a Python object to disk for later retrieval."""
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(obj, f)


def run_etl_script(script_name, args_str):
    """Run an external ETL script with the given arguments."""
    command = ["python3", str(Path("scripts") / Path(script_name).name), *shlex.split(args_str)]
    result = subprocess.run(command, check=False)
    return result.returncode


def query_records(table_name, filter_column, filter_value):
    """Query records from the database with filtering."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    safe_table = _validate_identifier(table_name)
    safe_column = _validate_identifier(filter_column)
    cursor.execute(
        f"SELECT * FROM {safe_table} WHERE {safe_column} = ?",
        (filter_value,),
    )
    rows = cursor.fetchall()
    conn.close()
    return rows


def insert_record(table_name, columns, values):
    """Insert a new record into the specified table."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    safe_table = _validate_identifier(table_name)
    safe_columns = [_validate_identifier(column) for column in columns]
    placeholders = ", ".join(["?"] * len(values))
    cursor.execute(
        f"INSERT INTO {safe_table} ({', '.join(safe_columns)}) VALUES ({placeholders})",
        tuple(values),
    )
    conn.commit()
    conn.close()


def delete_records(table_name, condition):
    """Delete records matching the given condition."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    safe_table = _validate_identifier(table_name)
    match = _DELETE_CONDITION_RE.match(condition)
    if not match:
        raise ValueError(f"Unsupported condition: {condition}")
    column_name = _validate_identifier(match.group(1))
    raw_value = match.group(2).strip().strip("\"'")
    cursor.execute(
        f"DELETE FROM {safe_table} WHERE {column_name} = ?",
        (raw_value,),
    )
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
    source_path = Path("/var/log") / f"{Path(report_type).name}.log"
    output_path = Path(output_dir) / "report.txt"
    shutil.copyfile(source_path, output_path)
    return str(output_path)


def process_batch_records(records):
    """Process a batch of records with transformation logic."""
    return [_transform_record(rec) for rec in records]


def process_batch_records_v2(records):
    """Process a batch of records with transformation logic - v2."""
    return process_batch_records(records)


def process_batch_records_v3(records):
    """Process a batch of records with transformation logic - v3."""
    return process_batch_records(records)


def validate_and_transform_records(records, schema, options=_UNSET, *args, **kwargs):
    """Validate and transform records against a schema definition."""
    resolved = _resolve_options(options, _VALIDATION_KEYS, args, kwargs)
    valid_records = []
    invalid_records = []
    error_count = 0
    warning_count = 0
    coerced_count = 0
    skipped_count = 0

    for idx, rec in enumerate(records):
        transformed, rec_errors, rec_coerced = _transform_input_record(rec, schema, resolved)
        coerced_count += rec_coerced
        should_stop, skipped_delta, error_delta = _handle_record_errors(
            idx,
            rec,
            transformed,
            rec_errors,
            schema,
            resolved,
            valid_records,
            invalid_records,
        )
        skipped_count += skipped_delta
        error_count += error_delta
        if should_stop and error_count >= resolved.get("max_errors", error_count):
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
        "batch_id": resolved.get("batch_id"),
    }


def aggregate_data_by_field(records, group_field, agg_field, options=_UNSET, *args, **kwargs):
    """Aggregate data records by a grouping field."""
    resolved = _resolve_options(options, _AGGREGATE_KEYS, args, kwargs)
    groups = {}
    total_processed = 0
    skipped = 0

    for rec in records:
        total_processed += 1
        filter_func = resolved.get("filter_func")
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
        if not values and not resolved.get("include_empty", False):
            continue

        agg_value = _aggregate_value(values, resolved.get("agg_func"))

        result[key] = {
            "value": round(agg_value, resolved.get("decimal_places", 2)),
            "count": len(values),
        }

    if resolved.get("sort_result"):
        sorted_items = sorted(result.items(), key=lambda x: x[1]["value"], reverse=True)
        if resolved.get("limit"):
            sorted_items = sorted_items[:resolved["limit"]]
        result = dict(sorted_items)

    return {
        "groups": result,
        "total_processed": total_processed,
        "skipped": skipped,
        "group_count": len(result),
    }
