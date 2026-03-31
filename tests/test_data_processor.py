import pytest
from unittest.mock import patch, MagicMock, mock_open
import data_processor


class TestImportDataFromFile:
    def test_import_basic(self, tmp_path):
        f = tmp_path / "data.csv"
        f.write_text("1,Alice,100.5,active\n2,Bob,200.0,inactive\n")
        records = data_processor.import_data_from_file(str(f))
        assert len(records) == 2
        assert records[0] == {"id": 1, "name": "Alice", "value": 100.5, "status": "active"}
        assert records[1]["status"] == "inactive"

    def test_import_default_status(self, tmp_path):
        f = tmp_path / "data.csv"
        f.write_text("1,Alice,50.0\n")
        records = data_processor.import_data_from_file(str(f))
        assert records[0]["status"] == "active"


class TestExportDataToFile:
    def test_export(self, tmp_path):
        f = tmp_path / "out.csv"
        records = [
            {"id": 1, "name": "A", "value": 10.0, "status": "active"},
            {"id": 2, "name": "B", "value": 20.0, "status": "inactive"},
        ]
        count = data_processor.export_data_to_file(str(f), records)
        assert count == 2
        content = f.read_text()
        assert "1,A,10.0,active" in content
        assert "2,B,20.0,inactive" in content


class TestHashUserPassword:
    def test_hash(self):
        h = data_processor.hash_user_password("secret")
        assert isinstance(h, str)
        assert len(h) == 32

    def test_same_password_same_hash(self):
        h1 = data_processor.hash_user_password("test")
        h2 = data_processor.hash_user_password("test")
        assert h1 == h2

    def test_different_passwords_different_hashes(self):
        h1 = data_processor.hash_user_password("a")
        h2 = data_processor.hash_user_password("b")
        assert h1 != h2


class TestVerifyPassword:
    def test_correct_password(self):
        h = data_processor.hash_user_password("mypass")
        assert data_processor.verify_password("mypass", h) is True

    def test_wrong_password(self):
        h = data_processor.hash_user_password("mypass")
        assert data_processor.verify_password("wrong", h) is False


class TestProcessBatchRecords:
    def test_empty(self):
        assert data_processor.process_batch_records([]) == []

    def test_free_tier(self):
        records = [{"id": 1, "name": " alice ", "value": 50.0, "status": "active"}]
        result = data_processor.process_batch_records(records)
        assert result[0]["name"] == "ALICE"
        assert result[0]["value"] == round(50.0 * 1.15, 2)
        assert result[0]["tier"] == "free"

    def test_basic_tier(self):
        records = [{"id": 1, "name": "x", "value": 100.0, "status": "active"}]
        result = data_processor.process_batch_records(records)
        assert result[0]["tier"] == "basic"

    def test_standard_tier(self):
        records = [{"id": 1, "name": "x", "value": 500.0, "status": "active"}]
        result = data_processor.process_batch_records(records)
        assert result[0]["tier"] == "standard"

    def test_premium_tier(self):
        records = [{"id": 1, "name": "x", "value": 1000.0, "status": "active"}]
        result = data_processor.process_batch_records(records)
        assert result[0]["tier"] == "premium"


class TestProcessBatchRecordsV2:
    def test_processes_same_as_v1(self):
        records = [{"id": 1, "name": "test", "value": 200.0, "status": "active"}]
        v1 = data_processor.process_batch_records(records)
        v2 = data_processor.process_batch_records_v2(records)
        assert v1 == v2


class TestProcessBatchRecordsV3:
    def test_processes_same_as_v1(self):
        records = [{"id": 1, "name": "test", "value": 200.0, "status": "active"}]
        v1 = data_processor.process_batch_records(records)
        v3 = data_processor.process_batch_records_v3(records)
        assert v1 == v3


class TestQueryRecords:
    @patch("sqlite3.connect")
    def test_query(self, mock_connect):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [(1, "test")]
        mock_connect.return_value.cursor.return_value = mock_cursor
        result = data_processor.query_records("products", "name", "test")
        assert result == [(1, "test")]
        mock_connect.return_value.close.assert_called_once()


class TestInsertRecord:
    @patch("sqlite3.connect")
    def test_insert(self, mock_connect):
        mock_cursor = MagicMock()
        mock_connect.return_value.cursor.return_value = mock_cursor
        data_processor.insert_record("products", ["name", "price"], ["Widget", 9.99])
        mock_cursor.execute.assert_called_once()
        mock_connect.return_value.commit.assert_called_once()


class TestDeleteRecords:
    @patch("sqlite3.connect")
    def test_delete(self, mock_connect):
        mock_cursor = MagicMock()
        mock_connect.return_value.cursor.return_value = mock_cursor
        data_processor.delete_records("products", "id = 1")
        mock_cursor.execute.assert_called_once()
        mock_connect.return_value.commit.assert_called_once()


class TestRunEtlScript:
    @patch("subprocess.call")
    def test_run(self, mock_call):
        mock_call.return_value = 0
        result = data_processor.run_etl_script("import.py", "--full")
        assert result == 0
        mock_call.assert_called_once()


class TestLoadCachedObject:
    @patch("builtins.open", mock_open(read_data=b""))
    @patch("pickle.loads")
    def test_load(self, mock_pickle):
        mock_pickle.return_value = {"key": "value"}
        result = data_processor.load_cached_object("/tmp/cache.pkl")
        assert result == {"key": "value"}


class TestSaveCachedObject:
    @patch("pickle.dump")
    def test_save(self, mock_dump):
        m = mock_open()
        with patch("builtins.open", m):
            data_processor.save_cached_object("/tmp/cache.pkl", {"key": "value"})
        mock_dump.assert_called_once()


class TestFetchRemoteConfig:
    @patch("urllib.request.urlopen")
    def test_fetch(self, mock_urlopen):
        mock_response = MagicMock()
        mock_response.read.return_value = b'{"debug": true}'
        mock_urlopen.return_value = mock_response
        result = data_processor.fetch_remote_config("http://example.com/config")
        assert result == '{"debug": true}'


class TestGenerateSystemReport:
    @patch("os.system")
    def test_generate(self, mock_system):
        mock_system.return_value = 0
        result = data_processor.generate_system_report("access", "/tmp")
        assert result == "/tmp/report.txt"


class TestValidateAndTransformRecords:
    def test_valid_records(self):
        records = [{"name": "Alice", "age": 30}]
        schema = {
            "name": {"type": "string", "required": True},
            "age": {"type": "int", "required": True},
        }
        result = data_processor.validate_and_transform_records(
            records, schema, strict_mode=False, coerce_types=False,
            default_values={}, on_error="skip", max_errors=10,
            log_level="info", batch_id="b1", output_format="json",
        )
        assert result["valid_count"] == 1
        assert result["invalid_count"] == 0

    def test_missing_required_strict(self):
        records = [{"name": "Alice"}]
        schema = {
            "name": {"type": "string", "required": True},
            "age": {"type": "int", "required": True},
        }
        result = data_processor.validate_and_transform_records(
            records, schema, strict_mode=True, coerce_types=False,
            default_values={}, on_error="skip", max_errors=10,
            log_level="info", batch_id="b1", output_format="json",
        )
        assert result["invalid_count"] == 1
        assert result["valid_count"] == 0

    def test_coerce_types(self):
        records = [{"name": "Bob", "age": "25"}]
        schema = {
            "name": {"type": "string", "required": True},
            "age": {"type": "int", "required": True},
        }
        result = data_processor.validate_and_transform_records(
            records, schema, strict_mode=False, coerce_types=True,
            default_values={}, on_error="skip", max_errors=10,
            log_level="info", batch_id="b1", output_format="json",
        )
        assert result["valid_count"] == 1
        assert result["coerced_count"] == 1

    def test_default_values_for_missing_required(self):
        records = [{"name": "Alice"}]
        schema = {
            "name": {"type": "string", "required": True},
            "age": {"type": "int", "required": True},
        }
        result = data_processor.validate_and_transform_records(
            records, schema, strict_mode=False, coerce_types=False,
            default_values={"age": 0}, on_error="skip", max_errors=10,
            log_level="info", batch_id="b1", output_format="json",
        )
        assert result["valid_count"] == 1

    def test_on_error_skip(self):
        records = [{"name": 123, "age": 30}]
        schema = {
            "name": {"type": "string", "required": True},
            "age": {"type": "int", "required": True},
        }
        result = data_processor.validate_and_transform_records(
            records, schema, strict_mode=False, coerce_types=False,
            default_values={}, on_error="skip", max_errors=10,
            log_level="info", batch_id="b1", output_format="json",
        )
        assert result["skipped_count"] == 1

    def test_min_max_validation(self):
        records = [{"score": 150}]
        schema = {
            "score": {"type": "int", "required": True, "min": 0, "max": 100},
        }
        result = data_processor.validate_and_transform_records(
            records, schema, strict_mode=True, coerce_types=False,
            default_values={}, on_error="skip", max_errors=10,
            log_level="info", batch_id="b1", output_format="json",
        )
        assert result["error_count"] >= 1

    def test_float_coercion(self):
        records = [{"price": "19.99"}]
        schema = {
            "price": {"type": "float", "required": True},
        }
        result = data_processor.validate_and_transform_records(
            records, schema, strict_mode=False, coerce_types=True,
            default_values={}, on_error="skip", max_errors=10,
            log_level="info", batch_id="b1", output_format="json",
        )
        assert result["valid_count"] == 1
        assert result["coerced_count"] == 1

    def test_optional_field_with_default(self):
        records = [{}]
        schema = {
            "name": {"type": "string", "required": False},
        }
        result = data_processor.validate_and_transform_records(
            records, schema, strict_mode=False, coerce_types=False,
            default_values={"name": "unknown"}, on_error="skip", max_errors=10,
            log_level="info", batch_id="b1", output_format="json",
        )
        assert result["valid_count"] == 1
        assert result["valid"][0]["name"] == "unknown"


class TestAggregateDataByField:
    def test_sum_aggregation(self):
        records = [
            {"category": "A", "value": 10},
            {"category": "A", "value": 20},
            {"category": "B", "value": 5},
        ]
        result = data_processor.aggregate_data_by_field(
            records, "category", "value", "sum",
            filter_func=None, include_empty=True, sort_result=False,
            limit=None, format_output="json", decimal_places=2,
        )
        assert result["groups"]["A"]["value"] == 30
        assert result["groups"]["B"]["value"] == 5
        assert result["group_count"] == 2

    def test_avg_aggregation(self):
        records = [
            {"category": "A", "value": 10},
            {"category": "A", "value": 20},
        ]
        result = data_processor.aggregate_data_by_field(
            records, "category", "value", "avg",
            filter_func=None, include_empty=True, sort_result=False,
            limit=None, format_output="json", decimal_places=2,
        )
        assert result["groups"]["A"]["value"] == 15.0

    def test_count_aggregation(self):
        records = [
            {"category": "A", "value": 10},
            {"category": "A", "value": 20},
        ]
        result = data_processor.aggregate_data_by_field(
            records, "category", "value", "count",
            filter_func=None, include_empty=True, sort_result=False,
            limit=None, format_output="json", decimal_places=2,
        )
        assert result["groups"]["A"]["value"] == 2

    def test_min_max_aggregation(self):
        records = [
            {"category": "A", "value": 10},
            {"category": "A", "value": 20},
        ]
        r_min = data_processor.aggregate_data_by_field(
            records, "category", "value", "min",
            filter_func=None, include_empty=True, sort_result=False,
            limit=None, format_output="json", decimal_places=2,
        )
        r_max = data_processor.aggregate_data_by_field(
            records, "category", "value", "max",
            filter_func=None, include_empty=True, sort_result=False,
            limit=None, format_output="json", decimal_places=2,
        )
        assert r_min["groups"]["A"]["value"] == 10
        assert r_max["groups"]["A"]["value"] == 20

    def test_with_filter(self):
        records = [
            {"category": "A", "value": 10},
            {"category": "A", "value": -5},
        ]
        result = data_processor.aggregate_data_by_field(
            records, "category", "value", "sum",
            filter_func=lambda r: r["value"] > 0, include_empty=True,
            sort_result=False, limit=None, format_output="json", decimal_places=2,
        )
        assert result["groups"]["A"]["value"] == 10
        assert result["skipped"] == 1


class TestProcessBatchRecordsV2Tiers:
    def test_premium_tier(self):
        records = [{"id": 1, "name": "x", "value": 1000.0, "status": "active"}]
        result = data_processor.process_batch_records_v2(records)
        assert result[0]["tier"] == "premium"

    def test_standard_tier(self):
        records = [{"id": 1, "name": "x", "value": 500.0, "status": "active"}]
        result = data_processor.process_batch_records_v2(records)
        assert result[0]["tier"] == "standard"

    def test_basic_tier(self):
        records = [{"id": 1, "name": "x", "value": 100.0, "status": "active"}]
        result = data_processor.process_batch_records_v2(records)
        assert result[0]["tier"] == "basic"

    def test_free_tier(self):
        records = [{"id": 1, "name": "x", "value": 50.0, "status": "active"}]
        result = data_processor.process_batch_records_v2(records)
        assert result[0]["tier"] == "free"


class TestProcessBatchRecordsV3Tiers:
    def test_premium_tier(self):
        records = [{"id": 1, "name": "x", "value": 1000.0, "status": "active"}]
        result = data_processor.process_batch_records_v3(records)
        assert result[0]["tier"] == "premium"

    def test_standard_tier(self):
        records = [{"id": 1, "name": "x", "value": 500.0, "status": "active"}]
        result = data_processor.process_batch_records_v3(records)
        assert result[0]["tier"] == "standard"

    def test_basic_tier(self):
        records = [{"id": 1, "name": "x", "value": 100.0, "status": "active"}]
        result = data_processor.process_batch_records_v3(records)
        assert result[0]["tier"] == "basic"

    def test_free_tier(self):
        records = [{"id": 1, "name": "x", "value": 50.0, "status": "active"}]
        result = data_processor.process_batch_records_v3(records)
        assert result[0]["tier"] == "free"


class TestValidateAndTransformEdgeCases:
    def test_on_error_default(self):
        records = [{"name": 123, "age": 30}]
        schema = {
            "name": {"type": "string", "required": True},
            "age": {"type": "int", "required": True},
        }
        result = data_processor.validate_and_transform_records(
            records, schema, strict_mode=False, coerce_types=False,
            default_values={"name": "fallback"}, on_error="default", max_errors=10,
            log_level="info", batch_id="b1", output_format="json",
        )
        assert result["valid_count"] == 1

    def test_on_error_other(self):
        records = [{"name": 123}]
        schema = {
            "name": {"type": "string", "required": True},
        }
        result = data_processor.validate_and_transform_records(
            records, schema, strict_mode=False, coerce_types=False,
            default_values={}, on_error="log", max_errors=10,
            log_level="info", batch_id="b1", output_format="json",
        )
        assert result["invalid_count"] == 1

    def test_coerce_int_failure(self):
        records = [{"count": "abc"}]
        schema = {
            "count": {"type": "int", "required": True},
        }
        result = data_processor.validate_and_transform_records(
            records, schema, strict_mode=True, coerce_types=True,
            default_values={}, on_error="skip", max_errors=10,
            log_level="info", batch_id="b1", output_format="json",
        )
        assert result["error_count"] >= 1

    def test_coerce_float_failure(self):
        records = [{"price": "abc"}]
        schema = {
            "price": {"type": "float", "required": True},
        }
        result = data_processor.validate_and_transform_records(
            records, schema, strict_mode=True, coerce_types=True,
            default_values={}, on_error="skip", max_errors=10,
            log_level="info", batch_id="b1", output_format="json",
        )
        assert result["error_count"] >= 1

    def test_float_type_no_coerce(self):
        records = [{"price": "19.99"}]
        schema = {
            "price": {"type": "float", "required": True},
        }
        result = data_processor.validate_and_transform_records(
            records, schema, strict_mode=False, coerce_types=False,
            default_values={}, on_error="skip", max_errors=10,
            log_level="info", batch_id="b1", output_format="json",
        )
        assert result["skipped_count"] == 1

    def test_string_coerce(self):
        records = [{"name": 42}]
        schema = {
            "name": {"type": "string", "required": True},
        }
        result = data_processor.validate_and_transform_records(
            records, schema, strict_mode=False, coerce_types=True,
            default_values={}, on_error="skip", max_errors=10,
            log_level="info", batch_id="b1", output_format="json",
        )
        assert result["valid_count"] == 1
        assert result["coerced_count"] == 1
        assert result["valid"][0]["name"] == "42"

    def test_int_type_no_coerce(self):
        records = [{"age": "25"}]
        schema = {
            "age": {"type": "int", "required": True},
        }
        result = data_processor.validate_and_transform_records(
            records, schema, strict_mode=False, coerce_types=False,
            default_values={}, on_error="skip", max_errors=10,
            log_level="info", batch_id="b1", output_format="json",
        )
        assert result["skipped_count"] == 1

    def test_optional_none_value(self):
        records = [{}]
        schema = {
            "notes": {"type": "string", "required": False},
        }
        result = data_processor.validate_and_transform_records(
            records, schema, strict_mode=False, coerce_types=False,
            default_values={}, on_error="skip", max_errors=10,
            log_level="info", batch_id="b1", output_format="json",
        )
        assert result["valid_count"] == 1
        assert result["valid"][0]["notes"] is None

    def test_strict_max_errors_break(self):
        records = [{"x": "a"}, {"x": "b"}, {"x": "c"}]
        schema = {
            "x": {"type": "int", "required": True},
        }
        result = data_processor.validate_and_transform_records(
            records, schema, strict_mode=True, coerce_types=False,
            default_values={}, on_error="skip", max_errors=1,
            log_level="info", batch_id="b1", output_format="json",
        )
        assert result["invalid_count"] >= 1

    def test_min_validation(self):
        records = [{"score": -5}]
        schema = {
            "score": {"type": "int", "required": True, "min": 0},
        }
        result = data_processor.validate_and_transform_records(
            records, schema, strict_mode=True, coerce_types=False,
            default_values={}, on_error="skip", max_errors=10,
            log_level="info", batch_id="b1", output_format="json",
        )
        assert any("below minimum" in e for e in result["invalid"][0]["errors"])


class TestAggregateUnknownFunc:
    def test_unknown_agg_defaults_to_sum(self):
        records = [{"cat": "A", "val": 10}, {"cat": "A", "val": 20}]
        result = data_processor.aggregate_data_by_field(
            records, "cat", "val", "unknown",
            filter_func=None, include_empty=True, sort_result=False,
            limit=None, format_output="json", decimal_places=2,
        )
        assert result["groups"]["A"]["value"] == 30

    def test_sorted_with_limit(self):
        records = [
            {"category": "A", "value": 100},
            {"category": "B", "value": 200},
            {"category": "C", "value": 50},
        ]
        result = data_processor.aggregate_data_by_field(
            records, "category", "value", "sum",
            filter_func=None, include_empty=True, sort_result=True,
            limit=2, format_output="json", decimal_places=2,
        )
        assert result["group_count"] == 2
        keys = list(result["groups"].keys())
        assert keys[0] == "B"
