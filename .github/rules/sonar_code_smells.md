# SonarCloud Code Smell Report

> **Project:** `zzonni_technical-debt-demo`  
> **Organization:** `zzonni`  
> **Generated:** 2026-04-01 17:34 UTC  
> **Total Issues:** 201

---

## 📊 Summary by Severity

| Severity | Count |
|----------|-------|
| 🔴 BLOCKER | 0 |
| 🟠 CRITICAL | 57 |
| 🟡 MAJOR | 102 |
| 🔵 MINOR | 42 |
| ⚪ INFO | 0 |

## 🏆 Top Violated Rules

| # | Rule Key | Violations | Description |
|---|----------|-----------|-------------|
| 1 | `python:S1172` | 63 | — |
| 2 | `python:S1481` | 39 | — |
| 3 | `python:S1066` | 27 | — |
| 4 | `python:S6903` | 24 | — |
| 5 | `python:S3776` | 24 | — |
| 6 | `python:S1192` | 9 | — |
| 7 | `python:S1871` | 5 | — |
| 8 | `python:S6965` | 3 | — |
| 9 | `python:S108` | 2 | — |
| 10 | `python:S117` | 2 | — |
| 11 | `python:S3626` | 1 | — |
| 12 | `python:S3358` | 1 | — |
| 13 | `css:S7924` | 1 | — |

## 📁 Most Affected Files

| # | File | Issues |
|---|------|--------|
| 1 | `inventory_manager.py` | 45 |
| 2 | `report_generator.py` | 27 |
| 3 | `storage.py` | 23 |
| 4 | `task_scheduler.py` | 23 |
| 5 | `user_manager.py` | 20 |
| 6 | `models.py` | 17 |
| 7 | `admin_panel.py` | 14 |
| 8 | `utils.py` | 12 |
| 9 | `data_processor.py` | 12 |
| 10 | `app.py` | 3 |
| 11 | `src/payment_gateway.py` | 2 |
| 12 | `auth/__init__.py` | 1 |
| 13 | `services/email.py` | 1 |
| 14 | `static/style.css` | 1 |

---

## 🔍 Issues by Rule

> Each section covers one violated rule with its description and all affected locations.

### `python:S1172` — Unknown Rule

| Property | Value |
|----------|-------|
| **Language** | — |
| **Type** | CODE_SMELL |
| **Tags** | — |
| **Violations** | 63 |

**Affected Locations:**

| Severity | File | Line | Message | Effort |
|----------|------|------|---------|--------|
| 🟡 MAJOR | `admin_panel.py` | 204 | Remove the unused function parameter "severity_filter". | 5min |
| 🟡 MAJOR | `admin_panel.py` | 205 | Remove the unused function parameter "export_format". | 5min |
| 🟡 MAJOR | `admin_panel.py` | 264 | Remove the unused function parameter "reason". | 5min |
| 🟡 MAJOR | `admin_panel.py` | 265 | Remove the unused function parameter "effective_date". | 5min |
| 🟡 MAJOR | `admin_panel.py` | 265 | Remove the unused function parameter "expiry_date". | 5min |
| 🟡 MAJOR | `admin_panel.py` | 265 | Remove the unused function parameter "notify_user". | 5min |
| 🟡 MAJOR | `admin_panel.py` | 266 | Remove the unused function parameter "require_mfa". | 5min |
| 🟡 MAJOR | `admin_panel.py` | 266 | Remove the unused function parameter "ip_whitelist". | 5min |
| 🟡 MAJOR | `data_processor.py` | 185 | Remove the unused function parameter "log_level". | 5min |
| 🟡 MAJOR | `data_processor.py` | 185 | Remove the unused function parameter "output_format". | 5min |
| 🟡 MAJOR | `data_processor.py` | 311 | Remove the unused function parameter "format_output". | 5min |
| 🟡 MAJOR | `inventory_manager.py` | 105 | Remove the unused function parameter "notify_warehouse". | 5min |
| 🟡 MAJOR | `inventory_manager.py` | 106 | Remove the unused function parameter "priority". | 5min |
| 🟡 MAJOR | `inventory_manager.py` | 106 | Remove the unused function parameter "notes". | 5min |
| 🟡 MAJOR | `inventory_manager.py` | 213 | Remove the unused function parameter "min_priority". | 5min |
| 🟡 MAJOR | `inventory_manager.py` | 215 | Remove the unused function parameter "format_type". | 5min |
| 🟡 MAJOR | `inventory_manager.py` | 216 | Remove the unused function parameter "notification_list". | 5min |
| 🟡 MAJOR | `inventory_manager.py` | 268 | Remove the unused function parameter "output_format". | 5min |
| 🟡 MAJOR | `inventory_manager.py` | 335 | Remove the unused function parameter "log_discrepancies". | 5min |
| 🟡 MAJOR | `inventory_manager.py` | 335 | Remove the unused function parameter "notify_manager". | 5min |
| 🟡 MAJOR | `inventory_manager.py` | 431 | Remove the unused function parameter "date_range_start". | 5min |
| 🟡 MAJOR | `inventory_manager.py` | 432 | Remove the unused function parameter "date_range_end". | 5min |
| 🟡 MAJOR | `inventory_manager.py` | 433 | Remove the unused function parameter "group_by". | 5min |
| 🟡 MAJOR | `inventory_manager.py` | 489 | Remove the unused function parameter "include_incoming". | 5min |
| 🟡 MAJOR | `inventory_manager.py` | 489 | Remove the unused function parameter "include_reserved". | 5min |
| 🟡 MAJOR | `inventory_manager.py` | 490 | Remove the unused function parameter "unit_type". | 5min |
| 🟡 MAJOR | `inventory_manager.py` | 491 | Remove the unused function parameter "detail_level". | 5min |
| 🟡 MAJOR | `inventory_manager.py` | 545 | Remove the unused function parameter "sync_descriptions". | 5min |
| 🟡 MAJOR | `inventory_manager.py` | 546 | Remove the unused function parameter "log_changes". | 5min |
| 🟡 MAJOR | `inventory_manager.py` | 547 | Remove the unused function parameter "timeout". | 5min |
| 🟡 MAJOR | `models.py` | 49 | Remove the unused function parameter "auto_assign". | 5min |
| 🟡 MAJOR | `models.py` | 49 | Remove the unused function parameter "notify". | 5min |
| 🟡 MAJOR | `report_generator.py` | 169 | Remove the unused function parameter "comparison_type". | 5min |
| 🟡 MAJOR | `report_generator.py` | 170 | Remove the unused function parameter "highlight_diffs". | 5min |
| 🟡 MAJOR | `report_generator.py` | 170 | Remove the unused function parameter "format_output". | 5min |
| 🟡 MAJOR | `report_generator.py` | 171 | Remove the unused function parameter "verbose". | 5min |
| 🟡 MAJOR | `report_generator.py` | 250 | Remove the unused function parameter "filters". | 5min |
| 🟡 MAJOR | `report_generator.py` | 251 | Remove the unused function parameter "output_format". | 5min |
| 🟡 MAJOR | `report_generator.py` | 252 | Remove the unused function parameter "label_format". | 5min |
| 🟡 MAJOR | `report_generator.py` | 252 | Remove the unused function parameter "weight_field". | 5min |
| 🟡 MAJOR | `report_generator.py` | 319 | Remove the unused function parameter "output_format". | 5min |
| 🟡 MAJOR | `report_generator.py` | 319 | Remove the unused function parameter "confidence_level". | 5min |
| 🟡 MAJOR | `report_generator.py` | 320 | Remove the unused function parameter "annotations". | 5min |
| 🟡 MAJOR | `services/email.py` | 4 | Remove the unused function parameter "body". | 5min |
| 🟡 MAJOR | `src/payment_gateway.py` | 3 | Remove the unused function parameter "cvv". | 5min |
| 🟡 MAJOR | `storage.py` | 79 | Remove the unused function parameter "notify". | 5min |
| 🟡 MAJOR | `storage.py` | 227 | Remove the unused function parameter "include_metadata". | 5min |
| 🟡 MAJOR | `storage.py` | 228 | Remove the unused function parameter "date_format". | 5min |
| 🟡 MAJOR | `task_scheduler.py` | 51 | Remove the unused function parameter "include_metadata". | 5min |
| 🟡 MAJOR | `task_scheduler.py` | 73 | Remove the unused function parameter "max_workers". | 5min |
| 🟡 MAJOR | `task_scheduler.py` | 74 | Remove the unused function parameter "notification_email". | 5min |
| 🟡 MAJOR | `task_scheduler.py` | 526 | Remove the unused function parameter "source_queue". | 5min |
| 🟡 MAJOR | `task_scheduler.py` | 527 | Remove the unused function parameter "preserve_metadata". | 5min |
| 🟡 MAJOR | `task_scheduler.py` | 584 | Remove the unused function parameter "include_charts". | 5min |
| 🟡 MAJOR | `user_manager.py` | 137 | Remove the unused function parameter "resource". | 5min |
| 🟡 MAJOR | `user_manager.py` | 201 | Remove the unused function parameter "send_notification". | 5min |
| 🟡 MAJOR | `user_manager.py` | 303 | Remove the unused function parameter "metrics". | 5min |
| 🟡 MAJOR | `user_manager.py` | 303 | Remove the unused function parameter "group_by". | 5min |
| 🟡 MAJOR | `user_manager.py` | 304 | Remove the unused function parameter "output_format". | 5min |
| 🟡 MAJOR | `user_manager.py` | 305 | Remove the unused function parameter "sampling_rate". | 5min |
| 🟡 MAJOR | `user_manager.py` | 305 | Remove the unused function parameter "timezone". | 5min |
| 🟡 MAJOR | `utils.py` | 123 | Remove the unused function parameter "include_metadata". | 5min |
| 🟡 MAJOR | `utils.py` | 124 | Remove the unused function parameter "highlight_overdue". | 5min |

### `python:S1481` — Unknown Rule

| Property | Value |
|----------|-------|
| **Language** | — |
| **Type** | CODE_SMELL |
| **Tags** | — |
| **Violations** | 39 |

**Affected Locations:**

| Severity | File | Line | Message | Effort |
|----------|------|------|---------|--------|
| 🔵 MINOR | `admin_panel.py` | 235 | Remove the unused local variable "unused_severity_map". | 5min |
| 🔵 MINOR | `admin_panel.py` | 270 | Remove the unused local variable "errors". | 5min |
| 🔵 MINOR | `admin_panel.py` | 271 | Remove the unused local variable "unused_logs". | 5min |
| 🔵 MINOR | `data_processor.py` | 193 | Remove the unused local variable "unused_tracker". | 5min |
| 🔵 MINOR | `data_processor.py` | 194 | Remove the unused local variable "temp_buffer". | 5min |
| 🔵 MINOR | `data_processor.py` | 198 | Remove the unused local variable "rec_warnings". | 5min |
| 🔵 MINOR | `data_processor.py` | 207 | Remove the unused local variable "pattern". | 5min |
| 🔵 MINOR | `data_processor.py` | 316 | Remove the unused local variable "unused_agg". | 5min |
| 🔵 MINOR | `inventory_manager.py` | 293 | Remove the unused local variable "product_id". | 5min |
| 🔵 MINOR | `inventory_manager.py` | 294 | Remove the unused local variable "product_name". | 5min |
| 🔵 MINOR | `inventory_manager.py` | 502 | Remove the unused local variable "total_weight". | 5min |
| 🔵 MINOR | `inventory_manager.py` | 503 | Remove the unused local variable "total_volume". | 5min |
| 🔵 MINOR | `models.py` | 54 | Remove the unused local variable "unused_counter". | 5min |
| 🔵 MINOR | `models.py` | 55 | Remove the unused local variable "temp_holder". | 5min |
| 🔵 MINOR | `models.py` | 95 | Remove the unused local variable "unused_count". | 5min |
| 🔵 MINOR | `models.py` | 96 | Remove the unused local variable "temp_filtered". | 5min |
| 🔵 MINOR | `models.py` | 151 | Remove the unused local variable "unused_stat". | 5min |
| 🔵 MINOR | `report_generator.py` | 177 | Remove the unused local variable "unused_counter". | 5min |
| 🔵 MINOR | `report_generator.py` | 178 | Remove the unused local variable "temp_flag". | 5min |
| 🔵 MINOR | `report_generator.py` | 257 | Remove the unused local variable "errors". | 5min |
| 🔵 MINOR | `report_generator.py` | 258 | Remove the unused local variable "unused_var". | 5min |
| 🔵 MINOR | `report_generator.py` | 259 | Remove the unused local variable "temp_data". | 5min |
| 🔵 MINOR | `report_generator.py` | 325 | Remove the unused local variable "unused_accumulator". | 5min |
| 🔵 MINOR | `src/payment_gateway.py` | 16 | Remove the unused local variable "e". | 5min |
| 🔵 MINOR | `storage.py` | 86 | Remove the unused local variable "unused_batch_id". | 5min |
| 🔵 MINOR | `storage.py` | 87 | Remove the unused local variable "temp_items". | 5min |
| 🔵 MINOR | `storage.py` | 138 | Remove the unused local variable "unused_count". | 5min |
| 🔵 MINOR | `storage.py` | 196 | Remove the unused local variable "unused_stat". | 5min |
| 🔵 MINOR | `storage.py` | 232 | Remove the unused local variable "unused_export_count". | 5min |
| 🔵 MINOR | `task_scheduler.py` | 86 | Remove the unused local variable "batch_number". | 5min |
| 🔵 MINOR | `task_scheduler.py` | 102 | Remove the unused local variable "task_owner". | 5min |
| 🔵 MINOR | `task_scheduler.py` | 103 | Remove the unused local variable "task_timeout". | 5min |
| 🔵 MINOR | `user_manager.py` | 212 | Remove the unused local variable "unused_temp". | 5min |
| 🔵 MINOR | `user_manager.py` | 213 | Remove the unused local variable "unused_flag". | 5min |
| 🔵 MINOR | `user_manager.py` | 319 | Remove the unused local variable "hourly_distribution". | 5min |
| 🔵 MINOR | `user_manager.py` | 320 | Remove the unused local variable "unused_metric". | 5min |
| 🔵 MINOR | `utils.py` | 30 | Remove the unused local variable "unused_counter". | 5min |
| 🔵 MINOR | `utils.py` | 84 | Remove the unused local variable "unused_metric". | 5min |
| 🔵 MINOR | `utils.py` | 129 | Remove the unused local variable "unused_format". | 5min |

### `python:S1066` — Unknown Rule

| Property | Value |
|----------|-------|
| **Language** | — |
| **Type** | CODE_SMELL |
| **Tags** | — |
| **Violations** | 27 |

**Affected Locations:**

| Severity | File | Line | Message | Effort |
|----------|------|------|---------|--------|
| 🟡 MAJOR | `admin_panel.py` | 294 | Merge this if statement with the enclosing one. | 5min |
| 🟡 MAJOR | `data_processor.py` | 253 | Merge this if statement with the enclosing one. | 5min |
| 🟡 MAJOR | `inventory_manager.py` | 136 | Merge this if statement with the enclosing one. | 5min |
| 🟡 MAJOR | `inventory_manager.py` | 573 | Merge this if statement with the enclosing one. | 5min |
| 🟡 MAJOR | `inventory_manager.py` | 588 | Merge this if statement with the enclosing one. | 5min |
| 🟡 MAJOR | `models.py` | 102 | Merge this if statement with the enclosing one. | 5min |
| 🟡 MAJOR | `models.py` | 106 | Merge this if statement with the enclosing one. | 5min |
| 🟡 MAJOR | `models.py` | 110 | Merge this if statement with the enclosing one. | 5min |
| 🟡 MAJOR | `models.py` | 114 | Merge this if statement with the enclosing one. | 5min |
| 🟡 MAJOR | `models.py` | 118 | Merge this if statement with the enclosing one. | 5min |
| 🟡 MAJOR | `models.py` | 122 | Merge this if statement with the enclosing one. | 5min |
| 🟡 MAJOR | `models.py` | 126 | Merge this if statement with the enclosing one. | 5min |
| 🟡 MAJOR | `storage.py` | 109 | Merge this if statement with the enclosing one. | 5min |
| 🟡 MAJOR | `storage.py` | 144 | Merge this if statement with the enclosing one. | 5min |
| 🟡 MAJOR | `storage.py` | 148 | Merge this if statement with the enclosing one. | 5min |
| 🟡 MAJOR | `storage.py` | 152 | Merge this if statement with the enclosing one. | 5min |
| 🟡 MAJOR | `storage.py` | 156 | Merge this if statement with the enclosing one. | 5min |
| 🟡 MAJOR | `storage.py` | 160 | Merge this if statement with the enclosing one. | 5min |
| 🟡 MAJOR | `storage.py` | 164 | Merge this if statement with the enclosing one. | 5min |
| 🟡 MAJOR | `storage.py` | 168 | Merge this if statement with the enclosing one. | 5min |
| 🟡 MAJOR | `storage.py` | 172 | Merge this if statement with the enclosing one. | 5min |
| 🟡 MAJOR | `user_manager.py` | 256 | Merge this if statement with the enclosing one. | 5min |
| 🟡 MAJOR | `utils.py` | 37 | Merge this if statement with the enclosing one. | 5min |
| 🟡 MAJOR | `utils.py` | 40 | Merge this if statement with the enclosing one. | 5min |
| 🟡 MAJOR | `utils.py` | 43 | Merge this if statement with the enclosing one. | 5min |
| 🟡 MAJOR | `utils.py` | 46 | Merge this if statement with the enclosing one. | 5min |
| 🟡 MAJOR | `utils.py` | 49 | Merge this if statement with the enclosing one. | 5min |

### `python:S6903` — Unknown Rule

| Property | Value |
|----------|-------|
| **Language** | — |
| **Type** | CODE_SMELL |
| **Tags** | — |
| **Violations** | 24 |

**Affected Locations:**

| Severity | File | Line | Message | Effort |
|----------|------|------|---------|--------|
| 🟠 CRITICAL | `admin_panel.py` | 79 | Don't use `datetime.datetime.utcnow` to create this datetime object. | 5min |
| 🟠 CRITICAL | `admin_panel.py` | 305 | Don't use `datetime.datetime.utcnow` to create this datetime object. | 5min |
| 🟠 CRITICAL | `inventory_manager.py` | 42 | Don't use `datetime.datetime.utcnow` to create this datetime object. | 5min |
| 🟠 CRITICAL | `inventory_manager.py` | 173 | Don't use `datetime.datetime.utcnow` to create this datetime object. | 5min |
| 🟠 CRITICAL | `inventory_manager.py` | 259 | Don't use `datetime.datetime.utcnow` to create this datetime object. | 5min |
| 🟠 CRITICAL | `inventory_manager.py` | 330 | Don't use `datetime.datetime.utcnow` to create this datetime object. | 5min |
| 🟠 CRITICAL | `inventory_manager.py` | 427 | Don't use `datetime.datetime.utcnow` to create this datetime object. | 5min |
| 🟠 CRITICAL | `inventory_manager.py` | 618 | Don't use `datetime.datetime.utcnow` to create this datetime object. | 5min |
| 🟠 CRITICAL | `models.py` | 170 | Don't use `datetime.datetime.utcnow` to create this datetime object. | 5min |
| 🟠 CRITICAL | `models.py` | 29 | Don't use `datetime.datetime.utcnow` to create this datetime object. | 5min |
| 🟠 CRITICAL | `report_generator.py` | 14 | Don't use `datetime.datetime.utcnow` to create this datetime object. | 5min |
| 🟠 CRITICAL | `report_generator.py` | 52 | Don't use `datetime.datetime.utcnow` to create this datetime object. | 5min |
| 🟠 CRITICAL | `report_generator.py` | 90 | Don't use `datetime.datetime.utcnow` to create this datetime object. | 5min |
| 🟠 CRITICAL | `report_generator.py` | 314 | Don't use `datetime.datetime.utcnow` to create this datetime object. | 5min |
| 🟠 CRITICAL | `storage.py` | 20 | Don't use `datetime.datetime.utcnow` to create this datetime object. | 5min |
| 🟠 CRITICAL | `storage.py` | 45 | Don't use `datetime.datetime.utcnow` to create this datetime object. | 5min |
| 🟠 CRITICAL | `task_scheduler.py` | 283 | Don't use `datetime.datetime.utcnow` to create this datetime object. | 5min |
| 🟠 CRITICAL | `task_scheduler.py` | 384 | Don't use `datetime.datetime.utcnow` to create this datetime object. | 5min |
| 🟠 CRITICAL | `task_scheduler.py` | 598 | Don't use `datetime.datetime.utcnow` to create this datetime object. | 5min |
| 🟠 CRITICAL | `user_manager.py` | 32 | Don't use `datetime.datetime.utcnow` to create this datetime object. | 5min |
| 🟠 CRITICAL | `user_manager.py` | 131 | Don't use `datetime.datetime.utcnow` to create this datetime object. | 5min |
| 🟠 CRITICAL | `user_manager.py` | 27 | Don't use `datetime.datetime.utcnow` to create this datetime object. | 5min |
| 🟠 CRITICAL | `user_manager.py` | 125 | Don't use `datetime.datetime.utcnow` to create this datetime object. | 5min |
| 🟠 CRITICAL | `user_manager.py` | 381 | Don't use `datetime.datetime.utcnow` to create this datetime object. | 5min |

### `python:S3776` — Unknown Rule

| Property | Value |
|----------|-------|
| **Language** | — |
| **Type** | CODE_SMELL |
| **Tags** | — |
| **Violations** | 24 |

**Affected Locations:**

| Severity | File | Line | Message | Effort |
|----------|------|------|---------|--------|
| 🟠 CRITICAL | `data_processor.py` | 183 | Refactor this function to reduce its Cognitive Complexity from 109 to the 15 allowed. | 1h39min |
| 🟠 CRITICAL | `data_processor.py` | 309 | Refactor this function to reduce its Cognitive Complexity from 29 to the 15 allowed. | 19min |
| 🟠 CRITICAL | `inventory_manager.py` | 104 | Refactor this function to reduce its Cognitive Complexity from 26 to the 15 allowed. | 16min |
| 🟠 CRITICAL | `inventory_manager.py` | 334 | Refactor this function to reduce its Cognitive Complexity from 23 to the 15 allowed. | 13min |
| 🟠 CRITICAL | `inventory_manager.py` | 431 | Refactor this function to reduce its Cognitive Complexity from 19 to the 15 allowed. | 9min |
| 🟠 CRITICAL | `inventory_manager.py` | 544 | Refactor this function to reduce its Cognitive Complexity from 28 to the 15 allowed. | 18min |
| 🟠 CRITICAL | `models.py` | 89 | Refactor this function to reduce its Cognitive Complexity from 39 to the 15 allowed. | 29min |
| 🟠 CRITICAL | `report_generator.py` | 169 | Refactor this function to reduce its Cognitive Complexity from 41 to the 15 allowed. | 31min |
| 🟠 CRITICAL | `report_generator.py` | 250 | Refactor this function to reduce its Cognitive Complexity from 18 to the 15 allowed. | 8min |
| 🟠 CRITICAL | `report_generator.py` | 318 | Refactor this function to reduce its Cognitive Complexity from 24 to the 15 allowed. | 14min |
| 🟠 CRITICAL | `storage.py` | 78 | Refactor this function to reduce its Cognitive Complexity from 23 to the 15 allowed. | 13min |
| 🟠 CRITICAL | `storage.py` | 132 | Refactor this function to reduce its Cognitive Complexity from 44 to the 15 allowed. | 34min |
| 🟠 CRITICAL | `storage.py` | 226 | Refactor this function to reduce its Cognitive Complexity from 18 to the 15 allowed. | 8min |
| 🟠 CRITICAL | `task_scheduler.py` | 73 | Refactor this function to reduce its Cognitive Complexity from 64 to the 15 allowed. | 54min |
| 🟠 CRITICAL | `task_scheduler.py` | 213 | Refactor this function to reduce its Cognitive Complexity from 72 to the 15 allowed. | 1h2min |
| 🟠 CRITICAL | `task_scheduler.py` | 324 | Refactor this function to reduce its Cognitive Complexity from 33 to the 15 allowed. | 23min |
| 🟠 CRITICAL | `task_scheduler.py` | 399 | Refactor this function to reduce its Cognitive Complexity from 18 to the 15 allowed. | 8min |
| 🟠 CRITICAL | `task_scheduler.py` | 526 | Refactor this function to reduce its Cognitive Complexity from 28 to the 15 allowed. | 18min |
| 🟠 CRITICAL | `task_scheduler.py` | 582 | Refactor this function to reduce its Cognitive Complexity from 17 to the 15 allowed. | 7min |
| 🟠 CRITICAL | `user_manager.py` | 137 | Refactor this function to reduce its Cognitive Complexity from 16 to the 15 allowed. | 6min |
| 🟠 CRITICAL | `user_manager.py` | 201 | Refactor this function to reduce its Cognitive Complexity from 42 to the 15 allowed. | 32min |
| 🟠 CRITICAL | `user_manager.py` | 303 | Refactor this function to reduce its Cognitive Complexity from 18 to the 15 allowed. | 8min |
| 🟠 CRITICAL | `utils.py` | 23 | Refactor this function to reduce its Cognitive Complexity from 35 to the 15 allowed. | 25min |
| 🟠 CRITICAL | `utils.py` | 122 | Refactor this function to reduce its Cognitive Complexity from 34 to the 15 allowed. | 24min |

### `python:S1192` — Unknown Rule

| Property | Value |
|----------|-------|
| **Language** | — |
| **Type** | CODE_SMELL |
| **Tags** | — |
| **Violations** | 9 |

**Affected Locations:**

| Severity | File | Line | Message | Effort |
|----------|------|------|---------|--------|
| 🟠 CRITICAL | `inventory_manager.py` | 61 | Define a constant instead of duplicating this literal " WHERE id = '" 5 times. | 10min |
| 🟠 CRITICAL | `inventory_manager.py` | 85 | Define a constant instead of duplicating this literal "quantity > 0" 3 times. | 6min |
| 🟠 CRITICAL | `inventory_manager.py` | 87 | Define a constant instead of duplicating this literal "warehouse_id = '" 3 times. | 6min |
| 🟠 CRITICAL | `inventory_manager.py` | 91 | Define a constant instead of duplicating this literal "SELECT * FROM products" 3 times. | 6min |
| 🟠 CRITICAL | `inventory_manager.py` | 93 | Define a constant instead of duplicating this literal " WHERE " 3 times. | 6min |
| 🟠 CRITICAL | `inventory_manager.py` | 93 | Define a constant instead of duplicating this literal " AND " 3 times. | 6min |
| 🟠 CRITICAL | `inventory_manager.py` | 123 | Define a constant instead of duplicating this literal "SELECT * FROM products WHERE id = '" 3 times. | 6min |
| 🟠 CRITICAL | `inventory_manager.py` | 161 | Define a constant instead of duplicating this literal "UPDATE products SET quantity = " 3 times. | 6min |
| 🟠 CRITICAL | `task_scheduler.py` | 142 | Define a constant instead of duplicating this literal "UPDATE scheduled_tasks SET status = 'running' WHERE id = '" 6 times. | 12min |

### `python:S1871` — Unknown Rule

| Property | Value |
|----------|-------|
| **Language** | — |
| **Type** | CODE_SMELL |
| **Tags** | — |
| **Violations** | 5 |

**Affected Locations:**

| Severity | File | Line | Message | Effort |
|----------|------|------|---------|--------|
| 🟡 MAJOR | `report_generator.py` | 299 | Either merge this branch with the identical one on line "296" or change one of the implementations. | 10min |
| 🟡 MAJOR | `report_generator.py` | 302 | Either merge this branch with the identical one on line "296" or change one of the implementations. | 10min |
| 🟡 MAJOR | `task_scheduler.py` | 157 | Either merge this branch with the identical one on line "152" or change one of the implementations. | 10min |
| 🟡 MAJOR | `task_scheduler.py` | 162 | Either merge this branch with the identical one on line "152" or change one of the implementations. | 10min |
| 🟡 MAJOR | `task_scheduler.py` | 167 | Either merge this branch with the identical one on line "152" or change one of the implementations. | 10min |

### `python:S6965` — Unknown Rule

| Property | Value |
|----------|-------|
| **Language** | — |
| **Type** | CODE_SMELL |
| **Tags** | — |
| **Violations** | 3 |

**Affected Locations:**

| Severity | File | Line | Message | Effort |
|----------|------|------|---------|--------|
| 🟡 MAJOR | `app.py` | 38 | Specify the HTTP methods this route should accept. | 5min |
| 🟡 MAJOR | `app.py` | 20 | Specify the HTTP methods this route should accept. | 5min |
| 🟡 MAJOR | `auth/__init__.py` | 14 | Specify the HTTP methods this route should accept. | 5min |

### `python:S108` — Unknown Rule

| Property | Value |
|----------|-------|
| **Language** | — |
| **Type** | CODE_SMELL |
| **Tags** | — |
| **Violations** | 2 |

**Affected Locations:**

| Severity | File | Line | Message | Effort |
|----------|------|------|---------|--------|
| 🟡 MAJOR | `inventory_manager.py` | 149 | Either remove or fill this block of code. | 5min |
| 🟡 MAJOR | `task_scheduler.py` | 130 | Either remove or fill this block of code. | 5min |

### `python:S117` — Unknown Rule

| Property | Value |
|----------|-------|
| **Language** | — |
| **Type** | CODE_SMELL |
| **Tags** | — |
| **Violations** | 2 |

**Affected Locations:**

| Severity | File | Line | Message | Effort |
|----------|------|------|---------|--------|
| 🔵 MINOR | `app.py` | 35 | Rename this parameter "itemId" to match the regular expression ^[_a-z][a-z0-9_]*$. | 2min |
| 🔵 MINOR | `storage.py` | 52 | Rename this parameter "itemId" to match the regular expression ^[_a-z][a-z0-9_]*$. | 2min |

### `python:S3626` — Unknown Rule

| Property | Value |
|----------|-------|
| **Language** | — |
| **Type** | CODE_SMELL |
| **Tags** | — |
| **Violations** | 1 |

**Affected Locations:**

| Severity | File | Line | Message | Effort |
|----------|------|------|---------|--------|
| 🔵 MINOR | `data_processor.py` | 284 | Remove this redundant continue. | 1min |

### `python:S3358` — Unknown Rule

| Property | Value |
|----------|-------|
| **Language** | — |
| **Type** | CODE_SMELL |
| **Tags** | — |
| **Violations** | 1 |

**Affected Locations:**

| Severity | File | Line | Message | Effort |
|----------|------|------|---------|--------|
| 🟡 MAJOR | `report_generator.py` | 367 | Extract this nested conditional expression into an independent statement. | 5min |

### `css:S7924` — Unknown Rule

| Property | Value |
|----------|-------|
| **Language** | — |
| **Type** | CODE_SMELL |
| **Tags** | — |
| **Violations** | 1 |

**Affected Locations:**

| Severity | File | Line | Message | Effort |
|----------|------|------|---------|--------|
| 🟡 MAJOR | `static/style.css` | 75 | Text does not meet the minimal contrast requirement with its background. | 5min |

---

## 📎 Appendix: Rule Reference

| Rule Key | Name | Tags |
|----------|------|------|
| `css:S7924` | — | — |
| `python:S1066` | — | — |
| `python:S108` | — | — |
| `python:S117` | — | — |
| `python:S1172` | — | — |
| `python:S1192` | — | — |
| `python:S1481` | — | — |
| `python:S1871` | — | — |
| `python:S3358` | — | — |
| `python:S3626` | — | — |
| `python:S3776` | — | — |
| `python:S6903` | — | — |
| `python:S6965` | — | — |

---
*Report generated by sonar_extract.py on 2026-04-01 17:34 UTC*
