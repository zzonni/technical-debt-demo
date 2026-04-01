# Technical Debt Analysis Results

| Category           | File:Line                    | Description                                                                                                     | Proposed Simple Fix                                                                                     |
| ------------------ | ---------------------------- | --------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| Code Debt          | data_processor.py:183        | validate_and_transform_records has very high cyclomatic complexity E (32) from radon.                           | Split into helper functions for validation, normalization, transformation, and error handling.          |
| Code Debt          | task_scheduler.py:213        | validate_task_config has very high complexity E (33) and long branching logic.                                  | Replace nested condition chains with declarative rule validators and small functions per rule group.    |
| Code Debt          | task_scheduler.py:73         | execute_task_queue has high complexity D (23) and large function size.                                          | Extract fetch, execute, retry, and bookkeeping into separate functions with explicit return contracts.  |
| Code Debt          | storage.py:132               | search_items_advanced has complexity C (20) with repetitive filter logic.                                       | Reuse a shared filtering utility/predicate pipeline instead of hand-written checks in multiple modules. |
| Code Debt          | models.py:89                 | search_tasks_advanced has complexity C (18) with many optional filters and sorting branches.                    | Break into composable predicates and apply them in a sequential filter pipeline.                        |
| Code Debt          | src/main.py:36               | format_domestic_address duplicates format_international_address logic.                                          | Consolidate into one address formatter and keep aliases only if needed for API compatibility.           |
| Architecture Debt  | models.py:10                 | Global mutable in-memory \_db is used as primary state while other modules use JSON/SQLite stores.              | Introduce a repository interface and migrate modules to one canonical persistence strategy per domain.  |
| Architecture Debt  | storage.py:5                 | File persistence via todos.json coexists with in-memory and SQLite storage, causing split source-of-truth risk. | Define ownership boundaries and add adapters during migration to avoid dual writes.                     |
| Architecture Debt  | src/main.py:8                | Global runtime state (TOTAL_REVENUE, PROCESSED_ORDERS) couples business behavior to process lifecycle.          | Persist order and revenue events, then compute aggregates from durable data.                            |
| Architecture Debt  | services/**init**.py:8       | LegacyThreadPool is globally initialized at import time, creating side effects and hidden concurrency.          | Initialize worker pools at app startup and inject executor dependency where needed.                     |
| Security Debt      | src/db_connector.py:7        | Hardcoded DB host/user/password in source code.                                                                 | Move credentials to environment variables or a secrets manager and fail fast if missing.                |
| Security Debt      | auth/**init**.py:10          | Plaintext password comparison during login.                                                                     | Store salted password hashes and verify using bcrypt/argon2.                                            |
| Code Debt          | auth/**init**.py:19          | Mutable default argument in current_user(default=[]).                                                           | Change default to None and initialize inside the function.                                              |
| Documentation Debt | README.md:1                  | README is minimal and omits full setup/testing/architecture guidance. [Inference]                               | Add sections for environment setup, test commands, coverage workflow, and architecture map.             |
| Documentation Debt | app.py:16                    | Public route/helper functions mostly lack docstrings. [Inference]                                               | Add concise docstrings for public functions and module intent notes.                                    |
| Testing Debt       | requirements.txt:1           | Test collection fails because bcrypt is missing, blocking full suite execution.                                 | Add bcrypt to dependencies and ensure CI installs it before test stage.                                 |
| Testing Debt       | tests/test_user_manager.py:3 | Coverage run aborts during collection due to missing dependency path, reducing confidence in current metrics.   | Fix dependency/import blockers and make test collection pass a required CI gate.                        |
| Testing Debt       | .:1                          | Captured coverage output shows low overall coverage (18%). [Unverified]                                         | Re-run after dependency fixes, then enforce per-module minimum thresholds.                              |

## Complexity And Size Snapshot

- radon cc hotspots: task_scheduler.py:213 (E33), data_processor.py:183 (E32), task_scheduler.py:73 (D23).
- Files over 300 LOC (non-test): task_scheduler.py, inventory_manager.py, user_manager.py, report_generator.py, data_processor.py, admin_panel.py.
- Long functions over 50 lines include: data_processor.py:183, task_scheduler.py:73, task_scheduler.py:213, inventory_manager.py:104, report_generator.py:169, user_manager.py:206.

## Explicit Marker Scan

- Scan for # TODO:, # FIXME:, # HACK:, # DEPRECATED: did not find confirmed application-source matches; matches observed were in repo metadata/tooling files.
