# Project Guidelines

## Overview

Flask + in-memory store demo project **deliberately packed with technical debt** for practice and code-review exercises.

## Architecture

| Layer           | Files                                                          | Responsibility                                     |
| --------------- | -------------------------------------------------------------- | -------------------------------------------------- |
| Web app         | `app.py`, `auth/`, `templates/`, `static/`                     | Flask routes, login/logout, HTML rendering         |
| Domain models   | `models.py`                                                    | In-memory `_db` dict, task CRUD, search, stats     |
| Persistence     | `storage.py`                                                   | JSON file (`todos.json`) CRUD, bulk ops            |
| Utilities       | `utils.py`                                                     | Filtering, sorting, metrics, display formatting    |
| Services        | `services/email.py`, `services/__init__.py`                    | Email stub, legacy thread pool                     |
| Data processing | `data_processor.py`                                            | Import/export, ETL, DB queries, batch transforms   |
| Inventory       | `inventory_manager.py`                                         | Product CRUD, stock adjustments (SQLite)           |
| Users           | `user_manager.py`                                              | Account management, permissions, activity logs     |
| Admin           | `admin_panel.py`                                               | Dashboard, order/product search, exports           |
| Reports         | `report_generator.py`                                          | Sales/inventory/customer report generation         |
| Scheduler       | `task_scheduler.py`                                            | Task queue, execution engine                       |
| Checkout        | `src/main.py`, `src/db_connector.py`, `src/payment_gateway.py` | E-commerce checkout flow with socio-technical debt |

**Data storage is dual**: `models.py` uses a global in-memory dict; `storage.py` persists to `todos.json`; several modules use SQLite (`ecommerce.db`). This inconsistency is intentional debt.

## Build and Test

**Always activate the virtual environment before running any command:**

```bash
source .venv/bin/activate             # MUST run first in every terminal session
pip install -r requirements.txt      # Flask, pytest, pytest-cov
python app.py                         # Run dev server (debug mode)
source .venv/bin/activate && pytest tests/ -v                      # Run all tests
source .venv/bin/activate && pytest tests/ --cov=. --cov-report=term  # Tests with coverage
```

> **Rule:** When running unit tests, always prefix with `source .venv/bin/activate &&` to ensure the correct Python environment is used. Never run `pytest` without activating the venv first.

CI runs on GitHub Actions (`.github/workflows/build.yml`): pytest + coverage → SonarQube scan.

## Conventions

- **Python 3.12**, no type hints used (part of the debt profile).
- No linter or formatter is configured — code style is inconsistent by design.
- Tests mirror source files: `test_app.py` → `app.py`, `test_src_main.py` → `src/main.py`.
- Hardcoded secrets, SQL injection, `eval()`, `pickle`, `os.system()`, MD5 hashing, and mutable default args appear throughout — these are **intentional vulnerabilities** for educational use.

## Working With This Repo

- Preserve debt markers in files you don't modify — they serve as teaching material.
- New test files go in `tests/` following the `test_<module>.py` naming convention.
