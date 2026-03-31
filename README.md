# Python Todo List with Intentional Technical Debt

A small Python-only Todo List web app built with Flask.

## Run

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python app.py
```

Then open `http://127.0.0.1:5000`.

## Project Structure

- `app.py` - main Flask app
- `storage.py` - file-based persistence layer
- `utils.py` - helper functions
- `templates/index.html` - UI
- `static/style.css` - styles

## Intentional Technical Debt

### Easy (3)
1. **Hardcoded secret key** in `app.py`.
2. **Print-based logging** instead of structured logging.
3. **Inconsistent naming** (`todo_id`, `itemId`, `task_name`) across modules.

### Medium (4)
4. **File-based JSON storage** instead of a proper database; not scalable.
5. **No input validation** for lengths/content beyond minimal checks.
6. **Mixed responsibilities** in route handlers (business logic + HTTP + persistence).
7. **Duplicate status logic** appears in multiple functions.

### Hard (2)
8. **Race condition risk** when multiple writes hit `todos.json` at the same time.
9. **No automated tests / contract checks**, making refactoring risky.

### Very Hard (1)
10. **Hidden schema coupling and backward-compatibility hack** in `storage.py`: old and new todo shapes are silently normalized, making future migrations risky and bugs subtle.

## Notes

This project is intentionally imperfect for code review, refactoring, or software maintenance exercises.
