
# Todo List – Big Debt Edition

Flask + in‑memory store demo project **deliberately packed with 20 technical debts**  
for practice / code review exercises.

## Run

```bash
pip install -r requirements.txt
python app.py
```

## Technical Debt Inventory (20)

| Level | # | Description | File |
|-------|---|-------------|------|
| Easy | 1 | Hard‑coded secret key | app.py |
| Easy | 2 | Plain‑text passwords (no hashing) | auth/__init__.py |
| Medium | 3 | No input validation | app.py |
| Medium | 4 | Duplicate session check scattered | app.py / auth |
| Medium | 5 | No pagination or batching | models.list_tasks | 
| Medium | 6 | Blocking email send in request thread | services/email.py |
| Medium | 7 | Global mutable in‑memory DB | models.py |
| Hard | 8 | Monolithic function placeholder (>100 lines) | app.py comment |
| Hard | 9 | Eval‑like SQL‑inj risk analog | models.list_tasks() |
| Hard | 10 | Business logic inside Jinja template | templates/index.html |
| Hard | 11 | Circular import risk (app↔models) | app.py |
| Hard | 12 | Direct data access from template | templates/index.html |
| Hard | 13 | Lack of transaction/error handling | models.py |
| Hard | 14 | `eval` on user input | app.py |
| Hard | 15 | Mutable default argument | auth/__init__.py |
| Hard | 16 | Tight coupling auth ↔ tasks | app.py / models |
| Hard | 17 | No unit tests | — |
| Hard | 18 | Legacy wrapper loaded but unused | services/__init__.py (placeholder) |
| Very Hard | 19 | Mixed str/bytes handling risk in email | services/email.py |
| Very Hard | 20 | Custom thread pool forgotten (race) | services/__init__.py comment |

Use this project to practice identifying and refactoring technical debt.
