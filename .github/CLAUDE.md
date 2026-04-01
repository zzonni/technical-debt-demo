# CLAUDE.md – AGGRESSIVE SONAR AUTO-FIX MODE

**CRITICAL DIRECTIVE:** You are in "Aggressive Auto-Fix Mode". Your sole objective is to reduce SonarQube/Linter technical debt as fast as possible. 
- **NO EXPLANATIONS:** Do not explain what you are going to do. Just execute the code changes.
- **NO TESTS:** Absolutely DO NOT run, read, or fix unit tests. Broken tests are expected and acceptable.
- **FILE-BY-FILE:** Fix all issues in one file completely before moving to the next.

---

## 🚀 Execution Workflow (Iterative File-by-File Loop)

1. **Target:** Identify the list of `.py` files to fix (excluding `tests/`). Pick the FIRST file on the list.
2. **Scan:** Run the linter strictly on that **SINGLE** file:
   `pylint <current_file.py> --max-args=7 --max-cognitive-complexity=15 --disable=C0114,C0115,C0116,C0111,W0703 --output-format=text`
3. **Execute:** Parse the output for this specific file. Apply the "Hardcoded Fix Patterns" below to resolve the errors.
4. **Verify & Repeat:** Re-run the exact scan command from Step 2 on the **SAME** file.
   - If there are still errors, apply fixes again.
   - **DO NOT** move to the next file until the current file returns 0 errors.
5. **Next:** Only when the current file is 100% clean, move to the next file in your list and repeat from Step 2.

---

## 🛠 Hardcoded Fix Patterns (Apply Mechanically)

### 1. S107 / R0913: Too many arguments ( > 7 )
**Trigger:** Pylint reports `too-many-arguments`.
**Immediate Action:**
1. Create a `@dataclass` named `<FunctionName>Config` right above the function.
2. Move all optional/filter parameters into this dataclass.
3. Keep only primary entities (like `owner`, `items`) as normal parameters.
4. Replace the old parameters in the function signature with `config: <FunctionName>Config = None`.
5. Add `cfg = config or <FunctionName>Config()` inside the function.

### 2. S3776 / R0912 / R0915: Cognitive Complexity / Too many branches
**Trigger:** Pylint reports `too-many-branches` or `too-many-statements`.
**Immediate Action:**
1. Locate the deepest nested `for` loop or `if` block.
2. Extract the entire body of that block into a new private module-level function named `_<original_function>_helper`.
3. Pass required local variables as arguments to the helper.
4. Replace the original block with a call to the new helper.
5. Apply early returns (`if not condition: return/continue`) to flatten the remaining code.

### 3. S1523: Dangerous `eval()`
**Trigger:** You see `eval(...)` in the code (e.g., `app.py`).
**Immediate Action:**
Delete the `eval()` call. Replace it with regex validation or a direct type cast.
*Template:*
```python
import re
_SAFE_RE = re.compile(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$")
value = request.args.get("param", "")
safe_value = value if _SAFE_RE.match(value) else "default_safe_value"
```

### 4. S2068: Hardcoded Credentials
**Trigger:** `app.secret_key = "..."` or variables named `password`, `secret`, `token` with hardcoded strings.
**Immediate Action:**
Replace the string with an environment variable fetch with a random fallback.
*Template:*
```python
import os
app.secret_key = os.environ.get("SECRET_KEY", os.urandom(32).hex())
```

### 5. S1481 / W0612 / W0611: Unused Variables & Imports
**Trigger:** Pylint reports `unused-variable`, `unused-import`, or `unused-argument`.
**Immediate Action:**
- **Variables:** Delete the assignment line entirely. Do NOT comment it out.
- **Unpacked Variables:** If part of a tuple unpacking (e.g., `x, y = get()`), rename the unused variable to `_` (e.g., `_, y = get()`).
- **Imports:** Delete the import line.
- **Arguments:** If an argument in a route/function is unused but required by signature, rename it to `_` or `unused_<name>`.

### 6. S1172: Dead Code (e.g., Unused `require_login`)
**Trigger:** A defined function is never called, or a decorator is defined but has no `@wraps`.
**Immediate Action:** If it's a decorator (like `require_login`), fix its implementation using `functools.wraps` and apply it to at least one route (like `/add` or `/toggle`). If it's a random unused utility, delete it entirely.

---

## 🛑 STRICT PROHIBITIONS
1. DO NOT touch the `tests/` folder.
2. DO NOT add docstrings (waste of time for this specific task).
3. DO NOT format code for aesthetics unless it fixes a linter error.
4. DO NOT prompt the user for confirmation. Just write the files.
5. **BRANCH NAMING:** Any work, fixes, or commits MUST be done on a branch following the naming convention: `features/yourname`. Do not commit directly to main.