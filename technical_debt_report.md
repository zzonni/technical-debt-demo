# Socio-Technical Debt Report

This report outlines 10 distinct kinds of **socio-technical debt**. It highlights how organizational structures, company culture, human behaviors, and restrictive policies directly infect the technical architecture and code health.
 
---

## Issue Breakdown

| Debt Type | Location | Description | Sociological / Organizational Driver |
| :--- | :--- | :--- | :--- |
| **1. Knowledge Silos (Bus Factor)** | `src/main.py` (Line 60) | Magic number `99` controls clearance pricing. | The original author ("Dave") hoarded knowledge and left. The team is paralyzed by fear of breaking the legacy system, so the code decays into a "black box". |
| **2. Conway's Law** | `src/main.py` (Lines 37-45) | Exact copy-pasting of address logic across two identical functions. | Siloed teams ("Domestic" vs "International") have strict boundaries, lack shared repositories, and engage in territorial infighting, forcing code duplication. |
| **3. Blame-Driven Development** | `src/payment_gateway.py` (Line 7) | Implicitly mutating bad user string inputs into floats deep within the gateway logic. | A toxic culture where teams blame each other for production crashes. Developers write stealthy, decoupled "fixes" just to get other teams' PMs to leave them alone. |
| **4. Executive Override (Process Bypass)** | `src/main.py` (Line 67) | Hardcoded `user_id == 1` to manually bypass standard authorization and billing tiers. | Sales or Executives bypass established engineering protocols to push deals through, causing fragmented and fragile logic pathways. |
| **5. Abandoned Ownership** | `src/db_connector.py` (Line 7) | Hardcoded credentials connecting to a database. | The team building the service was disbanded. Organizational restructuring left the system orphaned with broken deployment pipelines, forcing dangerous hardcoded patches to keep the lights on. |
| **6. Workaround for Bureaucracy** | `src/main.py` (Line 73) | Natively spinning up a rogue SQLite DB node. | Excessive governance (e.g., getting DB changes takes weeks) forces developers to build fragile "shadow IT" architectures to meet strict sprint deadlines. |
| **7. "Hero" Culture** | `src/main.py` (Line 7) | Use of globally persistent state arrays with aggressive developer comments. | The organization rewards individuals for hotfixing catastrophic failures rather than sustainable engineering. This creates singular "code owners" who discourage collaboration. |
| **8. Post-Acquisition Failure** | `src/main.py` (Line 14) | Parallel arrays instead of distinct Class models. | Enterprise M&A (Mergers and Acquisitions) without proper integration budgets results in Frankenstein codebases built strictly to appease inflexible, decaying proprietary APIs. |
| **9. KPI-Driven Sabotage (Perverse Incentives)** | `src/payment_gateway.py` (Line 18) | Broad exception swallowing (`except Exception... pass`). | Performance bonuses tied to flawed metrics (like an artificial 99.9% uptime requirement) incentivize developers to hide and silence systematic failures. |
| **10. Process Attrition (Doc Debt)** | `src/main.py` (Line 24) | Misleading docstring that actively contradicts the mathematical code. | Overly rigid administrative requirements (having to write Jira Epics just to fix spelling or code comments) prevents developers from maintaining accurate documentation. |

---

## Agent Processes & Context

* `[Inference]` The code assumes that team structures directly correlate to code architecture (Conway's Law).
* The scenario illustrates that "bad code" is rarely just a technical failing; it is almost always a symptom of systemic organizational dysfunction.
