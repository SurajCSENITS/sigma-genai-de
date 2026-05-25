# NL2SQL vs Cortex Analyst — Sigma DataTech Evaluation
**Team:** Opus
**Date:** May 25, 2026

## 5-Question Head-to-Head Results

| # | Question | Module 2 SQL Correct? | Cortex SQL Correct? | Module 2 Time | Cortex Time |
|---|----------|--------------------|---------------------|------------|-------------|
| 1 | Total transaction count | YES | YES | < 1s | 15.7s |
| 2 | Failed transaction count | YES | YES | < 1s | 14.0s |
| 3 | Highest revenue merchant | YES | YES | < 1s | 799.0s |
| 4 | Failure rate by payment method | YES | YES | < 1s | 18.0s |
| 5 | Total revenue (with COMPLETED filter) | YES | YES | < 1s | 276.5s |

## Observations

### Where Module 2 NL2SQL was better:
* **Latency:** The custom pipeline is significantly faster (sub-1 second) compared to Cortex Analyst, which experienced high latency, likely due to overhead in the environment’s semantic lookup.
* **Security Guardrails:** The custom validator successfully rejected a malicious `DROP TABLE` command, providing an essential safety layer that Cortex Analyst does not natively expose as part of its generated SQL audit.
* **Context Handling:** The system proved highly resilient in ablation experiments, correctly inferring JOINs and business logic even when specific hints were stripped.

### Where Cortex Analyst was better:
* **Governance:** It relies on a centralized semantic YAML model, which acts as a "single source of truth" and reduces the risk of "prompt drift" common in custom Python pipelines.
* **Low-Code Maintenance:** Since it uses native Snowflake metadata, you don't need to manually update long strings in prompts whenever the schema changes.

### Business Rule Accuracy
Question 5 is the critical test — revenue must only count COMPLETED transactions. Did both systems apply this rule correctly?
* **Module 2:** Yes, it explicitly used `WHERE STATUS = 'COMPLETED'`.
* **Cortex:** Yes, it correctly utilized the semantic model definition to filter the transactions.

---

## Your Recommendation

**Your recommendation:** Hybrid Approach

**Reason:** I recommend deploying a **Hybrid Approach** to combine the best of both worlds. We should use **Cortex Analyst** to maintain the semantic model and define governed business metrics (ensuring accuracy), while building a custom **API/Validation layer** (based on our Module 2 pipeline) to sit in front of it. This provides the fast response times and essential security guardrails (e.g., input validation and audit logging) that production enterprise users require.