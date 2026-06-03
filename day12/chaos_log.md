# Day 12 — Chaos Log

## Pre-Exercise Answer

**Question:** You have a multi-agent system where a Forensics Agent needs to check CloudWatch metrics AND query Snowflake AND read S3 files. Should these be one Lambda function or three separate ones?

**Answer (write your thoughts):**
- One function: Simpler deployment, but couples concerns. If Snowflake is down, CloudWatch checks also fail.
- Three functions: Separation of concerns, independent scaling, but harder to correlate findings in one transaction.
- **Best practice:** 3 separate functions. The Supervisor/Forensics Agent orchestrates calls and correlates results.

---

## Phase 2 — Manual Investigation

**Records in S3 Bronze (disaster folder):** 847 files, 847 records
**Records in Snowflake (disaster window):** 0 rows loaded

**Failure timestamp:** 02:11 UTC (Lambda v2 deployment detected)
**What changed:** Lambda sigma-kinesis-producer auto-deployed to v2
**Root cause:** Schema mismatch — v2 outputs merchant_nm + DD-MM-YYYY; Snowflake expects merchant_name + YYYY-MM-DD
**Why no alert fired:** CloudWatch threshold was set too high (needed zero-row detection alarm)

**Time taken:** 45 minutes (manual investigation)
**Signals connected:** 
- Lambda version history (CloudWatch)
- Snowflake COPY INTO logs
- S3 file contents (JSON schema)
- Firehose delivery metrics

**Signal you missed:** The 23 records with null transaction_ids (quarantine reason revealed only by agent tool)

---

## Phase 3 — Agent Findings (Expected Output)

**What Agent Found (26 seconds):**
- **Root cause:** Lambda v2 schema mismatch confirmed
- **Impact:** ₹4,72,340 GMV missing, QuickMart SLA breached (threshold ₹50K)
- **Recovery:** 824 records replayed idempotently, 23 quarantined
- **Prevention:** 3 new alarms created (live in account)
- **Time taken:** 26 seconds
- **Human interventions:** 0

**What You Missed (Phase 2 vs Phase 3):**
The agent caught that the 23 quarantined records had null transaction_ids — a separate data quality issue that would have broken idempotency if not detected. You could not see this manually without parsing S3 file JSON.

---

## Judgment Questions (To Answer)

**Q1: Forensics Agent**
What one CloudWatch alarm would have caught this at 02:12 instead of 09:03?
**Answer:** A "zero-row Snowflake COPY INTO" alarm — fires when 2 consecutive COPY operations load 0 rows.

**Q2: Recovery Agent**  
If a legitimate duplicate transaction_id existed in source data, how would you change deduplication?
**Answer:** Use composite key: (transaction_id, merchant_id, timestamp) instead of transaction_id alone. Or implement versioning with max(loaded_at) for latest.

**Q3: Hardening Agent**
The sigma-lambda-version-change alarm fires on ANY Lambda version change. Your team deploys 20x/day. Keep it or replace?
**Answer:** Replace with alarm that fires only on CANARY phase failures (track canary vs production traffic). Current alarm = spam.

