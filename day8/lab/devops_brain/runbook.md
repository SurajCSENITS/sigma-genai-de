## Pipeline Overview

This pipeline ingests transaction data, transforms it, and computes merchant performance and daily summaries. It runs to ensure data is up-to-date for reporting and analytics. If it stops, downstream reports and dashboards will be outdated.

## Pipeline Steps

1. Connect to DuckDB using `get_connection`.
2. Set up tables using `setup_tables`.
3. Load merchants using `load_merchants`.
4. Load transactions into bronze table using `load_bronze`.
5. Transform bronze to silver using `transform_bronze_to_silver`.
6. Load silver transactions using `load_silver`.
7. Compute merchant performance using `compute_merchant_performance`.
8. Compute daily summary using `compute_daily_summary`.
9. Load gold tables using `load_gold`.

## Schedule / Trigger

This pipeline runs every hour, triggered by a cron job.

## Failure Modes

1. **DuckDB Connection Failure**
   - **Root Cause:** Database is down.
   - **Symptom:** `get_connection` fails.
2. **Table Creation Failure**
   - **Root Cause:** SQL syntax error.
   - **Symptom:** `setup_tables` throws an exception.
3. **Merchant Data Load Failure**
   - **Root Cause:** Corrupt merchant data.
   - **Symptom:** `load_merchants` fails.
4. **Bronze Table Load Failure**
   - **Root Cause:** Invalid transaction data.
   - **Symptom:** `load_bronze` fails.
5. **Silver Table Transformation Failure**
   - **Root Cause:** Missing merchant IDs.
   - **Symptom:** `transform_bronze_to_silver` fails.

## Recovery Actions

1. **DuckDB Connection Failure**
   - Check DuckDB service status.
   - Restart DuckDB if necessary.
   - Retry pipeline.
2. **Table Creation Failure**
   - Review SQL in `setup_tables`.
   - Fix syntax error.
   - Retry pipeline.
3. **Merchant Data Load Failure**
   - Validate merchant data.
   - Correct any errors.
   - Retry pipeline.
4. **Bronze Table Load Failure**
   - Validate transaction data.
   - Correct any errors.
   - Retry pipeline.
5. **Silver Table Transformation Failure**
   - Ensure all merchant IDs exist.
   - Retry pipeline.

## Known Bugs

- Hardcoded AWS credentials in the code.
- Lack of null handling in `transform_bronze_to_silver`.

## Escalation Contacts

1. **On-call DE:** Priya Nair (priya.nair@sigmadatatech.in, +91-98400-11111)
2. **Tech Lead:** Arjun Mehta (arjun.mehta@sigmadatatech.in)
3. **Platform Manager:** Kavya Reddy (kavya.reddy@sigmadatatech.in)

## Data Quality Checks

- Verify the number of records in `silver_transactions`.
- Check `gold_merchant_performance` for expected merchant IDs.
- Ensure `gold_daily_summary` has today's date.