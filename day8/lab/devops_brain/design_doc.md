# Data Pipeline Design Document

## What This Pipeline Does
This pipeline ingests transaction data from both clean and dirty sources, processes it through bronze, silver, and gold layers, and ultimately computes merchant performance metrics and daily summaries.

## Data Flow Diagram
```plaintext
+--------------------+       +--------------------+       +--------------------+       +--------------------+
|   Source           |       |     Bronze Layer   |       |     Silver Layer   |       |      Gold Layer    |
|  (TRANSACTIONS)    | ----->| bronze_transactions| ----->|  silver_transactions| ----->| gold_merchant_perf  |
|                    |       |                    |       |                    |       |     gold_daily_summ |
+--------------------+       +--------------------+       +--------------------+       +--------------------+
```

## Key Design Decisions
- **Layered Approach**: The pipeline uses a bronze-silver-gold approach to ensure data quality and transformation are handled in distinct stages.
- **Quality Flags**: Introduced quality flags in the silver layer to distinguish between clean and potentially problematic data.
- **Aggregations in Gold**: Computed metrics are stored in the gold layer for efficient querying and reporting.
- **Date-based Partitioning**: Gold layer tables are partitioned by date to facilitate time-series analysis.

## Known Limitations
- **Single-threaded Processing**: The pipeline currently runs sequentially, which may not be optimal for very large datasets.
- **No Error Handling**: The pipeline lacks robust error handling, which could lead to data loss in case of failures.
- **Static Merchant Data**: Merchant data is loaded once and not updated dynamically, which may lead to stale information.
- **No Data Validation**: There is no built-in data validation mechanism to ensure the integrity of incoming transactions.

## Dependencies
- **DuckDB**: The pipeline relies on DuckDB for data storage and querying.
- **MERCHANTS**: A predefined list of merchants used for enriching transaction data.
- **TRANSACTIONS_CLEAN and TRANSACTIONS_DIRTY**: Source data files containing clean and dirty transaction records.