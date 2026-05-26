import logging
import shutil
import json
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, current_timestamp, lit, sum, count, max, broadcast, coalesce, avg, collect_set, to_date
from pyspark.sql.types import StringType, FloatType, DateType

logging.basicConfig(level=logging.INFO)

def ingest_bronze(spark, input_path, output_path, run_date, run_id):
    try:
        logging.info("Starting ingest_bronze stage")
        
        # Read raw CSV files with all columns as strings
        transactions_df = (spark.read.option("header", "true")
                          .option("inferSchema", "false")
                          .csv(input_path))
        
        # Add metadata columns
        transactions_df = (transactions_df.withColumn("ingestion_timestamp", current_timestamp())
                           .withColumn("source_file", lit(os.path.basename(input_path)))
                           .withColumn("pipeline_run_id", lit(run_id)))
        
        # Delete existing partition before writing
        partition_path = f"{output_path}/transaction_date={run_date}"
        shutil.rmtree(partition_path, ignore_errors=True)
        
        # Write as Parquet partitioned by date
        transactions_df.write.mode('overwrite').partitionBy('transaction_date').parquet(output_path)
        
        logging.info(f"[Stage: ingest_bronze] input_count: {transactions_df.count():,} rows")
        
    except Exception as e:
        logging.error(f"Error in ingest_bronze stage: {e}")
        logging.error(f"[Stage: ingest_bronze] input_count: {transactions_df.count():,} rows at time of failure")
        raise

def transform_silver(spark, bronze_path, merchants_path, output_path, run_date):
    try:
        logging.info("Starting transform_silver stage")
        
        # Read Bronze Parquet with partition pruning on run_date
        transactions_df = (spark.read.parquet(bronze_path)
                          .where(col("transaction_date") == run_date)
                           .cache())
        
        # Read merchants data and cache it
        merchants_df = (spark.read.option("header", "true")
                        .csv(merchants_path)
                        .withColumn("merchant_id", col("merchant_id").cast(StringType()))
                       .cache())
        
        # Cast columns to correct types
        transactions_df = (transactions_df.withColumn("amount", col("amount").cast(FloatType()))
                          .withColumn("transaction_date", col("transaction_date").cast(DateType()))
                           .withColumn("transaction_id", col("transaction_id").cast(StringType()))
                          .withColumn("merchant_id", col("merchant_id").cast(StringType())))
        
        # Filter: remove records where transaction_id is NULL or amount < 0
        filtered_transactions_df = transactions_df.filter((col("transaction_id").isNotNull()) & (col("amount") >= 0))
        logging.info(f"[Stage: transform_silver] after_filter_count: {filtered_transactions_df.count():,} rows")
        
        # Deduplicate: if same transaction_id appears twice, keep the record with latest ingestion_timestamp
        deduped_transactions_df = (filtered_transactions_df.orderBy(col("transaction_id"), col("ingestion_timestamp").desc())
                                  .dropDuplicates(["transaction_id"]))
        logging.info(f"[Stage: transform_silver] after_dedup_count: {deduped_transactions_df.count():,} rows")
        
        # Enrich: join transactions with merchants on merchant_id to get merchant_name, category, city
        enriched_transactions_df = (deduped_transactions_df.join(broadcast(merchants_df), deduped_transactions_df.merchant_id == merchants_df.merchant_id, "left_outer")
                                   .withColumn("quality_flag", 
                                               when(col("merchant_id").isNull(), "UNMATCHED").otherwise("CLEAN")))
        
        # Delete existing partition before writing
        partition_path = f"{output_path}/transaction_date={run_date}"
        shutil.rmtree(partition_path, ignore_errors=True)
        
        # Write as Parquet partitioned by date
        enriched_transactions_df.write.mode('overwrite').partitionBy('transaction_date').parquet(output_path)
        
        logging.info(f"[Stage: transform_silver] output_count: {enriched_transactions_df.count():,} rows")
        
    except Exception as e:
        logging.error(f"Error in transform_silver stage: {e}")
        logging.error(f"[Stage: transform_silver] input_count: {transactions_df.count():,} rows at time of failure")
        raise

def build_merchant_performance(spark, silver_path, output_path, run_date):
    try:
        logging.info("Starting build_merchant_performance stage")
        
        # Read Silver layer data with partition pruning
        silver_df = spark.read.parquet(silver_path).filter(col("transaction_date") == run_date)
        
        # Calculate total revenue, transaction count, and failure rate
        merchant_performance_df = silver_df.groupBy("merchant_id", "merchant_name", "category", "city", "transaction_date") \
          .agg(
                sum(when(col("status") == "COMPLETED", col("amount")).otherwise(0)).alias("total_revenue"),
                count("*").alias("txn_count"),
                (count(when(col("status") == "FAILED", 1)) / count("*") * 100).alias("failure_rate_pct")
            )
        
        # Delete existing partition before writing
        partition_path = f"{output_path}/transaction_date={run_date}"
        shutil.rmtree(partition_path, ignore_errors=True)
        
        # Write to Gold layer
        merchant_performance_df.write.mode("overwrite").partitionBy("transaction_date").parquet(output_path)
        
        logging.info(f"[Stage: build_merchant_performance] output_count: {merchant_performance_df.count():,} rows")
        
    except Exception as e:
        logging.error(f"Error in build_merchant_performance stage: {e}")
        logging.error(f"[Stage: build_merchant_performance] input_count: {silver_df.count():,} rows at time of failure")
        raise

def build_customer_ltv(spark, silver_path, output_path):
    try:
        logging.info("Starting build_customer_ltv stage")
        
        # Read Silver layer data
        silver_df = spark.read.parquet(silver_path)
        
        # Calculate LTV metrics
        customer_ltv_df = silver_df.filter(col("status") == "COMPLETED") \
            .groupBy("customer_id") \
           .agg(
                sum("amount").alias("total_spent"),
                count("*").alias("total_txns"),
                avg("amount").alias("avg_txn_value"),
                min("transaction_date").alias("first_txn_date"),
                max("transaction_date").alias("last_txn_date"),
                coalesce(collect_set("payment_method").agg()).alias("preferred_payment_method")
            )
        
        # Delete existing partition before writing
        shutil.rmtree(output_path, ignore_errors=True)
        
        # Write to Gold layer
        customer_ltv_df.write.mode("overwrite").parquet(output_path)
        
        logging.info(f"[Stage: build_customer_ltv] output_count: {customer_ltv_df.count():,} rows")
        
    except Exception as e:
        logging.error(f"Error in build_customer_ltv stage: {e}")
        logging.error(f"[Stage: build_customer_ltv] input_count: {silver_df.count():,} rows at time of failure")
        raise

def build_daily_summary(spark, silver_path, output_path, run_date):
    try:
        logging.info("Starting build_daily_summary stage")
        
        # Read Silver layer data for the specific date with partition pruning
        silver_df = spark.read.parquet(silver_path).filter(col("transaction_date") == run_date)
        
        # Calculate daily summary metrics
        daily_summary_df = silver_df.groupBy("transaction_date") \
            .agg(
                sum(when(col("status") == "COMPLETED", col("amount")).otherwise(0)).alias("total_revenue"),
                count("*").alias("total_txns"),
                count(distinct("customer_id")).alias("unique_customers"),
                count(distinct("merchant_id")).alias("unique_merchants"),
                (count(when(col("status") == "FAILED", 1)) / count("*") * 100).alias("failure_rate_pct")
            )
        
        # Delete existing partition before writing
        partition_path = f"{output_path}/transaction_date={run_date}"
        shutil.rmtree(partition_path, ignore_errors=True)
        
        # Write to Gold layer
        daily_summary_df.write.mode("overwrite").partitionBy("transaction_date").parquet(output_path)
        
        logging.info(f"[Stage: build_daily_summary] output_count: {daily_summary_df.count():,} rows")
        
    except Exception as e:
        logging.error(f"Error in build_daily_summary stage: {e}")
        logging.error(f"[Stage: build_daily_summary] input_count: {silver_df.count():,} rows at time of failure")
        raise

def run_pipeline(spark, input_path, merchants_path, bronze_path, silver_path, gold_output_dir, run_date, run_id):
    try:
        logging.info("Pipeline started")
        
        started_at = datetime.now().isoformat()
        
        # Ingest Bronze layer
        ingest_bronze(spark, input_path, bronze_path, run_date, run_id)
        
        # Transform Silver layer
        transform_silver(spark, bronze_path, merchants_path, silver_path, run_date)
        
        # Run Gold layer
        run_gold(spark, silver_path, gold_output_dir, run_date)
        
        completed_at = datetime.now().isoformat()
        
        run_metadata = {
            "pipeline_name": "Sigma DataTech Transaction Analytics Pipeline",
            "run_date": run_date,
            "run_id": run_id,
            "run_status": "SUCCESS",
            "started_at": started_at,
            "completed_at": completed_at
        }
        
        with open(f"{gold_output_dir}/run_metadata_{run_date}.json", "w") as f:
            json.dump(run_metadata, f)
        
        logging.info("Pipeline completed successfully")
        
    except Exception as e:
        completed_at = datetime.now().isoformat()
        
        run_metadata = {
            "pipeline_name": "Sigma DataTech Transaction Analytics Pipeline",
            "run_date": run_date,
            "run_id": run_id,
            "run_status": "FAILED",
            "error_message": str(e),
            "started_at": started_at,
            "completed_at": completed_at
        }
        
        with open(f"{gold_output_dir}/run_metadata_{run_date}.json", "w") as f:
            json.dump(run_metadata, f)
        
        logging.error("Pipeline failed")
        raise

def run_gold(spark, silver_path, gold_output_dir, run_date):
    try:
        logging.info("Starting run_gold stage")
        
        # Define output paths for each Gold table
        merchant_performance_output_path = f"{gold_output_dir}/merchant_performance"
        customer_ltv_output_path = f"{gold_output_dir}/customer_ltv"
        daily_summary_output_path = f"{gold_output_dir}/daily_summary"
        
        # Build each Gold table
        build_merchant_performance(spark, silver_path, merchant_performance_output_path, run_date)
        build_customer_ltv(spark, silver_path, customer_ltv_output_path)
        build_daily_summary(spark, silver_path, daily_summary_output_path, run_date)
        
        # Write run metadata summary to JSON
        run_metadata = {
            "run_date": run_date,
            "silver_path": silver_path,
            "gold_output_dir": gold_output_dir,
            "tables": [
                {"name": "merchant_performance", "path": merchant_performance_output_path},
                {"name": "customer_ltv", "path": customer_ltv_output_path},
                {"name": "daily_summary", "path": daily_summary_output_path}
            ]
        }
        
        spark.sparkContext.parallelize([run_metadata]).write.json(f"{gold_output_dir}/run_metadata.json")
        
    except Exception as e:
        logging.error(f"Error in run_gold stage: {e}")
        raise

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Sigma DataTech Transaction Analytics Pipeline").getOrCreate()
    
    input_path = "s3://sigma-datatech/bronze/transactions.csv"
    merchants_path = "s3://sigma-datatech/bronze/merchants.csv"
    bronze_path = "s3://sigma-datatech/silver/transactions"
    silver_path = "s3://sigma-datatech/silver/transactions"
    gold_output_dir = "s3://sigma-datatech/gold"
    run_date = "2026-05-27"
    run_id = "run_20260527"
    
    run_pipeline(spark, input_path, merchants_path, bronze_path, silver_path, gold_output_dir, run_date, run_id)
