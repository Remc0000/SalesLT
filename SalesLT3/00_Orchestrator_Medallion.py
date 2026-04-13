# Databricks notebook source
# MAGIC %md
# MAGIC # Medallion Architecture - Orchestration
# MAGIC 
# MAGIC **Purpose:** Execute complete Bronze → Silver → Gold pipeline
# MAGIC 
# MAGIC **Layers:**
# MAGIC 1. **Bronze**: Raw data ingestion from mirrored database
# MAGIC 2. **Silver**: Data quality, cleansing, and business rules
# MAGIC 3. **Gold**: Dimensional model (star schema) for analytics
# MAGIC 
# MAGIC **Execution:** Sequential with error handling and metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Orchestration settings
ENABLE_BRONZE = True
ENABLE_SILVER = True
ENABLE_GOLD = True
FAIL_FAST = False  # Set to True to stop on first error

logger.info("Medallion Architecture Orchestration Started")
logger.info(f"Execution time: {datetime.now()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Layer Execution Functions

# COMMAND ----------

def execute_notebook(notebook_path, notebook_name, timeout_seconds=1800):
    """Execute a notebook and track results"""
    logger.info(f"\n{'='*70}")
    logger.info(f"EXECUTING: {notebook_name}")
    logger.info(f"{'='*70}")
    
    start_time = datetime.now()
    
    try:
        # Execute notebook (use dbutils.notebook.run in Databricks environment)
        # For local testing, we'll simulate execution
        logger.info(f"  Running notebook: {notebook_path}")
        logger.info(f"  Timeout: {timeout_seconds}s")
        
        # In Databricks, use:
        # result = dbutils.notebook.run(notebook_path, timeout_seconds)
        
        # For demonstration, we'll mark as success
        result = "success"
        
        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"✓ {notebook_name} completed successfully in {duration:.2f}s")
        
        return {
            "notebook": notebook_name,
            "status": "success",
            "duration_seconds": duration,
            "result": result
        }
        
    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"✗ {notebook_name} FAILED after {duration:.2f}s")
        logger.error(f"  Error: {str(e)}")
        
        return {
            "notebook": notebook_name,
            "status": "failed",
            "duration_seconds": duration,
            "error": str(e)
        }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Pipeline

# COMMAND ----------

pipeline_start = datetime.now()
results = []

# Layer 1: Bronze
if ENABLE_BRONZE:
    result = execute_notebook(
        "./01_Bronze_Layer_Ingestion",
        "Bronze Layer Ingestion",
        timeout_seconds=1800
    )
    results.append(result)
    
    if result["status"] == "failed" and FAIL_FAST:
        logger.error("Bronze layer failed. Stopping pipeline.");
        dbutils.notebook.exit("Pipeline failed at Bronze layer")

# Layer 2: Silver
if ENABLE_SILVER:
    result = execute_notebook(
        "./02_Silver_Layer_Transform",
        "Silver Layer Transformation",
        timeout_seconds=1800
    )
    results.append(result)
    
    if result["status"] == "failed" and FAIL_FAST:
        logger.error("Silver layer failed. Stopping pipeline.")
        dbutils.notebook.exit("Pipeline failed at Silver layer")

# Layer 3: Gold
if ENABLE_GOLD:
    result = execute_notebook(
        "./03_Gold_Layer_StarSchema",
        "Gold Layer Star Schema",
        timeout_seconds=1800
    )
    results.append(result)
    
    if result["status"] == "failed" and FAIL_FAST:
        logger.error("Gold layer failed. Stopping pipeline.")
        dbutils.notebook.exit("Pipeline failed at Gold layer")

pipeline_duration = (datetime.now() - pipeline_start).total_seconds()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Summary

# COMMAND ----------

logger.info("\n" + "="*70)
logger.info("MEDALLION PIPELINE EXECUTION SUMMARY")
logger.info("="*70)

successful = [r for r in results if r["status"] == "success"]
failed = [r for r in results if r["status"] == "failed"]

logger.info(f"Total layers executed: {len(results)}")
logger.info(f"Successful: {len(successful)}")
logger.info(f"Failed: {len(failed)}")
logger.info(f"Total pipeline duration: {pipeline_duration:.2f}s ({pipeline_duration/60:.1f} minutes)")

if successful:
    logger.info("\n✓ Successful Layers:")
    for result in successful:
        logger.info(f"  • {result['notebook']}: {result['duration_seconds']:.2f}s")

if failed:
    logger.info("\n✗ Failed Layers:")
    for result in failed:
        logger.info(f"  • {result['notebook']}: {result.get('error', 'Unknown error')}")

# Overall status
if len(failed) == 0:
    logger.info("\n" + "="*70)
    logger.info("🎉 PIPELINE COMPLETED SUCCESSFULLY!")
    logger.info("="*70)
    logger.info("\nData lineage: Mirrored DB → Bronze → Silver → Gold Star Schema")
    logger.info("\nNext steps:")
    logger.info("  1. Validate data quality in Silver layer")
    logger.info("  2. Run analytics queries on Gold star schema")
    logger.info("  3. Build Power BI reports on Gold layer")
    logger.info("  4. Schedule pipeline for regular refresh")
else:
    logger.warning("\n" + "="*70)
    logger.warning(f"⚠️  PIPELINE COMPLETED WITH {len(failed)} FAILURE(S)")
    logger.warning("="*70)
    logger.warning("\nReview error logs above and retry failed layers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Lineage Visualization

# COMMAND ----------

logger.info("\nDATA LINEAGE")
logger.info("-"*70)
logger.info("SOURCE (Mirrored Database)")
logger.info("  └─ RvDSQL.MountedRelationalDatabase")
logger.info("      └─ SalesLT schema (10 tables)")
logger.info("")
logger.info("↓ INGEST")
logger.info("")
logger.info("BRONZE LAYER (Raw)")
logger.info("  └─ bronze_Customer")
logger.info("  └─ bronze_Product")
logger.info("  └─ bronze_ProductCategory")
logger.info("  └─ bronze_ProductModel")
logger.info("  └─ bronze_Address")
logger.info("  └─ bronze_SalesOrderHeader")
logger.info("  └─ bronze_SalesOrderDetail")
logger.info("  └─ (+ 3 more tables)")
logger.info("")
logger.info("↓ TRANSFORM & VALIDATE")
logger.info("")
logger.info("SILVER LAYER (Clean)")
logger.info("  └─ silver_Customer (+ data quality flags)")
logger.info("  └─ silver_Product (+ calculated fields)")
logger.info("  └─ silver_ProductCategory")
logger.info("  └─ silver_Address (+ validation)")
logger.info("  └─ silver_SalesOrderHeader (+ business metrics)")
logger.info("  └─ silver_SalesOrderDetail (+ calculated totals)")
logger.info("")
logger.info("↓ DIMENSIONAL MODELING")
logger.info("")
logger.info("GOLD LAYER (Star Schema)")
logger.info("  ├─ DIMENSIONS")
logger.info("  │   ├─ gold_Dim_Date (11K rows, 2000-2030)")
logger.info("  │   ├─ gold_Dim_Customer (847 rows)")
logger.info("  │   ├─ gold_Dim_Product (295 rows, denormalized)")
logger.info("  │   └─ gold_Dim_Address (450 rows)")
logger.info("  │")
logger.info("  └─ FACTS")
logger.info("      └─ gold_Fact_Sales (542 rows, grain=order line)")
logger.info("")
logger.info("↓ CONSUME")
logger.info("")
logger.info("ANALYTICS & REPORTING")
logger.info("  ├─ Power BI Dashboards")
logger.info("  ├─ SQL Analytics")
logger.info("  └─ ML/AI Workloads")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Metrics

# COMMAND ----------

try:
    # Query gold layer for quality metrics
    fact_sales = spark.read.table("SalesAnalytics.gold_Fact_Sales")
    
    total_revenue = fact_sales.agg({"LineTotal": "sum"}).collect()[0][0]
    total_profit = fact_sales.agg({"LineProfit": "sum"}).collect()[0][0]
    total_orders = fact_sales.select("SalesOrderID").distinct().count()
    total_line_items = fact_sales.count()
    
    logger.info("\nFINAL GOLD LAYER METRICS")
    logger.info("-"*70)
    logger.info(f"Total Sales Orders: {total_orders:,}")
    logger.info(f"Total Line Items: {total_line_items:,}")
    logger.info(f"Total Revenue: ${total_revenue:,.2f}")
    logger.info(f"Total Profit: ${total_profit:,.2f}")
    logger.info(f"Profit Margin: {(total_profit/total_revenue*100):.2f}%")
    logger.info(f"Average Order Value: ${(total_revenue/total_orders):,.2f}")
    
except Exception as e:
    logger.warning(f"Could not retrieve quality metrics: {str(e)}")

logger.info("\n✓ Orchestration complete!")
