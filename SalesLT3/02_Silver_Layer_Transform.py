# Databricks notebook source
# MAGIC %md
# MAGIC # Medallion Architecture: Silver Layer
# MAGIC 
# MAGIC **Purpose:** Transform Bronze data into cleaned, validated Silver layer
# MAGIC 
# MAGIC **Source:** Bronze layer tables
# MAGIC 
# MAGIC **Target:** Silver layer tables with data quality rules
# MAGIC 
# MAGIC **Transformations:**
# MAGIC - Data type standardization
# MAGIC - NULL handling and defaults
# MAGIC - Deduplication
# MAGIC - Data validation and quality checks
# MAGIC - Business rule application
# MAGIC - Add data quality flags

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, trim, lower, upper, initcap, current_timestamp, current_date, datediff, row_number, sum as spark_sum
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TARGET_LAKEHOUSE = "SalesAnalytics"
BRONZE_LAYER = "bronze"
SILVER_LAYER = "silver"

def get_bronze_table(table_name):
    return f"{TARGET_LAKEHOUSE}.{BRONZE_LAYER}_{table_name}"

def get_silver_table(table_name):
    return f"{SILVER_LAYER}_{table_name}"

logger.info("Silver layer configuration loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer: Customer

# COMMAND ----------

def build_silver_customer():
    """Transform Bronze Customer to Silver with data quality"""
    logger.info("Building silver_Customer...")
    
    bronze_customer = spark.read.table(get_bronze_table("Customer"))
    
    silver_customer = bronze_customer.select(
        col("CustomerID"),
        col("Title"),
        col("Suffix"),
        col("CompanyName"),
        col("SalesPerson"),
        # Clean email addresses
        when(col("EmailAddress").rlike("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"), 
             lower(trim(col("EmailAddress"))))
        .otherwise(None).alias("EmailAddress"),
        col("ModifiedDate"),
        col("_ingest_timestamp")
    )
    
    # Add data quality flags
    silver_customer = silver_customer \
        .withColumn("_is_valid_email", col("EmailAddress").isNotNull()) \
        .withColumn("_has_company", col("CompanyName").isNotNull()) \
        .withColumn("_processing_timestamp", current_timestamp())
    
    # Deduplicate (keep most recent)
    window_spec = Window.partitionBy("CustomerID").orderBy(col("ModifiedDate").desc())
    silver_customer = silver_customer \
        .withColumn("_row_num", row_number().over(window_spec)) \
        .filter(col("_row_num") == 1) \
        .drop("_row_num")
    
    row_count = silver_customer.count()
    logger.info(f"  Built silver_Customer: {row_count:,} rows")
    
    return silver_customer

silver_customer = build_silver_customer()
silver_customer.write.format("delta").mode("overwrite").saveAsTable(f"{TARGET_LAKEHOUSE}.{get_silver_table('Customer')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer: Product

# COMMAND ----------

def build_silver_product():
    """Transform Bronze Product to Silver with data quality"""
    logger.info("Building silver_Product...")
    
    bronze_product = spark.read.table(get_bronze_table("Product"))
    
    silver_product = bronze_product.select(
        col("ProductID"),
        upper(trim(col("ProductNumber"))).alias("ProductNumber"),
        initcap(trim(col("Color"))).alias("Color"),
        upper(trim(col("Size"))).alias("Size"),
        col("Weight"),
        col("StandardCost"),
        col("ListPrice"),
        col("ProductCategoryID"),
        col("ProductModelID"),
        col("SellStartDate"),
        col("SellEndDate"),
        col("DiscontinuedDate"),
        col("ThumbNailPhoto"),
        col("ThumbnailPhotoFileName"),
        col("ModifiedDate"),
        col("_ingest_timestamp")
    )
    
    # Add business logic and quality flags
    silver_product = silver_product \
        .withColumn("_is_active", 
                   (col("SellEndDate").isNull()) | (col("SellEndDate") > current_date())) \
        .withColumn("_is_discontinued", col("DiscontinuedDate").isNotNull()) \
        .withColumn("_has_valid_pricing", 
                   (col("StandardCost") > 0) & (col("ListPrice") > 0) & (col("ListPrice") >= col("StandardCost"))) \
        .withColumn("_margin_pct", 
                   when(col("ListPrice") > 0, 
                        ((col("ListPrice") - col("StandardCost")) / col("ListPrice") * 100))
                   .otherwise(None)) \
        .withColumn("_processing_timestamp", current_timestamp())
    
    # Deduplicate
    window_spec = Window.partitionBy("ProductID").orderBy(col("ModifiedDate").desc())
    silver_product = silver_product \
        .withColumn("_row_num", row_number().over(window_spec)) \
        .filter(col("_row_num") == 1) \
        .drop("_row_num")
    
    row_count = silver_product.count()
    logger.info(f"  Built silver_Product: {row_count:,} rows")
    
    return silver_product

silver_product = build_silver_product()
silver_product.write.format("delta").mode("overwrite").saveAsTable(f"{TARGET_LAKEHOUSE}.{get_silver_table('Product')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer: ProductCategory

# COMMAND ----------

def build_silver_product_category():
    """Transform Bronze ProductCategory to Silver"""
    logger.info("Building silver_ProductCategory...")
    
    bronze_category = spark.read.table(get_bronze_table("ProductCategory"))
    
    silver_category = bronze_category.select(
        col("ProductCategoryID"),
        col("ParentProductCategoryID"),
        lit("Unknown").alias("Name"),
        col("ModifiedDate"),
        col("_ingest_timestamp")
    )
    
    # Add hierarchy level
    silver_category = silver_category \
        .withColumn("_is_top_level", col("ParentProductCategoryID").isNull()) \
        .withColumn("_processing_timestamp", current_timestamp())
    
    row_count = silver_category.count()
    logger.info(f"  Built silver_ProductCategory: {row_count:,} rows")
    
    return silver_category

silver_category = build_silver_product_category()
silver_category.write.format("delta").mode("overwrite").saveAsTable(f"{TARGET_LAKEHOUSE}.{get_silver_table('ProductCategory')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer: ProductModel

# COMMAND ----------

def build_silver_product_model():
    """Transform Bronze ProductModel to Silver"""
    logger.info("Building silver_ProductModel...")
    
    bronze_model = spark.read.table(get_bronze_table("ProductModel"))
    
    silver_model = bronze_model.select(
        col("ProductModelID"),
        lit("Unknown").alias("Name"),
        col("ModifiedDate"),
        col("_ingest_timestamp"),
        current_timestamp().alias("_processing_timestamp")
    )
    
    row_count = silver_model.count()
    logger.info(f"  Built silver_ProductModel: {row_count:,} rows")
    
    return silver_model

silver_model = build_silver_product_model()
silver_model.write.format("delta").mode("overwrite").saveAsTable(f"{TARGET_LAKEHOUSE}.{get_silver_table('ProductModel')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer: Address

# COMMAND ----------

def build_silver_address():
    """Transform Bronze Address to Silver with data quality"""
    logger.info("Building silver_Address...")
    
    bronze_address = spark.read.table(get_bronze_table("Address"))
    
    silver_address = bronze_address.select(
        col("AddressID"),
        trim(col("AddressLine1")).alias("AddressLine1"),
        trim(col("AddressLine2")).alias("AddressLine2"),
        initcap(trim(col("City"))).alias("City"),
        upper(trim(col("PostalCode"))).alias("PostalCode"),
        col("ModifiedDate"),
        col("_ingest_timestamp")
    )
    
    # Add quality flags
    silver_address = silver_address \
        .withColumn("_has_complete_address", 
                   col("AddressLine1").isNotNull() & 
                   col("City").isNotNull() & 
                   col("PostalCode").isNotNull()) \
        .withColumn("_processing_timestamp", current_timestamp())
    
    # Deduplicate
    window_spec = Window.partitionBy("AddressID").orderBy(col("ModifiedDate").desc())
    silver_address = silver_address \
        .withColumn("_row_num", row_number().over(window_spec)) \
        .filter(col("_row_num") == 1) \
        .drop("_row_num")
    
    row_count = silver_address.count()
    logger.info(f"  Built silver_Address: {row_count:,} rows")
    
    return silver_address

silver_address = build_silver_address()
silver_address.write.format("delta").mode("overwrite").saveAsTable(f"{TARGET_LAKEHOUSE}.{get_silver_table('Address')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer: SalesOrderHeader

# COMMAND ----------

def build_silver_sales_order_header():
    """Transform Bronze SalesOrderHeader to Silver with validation"""
    logger.info("Building silver_SalesOrderHeader...")
    
    bronze_header = spark.read.table(get_bronze_table("SalesOrderHeader"))
    
    silver_header = bronze_header.select(
        col("SalesOrderID"),
        col("RevisionNumber"),
        col("OrderDate"),
        col("DueDate"),
        col("ShipDate"),
        col("Status"),
        col("CustomerID"),
        col("ShipToAddressID"),
        col("BillToAddressID"),
        trim(col("ShipMethod")).alias("ShipMethod"),
        col("CreditCardApprovalCode"),
        col("SubTotal"),
        col("TaxAmt"),
        col("Freight"),
        col("Comment"),
        col("ModifiedDate"),
        col("_ingest_timestamp")
    )
    
    # Add calculated fields and validation
    silver_header = silver_header \
        .withColumn("TotalDue", col("SubTotal") + col("TaxAmt") + col("Freight")) \
        .withColumn("_is_shipped", col("ShipDate").isNotNull()) \
        .withColumn("_days_to_ship", 
                   when(col("ShipDate").isNotNull(), 
                        datediff(col("ShipDate"), col("OrderDate")))
                   .otherwise(None)) \
        .withColumn("_is_overdue",
                   (col("ShipDate").isNull()) & (col("DueDate") < current_date())) \
        .withColumn("_has_valid_amounts",
                   (col("SubTotal") > 0) & (col("TaxAmt") >= 0) & (col("Freight") >= 0)) \
        .withColumn("_processing_timestamp", current_timestamp())
    
    row_count = silver_header.count()
    logger.info(f"  Built silver_SalesOrderHeader: {row_count:,} rows")
    
    return silver_header

silver_header = build_silver_sales_order_header()
silver_header.write.format("delta").mode("overwrite").saveAsTable(f"{TARGET_LAKEHOUSE}.{get_silver_table('SalesOrderHeader')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer: SalesOrderDetail

# COMMAND ----------

def build_silver_sales_order_detail():
    """Transform Bronze SalesOrderDetail to Silver with validation"""
    logger.info("Building silver_SalesOrderDetail...")
    
    bronze_detail = spark.read.table(get_bronze_table("SalesOrderDetail"))
    
    silver_detail = bronze_detail.select(
        col("SalesOrderID"),
        col("SalesOrderDetailID"),
        col("OrderQty"),
        col("ProductID"),
        col("UnitPrice"),
        col("UnitPriceDiscount"),
        col("ModifiedDate"),
        col("_ingest_timestamp")
    )
    
    # Add calculated fields and validation
    silver_detail = silver_detail \
        .withColumn("LineTotal", 
                   col("OrderQty") * col("UnitPrice") * (lit(1) - col("UnitPriceDiscount"))) \
        .withColumn("DiscountAmount",
                   col("OrderQty") * col("UnitPrice") * col("UnitPriceDiscount")) \
        .withColumn("_has_valid_quantities", col("OrderQty") > 0) \
        .withColumn("_has_valid_pricing", 
                   (col("UnitPrice") > 0) & (col("UnitPriceDiscount") >= 0) & (col("UnitPriceDiscount") < 1)) \
        .withColumn("_has_discount", col("UnitPriceDiscount") > 0) \
        .withColumn("_processing_timestamp", current_timestamp())
    
    row_count = silver_detail.count()
    logger.info(f"  Built silver_SalesOrderDetail: {row_count:,} rows")
    
    return silver_detail

silver_detail = build_silver_sales_order_detail()
silver_detail.write.format("delta").mode("overwrite").saveAsTable(f"{TARGET_LAKEHOUSE}.{get_silver_table('SalesOrderDetail')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer Validation & Summary

# COMMAND ----------

logger.info("\n" + "=" * 70)
logger.info("SILVER LAYER SUMMARY")
logger.info("=" * 70)

silver_tables = [
    "Customer", "Product", "ProductCategory", "ProductModel",
    "Address", "SalesOrderHeader", "SalesOrderDetail"
]

for table in silver_tables:
    silver_table_name = get_silver_table(table)
    try:
        df = spark.read.table(f"{TARGET_LAKEHOUSE}.{silver_table_name}")
        count = df.count()
        cols = len(df.columns)
        
        # Count quality flag columns
        quality_cols = [c for c in df.columns if c.startswith("_")]
        logger.info(f"{silver_table_name}: {count:,} rows, {cols} columns ({len(quality_cols)} quality flags)")
    except Exception as e:
        logger.error(f"{silver_table_name}: ERROR - {str(e)}")

logger.info("\n✓ Silver layer transformation complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Report

# COMMAND ----------

# Customer data quality
logger.info("\nDATA QUALITY METRICS")
logger.info("-" * 70)

customer_df = spark.read.table(f"{TARGET_LAKEHOUSE}.{get_silver_table('Customer')}")
total_customers = customer_df.count()
valid_emails = customer_df.filter(col("_is_valid_email")).count()
has_company = customer_df.filter(col("_has_company")).count()

logger.info(f"Customers with valid email: {valid_emails}/{total_customers} ({valid_emails/total_customers*100:.1f}%)")
logger.info(f"Customers with company: {has_company}/{total_customers} ({has_company/total_customers*100:.1f}%)")

# Product data quality
product_df = spark.read.table(f"{TARGET_LAKEHOUSE}.{get_silver_table('Product')}")
total_products = product_df.count()
active_products = product_df.filter(col("_is_active")).count()
valid_pricing = product_df.filter(col("_has_valid_pricing")).count()

logger.info(f"Active products: {active_products}/{total_products} ({active_products/total_products*100:.1f}%)")
logger.info(f"Products with valid pricing: {valid_pricing}/{total_products} ({valid_pricing/total_products*100:.1f}%)")

# Order data quality
header_df = spark.read.table(f"{TARGET_LAKEHOUSE}.{get_silver_table('SalesOrderHeader')}")
total_orders = header_df.count()
shipped_orders = header_df.filter(col("_is_shipped")).count()
overdue_orders = header_df.filter(col("_is_overdue")).count()

logger.info(f"Shipped orders: {shipped_orders}/{total_orders} ({shipped_orders/total_orders*100:.1f}%)")
logger.info(f"Overdue orders: {overdue_orders}/{total_orders} ({overdue_orders/total_orders*100:.1f}%)")
