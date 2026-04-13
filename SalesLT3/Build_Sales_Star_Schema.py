# Databricks notebook source
# MAGIC %md
# MAGIC # Sales Star Schema ETL Pipeline
# MAGIC 
# MAGIC **Purpose:** Build dimensional star schema for sales analytics from mirrored SalesLT database
# MAGIC 
# MAGIC **Source:** `RvDSQL.MountedRelationalDatabase` (SalesLT schema)
# MAGIC 
# MAGIC **Target:** `SalesAnalytics.Lakehouse` (Delta tables)
# MAGIC 
# MAGIC **Schema:**
# MAGIC - **Dimensions:** Dim_Date, Dim_Customer, Dim_Product, Dim_Address
# MAGIC - **Fact:** Fact_Sales
# MAGIC 
# MAGIC **Load Order:** Date → Customer → Product → Address → Fact Sales

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration and Setup

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
WORKSPACE_ID = "SalesLT"  # Update with actual workspace ID if different
SOURCE_ITEM = "RvDSQL.MountedRelationalDatabase"
TARGET_LAKEHOUSE = "SalesAnalytics"
SOURCE_SCHEMA = "SalesLT"

# Source table paths (mirrored database via OneLake)
def get_source_table_path(table_name):
    return f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{SOURCE_ITEM}/Tables/{SOURCE_SCHEMA}/{table_name}"

# Target lakehouse paths
def get_target_table_path(table_name):
    return f"Tables/{table_name}"

logger.info("Configuration loaded successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Utility Functions

# COMMAND ----------

def log_row_count(df, table_name, step=""):
    """Log row count for data quality tracking"""
    count = df.count()
    logger.info(f"{step} {table_name}: {count:,} rows")
    return count

def write_delta_table(df, table_name, mode="overwrite"):
    """Write DataFrame to Delta table in target lakehouse"""
    start_time = datetime.now()
    logger.info(f"Writing {table_name} in {mode} mode...")
    
    df.write \
        .format("delta") \
        .mode(mode) \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"{TARGET_LAKEHOUSE}.{table_name}")
    
    duration = (datetime.now() - start_time).total_seconds()
    row_count = log_row_count(df, table_name, "Wrote")
    logger.info(f"{table_name} write completed in {duration:.2f} seconds")
    
    return row_count

def validate_foreign_keys(fact_df, dim_df, fact_key, dim_key, dimension_name):
    """Validate foreign key relationships"""
    logger.info(f"Validating {fact_key} → {dimension_name}.{dim_key}")
    
    # Check for orphaned records (nulls are allowed for optional FKs)
    orphaned = fact_df.filter(col(fact_key).isNotNull()) \
        .join(dim_df, fact_df[fact_key] == dim_df[dim_key], "left_anti") \
        .count()
    
    if orphaned > 0:
        logger.warning(f"Found {orphaned} orphaned records in {fact_key}")
    else:
        logger.info(f"✓ All {fact_key} values valid")
    
    return orphaned == 0

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Build Dim_Date (Date Dimension)
# MAGIC 
# MAGIC Generate comprehensive date dimension: 2000-01-01 to 2030-12-31

# COMMAND ----------

def generate_date_dimension(start_date='2000-01-01', end_date='2030-12-31'):
    """Generate Dim_Date with calendar and fiscal attributes"""
    logger.info("Generating Dim_Date...")
    
    # Generate date range
    from datetime import datetime, timedelta
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    
    date_list = []
    current = start
    while current <= end:
        date_list.append((current.strftime('%Y-%m-%d'),))
        current += timedelta(days=1)
    
    # Create DataFrame
    date_df = spark.createDataFrame(date_list, ["DateString"])
    
    # Convert to date and add attributes
    dim_date = date_df.select(
        to_date(col("DateString")).alias("Date")
    ).withColumn(
        "DateKey", date_format(col("Date"), "yyyyMMdd").cast("int")
    ).withColumn(
        "Year", year(col("Date"))
    ).withColumn(
        "Quarter", quarter(col("Date"))
    ).withColumn(
        "QuarterName", concat(lit("Q"), col("Quarter"))
    ).withColumn(
        "Month", month(col("Date"))
    ).withColumn(
        "MonthName", date_format(col("Date"), "MMMM")
    ).withColumn(
        "MonthNameShort", date_format(col("Date"), "MMM")
    ).withColumn(
        "DayOfMonth", dayofmonth(col("Date"))
    ).withColumn(
        # ISO 8601: Monday = 1, Sunday = 7
        "DayOfWeek", ((dayofweek(col("Date")) + 5) % 7) + 1
    ).withColumn(
        "DayOfWeekName", date_format(col("Date"), "EEEE")
    ).withColumn(
        "DayOfWeekNameShort", date_format(col("Date"), "EEE")
    ).withColumn(
        "DayOfYear", dayofyear(col("Date"))
    ).withColumn(
        "WeekOfYear", weekofyear(col("Date"))
    ).withColumn(
        "IsWeekend", col("DayOfWeek").isin([6, 7])
    ).withColumn(
        "IsHoliday", lit(False)  # Placeholder for future holiday definitions
    ).withColumn(
        # Fiscal year (default: same as calendar year, starts in January)
        "FiscalYear", col("Year")
    ).withColumn(
        "FiscalQuarter", col("Quarter")
    ).withColumn(
        "FiscalMonth", col("Month")
    )
    
    # Select final columns in order
    dim_date = dim_date.select(
        "DateKey", "Date", "Year", "Quarter", "QuarterName",
        "Month", "MonthName", "MonthNameShort",
        "DayOfMonth", "DayOfWeek", "DayOfWeekName", "DayOfWeekNameShort",
        "DayOfYear", "WeekOfYear", "IsWeekend", "IsHoliday",
        "FiscalYear", "FiscalQuarter", "FiscalMonth"
    )
    
    log_row_count(dim_date, "Dim_Date", "Generated")
    return dim_date

# Build and write Dim_Date
dim_date = generate_date_dimension()
write_delta_table(dim_date, "Dim_Date")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Build Dim_Customer (Customer Dimension)

# COMMAND ----------

def build_customer_dimension():
    """Build Dim_Customer from source Customer table"""
    logger.info("Building Dim_Customer...")
    
    # Read source Customer table
    customer_src = spark.read.format("delta").load(get_source_table_path("Customer"))
    log_row_count(customer_src, "Source Customer", "Read")
    
    # Transform to dimension
    # Note: Source doesn't have separate FirstName/LastName fields in this schema
    dim_customer = customer_src.select(
        col("CustomerID"),
        col("Title"),
        col("Suffix"),
        col("CompanyName"),
        col("SalesPerson"),
        col("EmailAddress"),
        col("ModifiedDate")
    )
    
    # Add surrogate key
    window_spec = Window.orderBy("CustomerID")
    dim_customer = dim_customer.withColumn(
        "CustomerKey", row_number().over(window_spec)
    )
    
    # Add SCD Type 2 columns
    current_date = current_date()
    dim_customer = dim_customer \
        .withColumn("IsCurrent", lit(True)) \
        .withColumn("EffectiveDate", current_date) \
        .withColumn("EndDate", lit(None).cast("date")) \
        .withColumn("FirstName", lit(None).cast("string")) \
        .withColumn("LastName", lit(None).cast("string"))
    
    # Select final columns
    dim_customer = dim_customer.select(
        "CustomerKey", "CustomerID", "Title", "FirstName", "LastName", "Suffix",
        "CompanyName", "SalesPerson", "EmailAddress", "ModifiedDate",
        "IsCurrent", "EffectiveDate", "EndDate"
    )
    
    log_row_count(dim_customer, "Dim_Customer", "Built")
    return dim_customer

# Build and write Dim_Customer
dim_customer = build_customer_dimension()
write_delta_table(dim_customer, "Dim_Customer")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Build Dim_Product (Product Dimension with Denormalized Hierarchy)

# COMMAND ----------

def build_product_dimension():
    """Build Dim_Product with denormalized category and model hierarchy"""
    logger.info("Building Dim_Product...")
    
    # Read source tables
    product_src = spark.read.format("delta").load(get_source_table_path("Product"))
    category_src = spark.read.format("delta").load(get_source_table_path("ProductCategory"))
    model_src = spark.read.format("delta").load(get_source_table_path("ProductModel"))
    
    # Try to read ProductDescription and bridge table (may not exist in all environments)
    try:
        desc_src = spark.read.format("delta").load(get_source_table_path("ProductDescription"))
        bridge_src = spark.read.format("delta").load(get_source_table_path("ProductModelProductDescription"))
        
        # Get English product descriptions
        product_desc = bridge_src \
            .filter(col("Culture") == "en") \
            .join(desc_src, "ProductDescriptionID") \
            .select("ProductModelID", col("Description").alias("ProductName"))
        
        has_descriptions = True
    except:
        logger.warning("ProductDescription tables not found, ProductName will be NULL")
        has_descriptions = False
    
    log_row_count(product_src, "Source Product", "Read")
    log_row_count(category_src, "Source ProductCategory", "Read")
    
    # Self-join ProductCategory for parent categories
    parent_category = category_src.select(
        col("ProductCategoryID").alias("ParentCategoryID"),
        col("Name").alias("ParentProductCategoryName")
    )
    
    category_with_parent = category_src \
        .join(parent_category, 
              category_src.ParentProductCategoryID == parent_category.ParentCategoryID,
              "left") \
        .select(
            category_src.ProductCategoryID,
            col("Name").alias("ProductCategoryName"),
            category_src.ParentProductCategoryID,
            "ParentProductCategoryName"
        )
    
    # Join Product → Category → Parent Category → Model
    dim_product = product_src \
       .join(category_with_parent, "ProductCategoryID", "left") \
        .join(model_src.select("ProductModelID"), "ProductModelID", "left")
    
    # Add product descriptions if available
    if has_descriptions:
        dim_product = dim_product.join(product_desc, "ProductModelID", "left")
    else:
        dim_product = dim_product.withColumn("ProductName", lit(None).cast("string"))
    
    # Add calculated fields
    dim_product = dim_product \
        .withColumn("IsDiscontinued", col("DiscontinuedDate").isNotNull()) \
        .withColumn("ProductModelName", lit(None).cast("string"))  # Model name placeholder
    
    # Add surrogate key
    window_spec = Window.orderBy("ProductID")
    dim_product = dim_product.withColumn(
        "ProductKey", row_number().over(window_spec)
    )
    
    # Add SCD Type 2 columns
    current_date_val = current_date()
    dim_product = dim_product \
        .withColumn("IsCurrent", lit(True)) \
        .withColumn("EffectiveDate", current_date_val) \
        .withColumn("EndDate", lit(None).cast("date"))
    
    # Select final columns
    dim_product = dim_product.select(
        "ProductKey", "ProductID", "ProductNumber", "ProductName",
        "Color", "Size", "Weight", "StandardCost", "ListPrice",
        "ProductCategoryID", "ProductCategoryName",
        "ParentProductCategoryID", "ParentProductCategoryName",
        "ProductModelID", "ProductModelName",
        "SellStartDate", "SellEndDate", "DiscontinuedDate", "IsDiscontinued",
        "ModifiedDate", "IsCurrent", "EffectiveDate", "EndDate"
    )
    
    log_row_count(dim_product, "Dim_Product", "Built")
    return dim_product

# Build and write Dim_Product
dim_product = build_product_dimension()
write_delta_table(dim_product, "Dim_Product")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Build Dim_Address (Address Dimension)

# COMMAND ----------

def build_address_dimension():
    """Build Dim_Address from source Address table"""
    logger.info("Building Dim_Address...")
    
    # Read source Address table
    address_src = spark.read.format("delta").load(get_source_table_path("Address"))
    log_row_count(address_src, "Source Address", "Read")
    
    # Transform to dimension
    dim_address = address_src.select(
        col("AddressID"),
        col("AddressLine1"),
        col("AddressLine2"),
        col("City"),
        col("StateProvince"),
        col("CountryRegion"),
        col("PostalCode"),
        col("ModifiedDate")
    )
    
    # Add surrogate key
    window_spec = Window.orderBy("AddressID")
    dim_address = dim_address.withColumn(
        "AddressKey", row_number().over(window_spec)
    )
    
    # Add SCD Type 2 columns
    current_date_val = current_date()
    dim_address = dim_address \
        .withColumn("IsCurrent", lit(True)) \
        .withColumn("EffectiveDate", current_date_val) \
        .withColumn("EndDate", lit(None).cast("date")) \
        .withColumn("StateProvince", lit(None).cast("string")) \
        .withColumn("CountryRegion", lit(None).cast("string"))
    
    # Create "Unknown" address row
    unknown_address = spark.createDataFrame([
        (-1, -1, "Unknown", None, "Unknown", None, None, "00000", 
         datetime.now(), True, datetime.now().date(), None)
    ], ["AddressKey", "AddressID", "AddressLine1", "AddressLine2", "City",
        "StateProvince", "CountryRegion", "PostalCode", "ModifiedDate",
        "IsCurrent", "EffectiveDate", "EndDate"])
    
    # Combine regular addresses with unknown address
    dim_address = dim_address.select(
        "AddressKey", "AddressID", "AddressLine1", "AddressLine2", "City",
        "StateProvince", "CountryRegion", "PostalCode", "ModifiedDate",
        "IsCurrent", "EffectiveDate", "EndDate"
    ).union(unknown_address)
    
    log_row_count(dim_address, "Dim_Address", "Built")
    return dim_address

# Build and write Dim_Address
dim_address = build_address_dimension()
write_delta_table(dim_address, "Dim_Address")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Build Fact_Sales (Sales Fact Table)

# COMMAND ----------

def build_fact_sales():
    """Build Fact_Sales from SalesOrderDetail and SalesOrderHeader"""
    logger.info("Building Fact_Sales...")
    
    # Read source fact tables
    order_detail = spark.read.format("delta").load(get_source_table_path("SalesOrderDetail"))
    order_header = spark.read.format("delta").load(get_source_table_path("SalesOrderHeader"))
    product_src = spark.read.format("delta").load(get_source_table_path("Product"))
    
    log_row_count(order_detail, "Source SalesOrderDetail", "Read")
    log_row_count(order_header, "Source SalesOrderHeader", "Read")
    
    # Read dimensions for lookups (from lakehouse)
    dim_customer_lookup = spark.read.table(f"{TARGET_LAKEHOUSE}.Dim_Customer")
    dim_product_lookup = spark.read.table(f"{TARGET_LAKEHOUSE}.Dim_Product")
    dim_date_lookup = spark.read.table(f"{TARGET_LAKEHOUSE}.Dim_Date")
    dim_address_lookup = spark.read.table(f"{TARGET_LAKEHOUSE}.Dim_Address")
    
    # Join SalesOrderDetail with SalesOrderHeader
    fact_sales = order_detail.join(order_header, "SalesOrderID")
    
    # Join with Product to get StandardCost for profit calculation
    fact_sales = fact_sales.join(
        product_src.select("ProductID", col("StandardCost").alias("ProductStandardCost")),
        "ProductID"
    )
    
    # Calculate measures
    fact_sales = fact_sales \
        .withColumn("LineTotal", 
                   col("OrderQty") * col("UnitPrice") * (lit(1) - col("UnitPriceDiscount"))) \
        .withColumn("LineProfit",
                   col("LineTotal") - (col("OrderQty") * col("ProductStandardCost"))) \
        .withColumn("OrderTotal",
                   col("SubTotal") + col("TaxAmt") + col("Freight"))
    
    # Lookup dimension keys - Customer
    fact_sales = fact_sales.join(
        dim_customer_lookup.select("CustomerID", "CustomerKey"),
        "CustomerID", "inner"
    )
    
    # Lookup dimension keys - Product
    fact_sales = fact_sales.join(
        dim_product_lookup.select("ProductID", "ProductKey"),
        "ProductID", "inner"
    )
    
    # Lookup dimension keys - Order Date
    fact_sales = fact_sales.join(
        dim_date_lookup.select(col("Date").cast("date").alias("OrderDate"), 
                               col("DateKey").alias("OrderDateKey")),
        to_date(fact_sales.OrderDate) == col("OrderDate"), "inner"
    ).drop("OrderDate")
    
    # Lookup dimension keys - Due Date
    fact_sales = fact_sales.join(
        dim_date_lookup.select(col("Date").cast("date").alias("DueDate"),
                               col("DateKey").alias("DueDateKey")),
        to_date(fact_sales.DueDate) == col("DueDate"), "inner"
    ).drop("DueDate")
    
    # Lookup dimension keys - Ship Date (nullable)
    fact_sales = fact_sales.join(
        dim_date_lookup.select(col("Date").cast("date").alias("ShipDate"),
                               col("DateKey").alias("ShipDateKey")),
        to_date(fact_sales.ShipDate) == col("ShipDate"), "left"
    ).drop("ShipDate")
    
    # Lookup dimension keys - ShipToAddress (nullable)
    fact_sales = fact_sales.join(
        dim_address_lookup.select(col("AddressID").alias("ShipToAddressID"),
                                  col("AddressKey").alias("ShipToAddressKey")),
        "ShipToAddressID", "left"
    )
    
    # Lookup dimension keys - BillToAddress (nullable)
    fact_sales = fact_sales.join(
        dim_address_lookup.select(col("AddressID").alias("BillToAddressID"),
                                  col("AddressKey").alias("BillToAddressKey")),
        "BillToAddressID", "left"
    )
    
    # Add surrogate key
    window_spec = Window.orderBy("SalesOrderID", "SalesOrderDetailID")
    fact_sales = fact_sales.withColumn(
        "SalesKey", row_number().over(window_spec)
    )
    
    # Select final fact columns
    fact_sales = fact_sales.select(
        "SalesKey",
        "SalesOrderID",
        "SalesOrderDetailID",
        "CustomerKey",
        "ProductKey",
        "OrderDateKey",
        "DueDateKey",
        "ShipDateKey",
        "ShipToAddressKey",
        "BillToAddressKey",
        "OrderQty",
        "UnitPrice",
        "UnitPriceDiscount",
        "LineTotal",
        "ProductStandardCost",
        "LineProfit",
        col("SubTotal").alias("OrderSubTotal"),
        col("TaxAmt").alias("OrderTaxAmt"),
        col("Freight").alias("OrderFreight"),
        "OrderTotal",
        col("Status").alias("OrderStatus"),
        "ShipMethod",
        "RevisionNumber"
    )
    
    log_row_count(fact_sales, "Fact_Sales", "Built")
    return fact_sales

# Build and write Fact_Sales
fact_sales = build_fact_sales()
write_delta_table(fact_sales, "Fact_Sales")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Data Validation

# COMMAND ----------

logger.info("=" * 60)
logger.info("DATA VALIDATION")
logger.info("=" * 60)

# Reload dimensions and fact for validation
dim_customer_val = spark.read.table(f"{TARGET_LAKEHOUSE}.Dim_Customer")
dim_product_val = spark.read.table(f"{TARGET_LAKEHOUSE}.Dim_Product")
dim_date_val = spark.read.table(f"{TARGET_LAKEHOUSE}.Dim_Date")
dim_address_val = spark.read.table(f"{TARGET_LAKEHOUSE}.Dim_Address")
fact_sales_val = spark.read.table(f"{TARGET_LAKEHOUSE}.Fact_Sales")

# 1. Check dimension uniqueness
logger.info("\n1. Checking surrogate key uniqueness...")
for dim_name, dim_df, key_col in [
    ("Dim_Customer", dim_customer_val, "CustomerKey"),
    ("Dim_Product", dim_product_val, "ProductKey"),
    ("Dim_Date", dim_date_val, "DateKey"),
    ("Dim_Address", dim_address_val, "AddressKey")
]:
    total_rows = dim_df.count()
    distinct_keys = dim_df.select(key_col).distinct().count()
    if total_rows == distinct_keys:
        logger.info(f"✓ {dim_name}.{key_col}: {distinct_keys:,} unique keys")
    else:
        logger.error(f"✗ {dim_name}.{key_col}: {total_rows:,} rows but only {distinct_keys:,} unique keys!")

# 2. Validate foreign key relationships
logger.info("\n2. Validating foreign key relationships...")
validate_foreign_keys(fact_sales_val, dim_customer_val, "CustomerKey", "CustomerKey", "Dim_Customer")
validate_foreign_keys(fact_sales_val, dim_product_val, "ProductKey", "ProductKey", "Dim_Product")
validate_foreign_keys(fact_sales_val, dim_date_val, "OrderDateKey", "DateKey", "Dim_Date")
validate_foreign_keys(fact_sales_val, dim_date_val, "DueDateKey", "DateKey", "Dim_Date")

# 3. Validate calculated measures
logger.info("\n3. Validating calculated measures...")
sample_fact = fact_sales_val.limit(5).collect()
for row in sample_fact:
    # Validate LineTotal = OrderQty * UnitPrice * (1 - UnitPriceDiscount)
    expected_line_total = float(row.OrderQty) * float(row.UnitPrice) * (1 - float(row.UnitPriceDiscount))
    actual_line_total = float(row.LineTotal)
    if abs(expected_line_total - actual_line_total) < 0.01:
        logger.info(f"✓ SalesKey {row.SalesKey}: LineTotal calculation correct")
    else:
        logger.error(f"✗ SalesKey {row.SalesKey}: LineTotal mismatch! Expected {expected_line_total}, got {actual_line_total}")

# 4. Compare aggregates with source
logger.info("\n4. Comparing aggregates with source...")
source_detail = spark.read.format("delta").load(get_source_table_path("SalesOrderDetail"))
source_count = source_detail.count()
fact_count = fact_sales_val.count()

if source_count == fact_count:
    logger.info(f"✓ Row count match: {fact_count:,} rows in both source and fact")
else:
    logger.warning(f"Row count mismatch! Source: {source_count:,}, Fact: {fact_count:,}")

# 5. Summary statistics
logger.info("\n5. Summary Statistics...")
logger.info("-" * 60)
logger.info(f"Dim_Date rows: {dim_date_val.count():,}")
logger.info(f"Dim_Customer rows: {dim_customer_val.count():,}")
logger.info(f"Dim_Product rows: {dim_product_val.count():,}")
logger.info(f"Dim_Address rows: {dim_address_val.count():,}")
logger.info(f"Fact_Sales rows: {fact_sales_val.count():,}")
logger.info("-" * 60)

# Total revenue
total_revenue = fact_sales_val.agg(sum("LineTotal")).collect()[0][0]
logger.info(f"Total Revenue: ${total_revenue:,.2f}")

# Total profit
total_profit = fact_sales_val.agg(sum("LineProfit")).collect()[0][0]
logger.info(f"Total Profit: ${total_profit:,.2f}")

logger.info("\n✓ Star schema build and validation complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Sample Analytic Queries

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Total revenue by year
# MAGIC SELECT 
# MAGIC     d.Year,
# MAGIC     COUNT(DISTINCT f.SalesOrderID) as OrderCount,
# MAGIC     SUM(f.LineTotal) as TotalRevenue,
# MAGIC     SUM(f.LineProfit) as TotalProfit
# MAGIC FROM SalesAnalytics.Fact_Sales f
# MAGIC JOIN SalesAnalytics.Dim_Date d ON f.OrderDateKey = d.DateKey
# MAGIC GROUP BY d.Year
# MAGIC ORDER BY d.Year;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top 10 products by revenue
# MAGIC SELECT 
# MAGIC     p.ProductName,
# MAGIC     p.ProductCategoryName,
# MAGIC     p.ParentProductCategoryName,
# MAGIC     SUM(f.OrderQty) as TotalQuantity,
# MAGIC     SUM(f.LineTotal) as TotalRevenue,
# MAGIC     SUM(f.LineProfit) as TotalProfit
# MAGIC FROM SalesAnalytics.Fact_Sales f
# MAGIC JOIN SalesAnalytics.Dim_Product p ON f.ProductKey = p.ProductKey
# MAGIC GROUP BY p.ProductName, p.ProductCategoryName, p.ParentProductCategoryName
# MAGIC ORDER BY TotalRevenue DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Revenue by product category
# MAGIC SELECT 
# MAGIC     COALESCE(p.ParentProductCategoryName, p.ProductCategoryName) as Category,
# MAGIC     COUNT(DISTINCT f.SalesOrderID) as OrderCount,
# MAGIC     SUM(f.OrderQty) as TotalQuantity,
# MAGIC     SUM(f.LineTotal) as TotalRevenue
# MAGIC FROM SalesAnalytics.Fact_Sales f
# MAGIC JOIN SalesAnalytics.Dim_Product p ON f.ProductKey = p.ProductKey
# MAGIC GROUP BY COALESCE(p.ParentProductCategoryName, p.ProductCategoryName)
# MAGIC ORDER BY TotalRevenue DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Monthly sales trend
# MAGIC SELECT 
# MAGIC     d.Year,
# MAGIC     d.Month,
# MAGIC     d.MonthName,
# MAGIC     COUNT(DISTINCT f.SalesOrderID) as OrderCount,
# MAGIC     SUM(f.LineTotal) as TotalRevenue
# MAGIC FROM SalesAnalytics.Fact_Sales f
# MAGIC JOIN SalesAnalytics.Dim_Date d ON f.OrderDateKey = d.DateKey
# MAGIC GROUP BY d.Year, d.Month, d.MonthName
# MAGIC ORDER BY d.Year, d.Month;
