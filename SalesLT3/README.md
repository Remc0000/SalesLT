# SalesLT Medallion Architecture

**End-to-end data pipeline implementing Bronze → Silver → Gold medallion architecture for sales analytics**

## 📊 Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│ SOURCE: Mirrored Database (External)                            │
│ RvDSQL.MountedRelationalDatabase (SalesLT schema)              │
└───────────────────────┬─────────────────────────────────────────┘
                        │ Raw Ingestion
                        ↓
┌─────────────────────────────────────────────────────────────────┐
│ BRONZE LAYER: Raw Data Copy                                     │
│ • Minimal transformation                                        │
│ • Audit columns: _ingest_timestamp, _source_system             │
│ • Full historical preservation                                 │
│ • 10 tables ingested                                            │
└───────────────────────┬─────────────────────────────────────────┘
                        │ Cleansing & Validation
                        ↓
┌─────────────────────────────────────────────────────────────────┐
│ SILVER LAYER: Cleaned & Validated                              │
│ • Data quality checks and flags                                │
│ • Deduplication                                                 │
│ • Business rule application                                    │
│ • Calculated fields                                             │
│ • Type standardization                                          │
└───────────────────────┬─────────────────────────────────────────┘
                        │ Dimensional Modeling
                        ↓
┌─────────────────────────────────────────────────────────────────┐
│ GOLD LAYER: Star Schema for Analytics                          │
│ • Fact_Sales (542 rows, grain = order line)                    │
│ • Dim_Date (11,300 rows, 2000-2030)                            │
│ • Dim_Customer (847 rows)                                       │
│ • Dim_Product (295 rows, denormalized hierarchy)               │
│ • Dim_Address (450 rows)                                        │
└───────────────────────┬─────────────────────────────────────────┘
                        │ Consumption
                        ↓
             ┌──────────┴──────────┐
             │                     │
    ┌────────▼─────┐     ┌────────▼─────┐
    │  Power BI    │     │  SQL Analytics│
    │  Dashboards  │     │  & ML Workloads│
    └──────────────┘     └──────────────────┘
```

## 📁 Project Structure

```
SalesLT3/
├── 00_Orchestrator_Medallion.py      # Main pipeline orchestration
├── 01_Bronze_Layer_Ingestion.py     # Bronze: Raw data ingestion
├── 02_Silver_Layer_Transform.py     # Silver: Data quality & cleansing
├── 03_Gold_Layer_StarSchema.py      # Gold: Dimensional model
├── Build_Sales_Star_Schema.py       # DEPRECATED: Legacy single-step approach
├── openspec/
│   └── changes/
│       └── sales-star-schema/        # OpenSpec specifications
│           ├── proposal.md           # Project proposal
│           ├── design.md             # Technical design
│           ├── tasks.md              # Implementation tasks
│           └── specs/                # 6 dimensional specifications
└── README.md                         # This file
```

## 🎯 Implementation Details

### Bronze Layer (Raw Ingestion)
**File:** `01_Bronze_Layer_Ingestion.py`

**Purpose:** Ingest raw data from mirrored database with minimal transformation

**Tables Ingested:**
- bronze_Customer
- bronze_Product
- bronze_ProductCategory
- bronze_ProductModel
- bronze_ProductDescription
- bronze_ProductModelProductDescription
- bronze_Address
- bronze_CustomerAddress
- bronze_SalesOrderHeader
- bronze_SalesOrderDetail

**Audit Columns Added:**
- `_ingest_timestamp`: When data was ingested
- `_source_system`: Source system identifier
- `_source_schema`: Source schema name
- `_source_table`: Source table name

**Pattern:** Full load with Delta overwrite (can be enhanced to incremental)

### Silver Layer (Data Quality)
**File:** `02_Silver_Layer_Transform.py`

**Purpose:** Clean, validate, and enrich data with business rules

**Transformations:**
1. **Data Cleansing:**
   - Email validation with regex
   - Text standardization (trim, proper case)
   - NULL handling

2. **Deduplication:**
   - Keep most recent record by ModifiedDate
   - Remove duplicate natural keys

3. **Business Rules:**
   - Calculate IsActive, IsDiscontinued flags
   - Compute margin percentage
   - Derive shipping metrics (days to ship, overdue status)
   - Calculate line totals and discounts

4. **Quality Flags:**
   - `_is_valid_email`: Email format validation
   - `_has_valid_pricing`: Price logic validation
   - `_is_active`: Product availability
   - `_has_complete_address`: Address completeness
   - + more business-specific flags

**Tables Created:**
- silver_Customer
- silver_Product
- silver_ProductCategory
- silver_ProductModel
- silver_Address
- silver_SalesOrderHeader
- silver_SalesOrderDetail

### Gold Layer (Star Schema)
**File:** `03_Gold_Layer_StarSchema.py`

**Purpose:** Build dimensional model optimized for analytics

**Star Schema Design:**

#### Fact Table
- **gold_Fact_Sales** (542 rows)
  - Grain: One row per order line item
  - Measures: LineTotal, LineProfit, OrderQty, UnitPrice, Discount
  - Foreign Keys: CustomerKey, ProductKey, OrderDateKey, DueDateKey, ShipDateKey, ShipToAddressKey, BillToAddressKey

#### Dimension Tables
1. **gold_Dim_Date** (11,300 rows)
   - Date range: 2000-01-01 to 2030-12-31
   - Attributes: Year, Quarter, Month, Week, Day, Fiscal periods
   - Surrogate key: DateKey (YYYYMMDD format)

2. **gold_Dim_Customer** (847 rows)
   - Attributes: Name, Company, SalesPerson, Email
   - SCD Type 2 structure (IsCurrent, EffectiveDate, EndDate)
   - Surrogate key: CustomerKey

3. **gold_Dim_Product** (295 rows)
   - **Denormalized Hierarchy:** ProductCategory → ParentCategory
   - Attributes: ProductNumber, Color, Size, Weight, Pricing, Dates
   - Flags: IsDiscontinued
   - Surrogate key: ProductKey

4. **gold_Dim_Address** (450 rows)
   - Role-agnostic (supports ship-to and bill-to)
   - Attributes: AddressLine1/2, City, State, Country, PostalCode
   - Includes "Unknown" address (AddressKey = -1)
   - Surrogate key: AddressKey

**SCD Strategy:** Type 1 (overwrite) with structure prepared for Type 2

## 🚀 Execution

### Run Complete Pipeline
```python
# Execute orchestrator notebook
%run ./00_Orchestrator_Medallion

# Or run individual layers:
%run ./01_Bronze_Layer_Ingestion
%run ./02_Silver_Layer_Transform
%run ./03_Gold_Layer_StarSchema
```

### Configuration
Edit configuration sections in each notebook:
```python
WORKSPACE_ID = "SalesLT"
SOURCE_ITEM = "RvDSQL.MountedRelationalDatabase"
TARGET_LAKEHOUSE = "SalesAnalytics"
```

## 📊 Sample Analytics Queries

### Revenue by Year
```sql
SELECT 
    d.Year,
    COUNT(DISTINCT f.SalesOrderID) as OrderCount,
    SUM(f.LineTotal) as TotalRevenue,
    SUM(f.LineProfit) as TotalProfit
FROM SalesAnalytics.gold_Fact_Sales f
JOIN SalesAnalytics.gold_Dim_Date d ON f.OrderDateKey = d.DateKey
GROUP BY d.Year
ORDER BY d.Year;
```

### Top Products by Revenue
```sql
SELECT 
    p.ProductName,
    p.ProductCategoryName,
    p.ParentProductCategoryName,
    SUM(f.OrderQty) as TotalQuantity,
    SUM(f.LineTotal) as TotalRevenue
FROM SalesAnalytics.gold_Fact_Sales f
JOIN SalesAnalytics.gold_Dim_Product p ON f.ProductKey = p.ProductKey
GROUP BY p.ProductName, p.ProductCategoryName, p.ParentProductCategoryName
ORDER BY TotalRevenue DESC
LIMIT 10;
```

### Revenue by Product Category
```sql
SELECT 
    COALESCE(p.ParentProductCategoryName, p.ProductCategoryName) as Category,
    COUNT(DISTINCT f.SalesOrderID) as Orders,
    SUM(f.LineTotal) as Revenue
FROM SalesAnalytics.gold_Fact_Sales f
JOIN SalesAnalytics.gold_Dim_Product p ON f.ProductKey = p.ProductKey
GROUP BY COALESCE(p.ParentProductCategoryName, p.ProductCategoryName)
ORDER BY Revenue DESC;
```

## 🔍 Data Quality Metrics

**Customer Layer:**
- Valid email addresses: ~85%
- Customers with company: ~40%

**Product Layer:**
- Active products: ~90%
- Products with valid pricing: ~98%
- Average product margin: ~35%

**Sales Layer:**
- Shipped orders: ~95%
- Overdue orders: ~2%
- Orders with valid amounts: ~100%

## 📈 Business Metrics

Based on current data:
- **Total Orders:** 32
- **Total Line Items:** 542
- **Total Revenue:** $1.4M (estimated)
- **Total Profit:** $490K (estimated)
- **Profit Margin:** ~35%
- **Average Order Value:** ~$44K

## 🛠️ Technology Stack

- **Platform:** Microsoft Fabric
- **Compute:** Spark (PySpark)
- **Storage:** OneLake (Delta Lake format)
- **Source:** Mirrored SQL Database
- **Target:** Lakehouse (SalesAnalytics)

## 📝 Design Decisions

### Why Medallion Architecture?
1. **Separation of Concerns:** Each layer has a clear purpose
2. **Data Quality:** Progressive refinement from raw to curated
3. **Flexibility:** Can consume from any layer based on needs
4. **Auditability:** Full data lineage and traceability
5. **Performance:** Optimized for different use cases per layer

### Why Bronze Layer?
- Mirrored database is **external** and controlled by source system
- Bronze provides our **first internal checkpoint**
- Enables historical tracking even if source changes
- Isolates pipeline from source system outages

### Why Star Schema in Gold?
- **Query Performance:** Denormalized for fast aggregations
- **Business Alignment:** Matches how users think about data
- **Tool Compatibility:** Works well with Power BI, Excel, etc.
- **Scalability:** Proven pattern for large-scale analytics

### Why Surrogate Keys?
- **Future-proof:** Enables SCD Type 2 without changing facts
- **Performance:** Smaller join keys (INT vs. VARCHAR)
- **Decoupling:** Insulates from source key changes

## 🔄 Refresh Strategy

**Current:** Full refresh (overwrite mode)

**Future Enhancements:**
1. **Incremental Bronze:** Use ModifiedDate or CDC
2. **Merge Silver:** Upsert pattern with Delta MERGE
3. **SCD Type 2 Gold:** Historical dimension tracking
4. **Partitioning:** By Year/Month for fact tables

## 📅 Scheduling

**Recommended Schedule:**
- **Bronze:** Hourly or daily (based on source update frequency)
- **Silver:** Every 6 hours or triggered after Bronze
- **Gold:** Daily overnight or triggered after Silver

**Implementation:** Use Fabric Data Factory pipeline or Apache Airflow

## 🎓 OpenSpec Documentation

Full specifications available in `openspec/changes/sales-star-schema/`:
- **proposal.md:** Business justification and requirements
- **design.md:** Technical architecture decisions
- **specs/:** Detailed functional specifications for each component
- **tasks.md:** Implementation checklist (94 tasks across 12 groups)

## 🔐 Security Considerations

- **Row-Level Security:** Can be added to Gold layer dimensions/facts
- **Column Masking:** PII fields (email, etc.) can be masked
- **Audit Trail:** All layers include processing timestamps
- **Data Governance:** Use Fabric's built-in governance features

## 🚧 Known Limitations

1. **ProductDescription:** May not exist in all environments; ProductName defaults to NULL
2. **StateProvince/CountryRegion:** Not in source Address table; placeholders added
3. **Full Refresh:** No incremental loading yet (ok for current data volume)
4. **SCD Type 1:** No historical dimension tracking (structure ready for Type 2)

## 🔜 Future Enhancements

1. **Incremental Loading:** Delta merge for all layers
2. **SCD Type 2:** Implement full historical tracking
3. **Data Quality Framework:** Great Expectations integration
4. **Automated Testing:** Validation tests for each layer
5. **Monitoring & Alerting:** Pipeline health dashboards
6. **Additional Gold Assets:**
   - Aggregate tables (monthly/yearly summaries)
   - Customer segmentation
   - Product affinity analysis
7. **Documentation:** Auto-generated data catalog with lineage

## 📞 Support

For questions or issues:
1. Review OpenSpec documentation in `openspec/` folder
2. Check execution logs in Fabric workspace
3. Validate source data availability

## 📄 License

MIT License - See repository for details

---

**Built with ❤️ using Microsoft Fabric and OpenSpec methodology**

**Last Updated:** April 13, 2026
