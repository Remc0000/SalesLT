## Context

The SalesLT workspace contains a mirrored SQL database (`RvDSQL.MountedRelationalDatabase`) with normalized operational tables in the SalesLT schema. This database follows a typical OLTP design with 10 tables including SalesOrderHeader, SalesOrderDetail, Customer, Product, ProductCategory, ProductModel, Address, and bridge tables.

Current limitations:
- Complex multi-table joins required for simple analytical queries
- No time-based analysis without date dimension
- Product hierarchy requires multiple joins (Product → ProductCategory → parent categories)
- No calculated measures readily available (line totals, revenue, etc.)
- Not optimized for aggregation queries

The target is the `SalesAnalytics.Lakehouse` which already exists in the SalesLT workspace and will host the star schema tables in Delta format.

## Goals / Non-Goals

**Goals:**
- Design a dimensional model optimized for sales reporting and analytics
- Simplify queries by denormalizing relationships into dimension tables
- Support time-based analysis with a comprehensive date dimension
- Enable efficient aggregations and drill-down analysis
- Establish surrogate key pattern for future SCD Type 2 support
- Use Delta Lake format for ACID transactions and time travel capabilities
- Create reusable ETL pattern that can be extended to other subject areas

**Non-Goals:**
- Real-time data synchronization (batch ETL is acceptable)
- Modifying or impacting the source mirrored database
- SCD Type 2 implementation in this phase (prepare structure only)
- Advanced analytics or ML features
- Data quality rules or validation beyond basic integrity
- Historical data reconstruction (start from current state)

## Decisions

### 1. Star Schema vs Snowflake Schema
**Decision:** Use star schema with denormalized dimensions  
**Rationale:** The product hierarchy is only 2 levels deep (category → parent category), making denormalization simple. Star schema provides better query performance and is easier for business users to understand. The small dimension sizes (41 categories, 128 models, 847 customers) make storage overhead negligible.  
**Alternative considered:** Snowflake schema would preserve normalization but add query complexity without meaningful benefits at this scale.

### 2. Grain of the Fact Table
**Decision:** One row per order line item (SalesOrderDetail level)  
**Rationale:** This is the most atomic level of sales transactions, enabling maximum flexibility for aggregation. Users can analyze at the product level, order level, or higher. The current volume (542 rows) is easily manageable, and growth is expected to remain moderate.  
**Alternative considered:** Order header grain would lose product-level detail. Daily aggregates would prevent drill-down to individual transactions.

### 3. Surrogate Keys vs Natural Keys
**Decision:** Use surrogate keys (auto-incrementing integers) as primary keys for all dimensions, maintain natural keys as attributes  
**Rationale:** 
- Enables future SCD Type 2 without changing fact table structure
- Smaller join keys improve query performance
- Decouples dimensional model from source system key changes
- Standard dimensional modeling best practice  
**Alternative considered:** Natural keys are simpler initially but limit future flexibility and create tight coupling to source systems.

### 4. Date Dimension Scope
**Decision:** Generate date dimension from 2000-01-01 to 2030-12-31  
**Rationale:** Covers historical data range (earliest order: 2008) with sufficient future dates. Includes calendar attributes (year, quarter, month, week, day) and fiscal period support. Total ~11,300 rows is negligible storage.  
**Alternative considered:** Dynamic date range based on data would require periodic regeneration and complicate queries with missing future dates.

### 5. ETL Tool and Pattern
**Decision:** Use Spark notebooks with PySpark for ETL, implement full refresh initially  
**Rationale:** 
- Spark is native to Fabric and provides excellent performance for joins and transformations
- PySpark offers flexibility for complex logic
- Full refresh is acceptable given small data volumes
- Delta overwrite provides transactional consistency
- Can evolve to incremental loads using Delta change data features  
**Alternative considered:** Data Factory pipelines would add orchestration overhead without benefits at this scale. T-SQL stored procedures would be less portable.

### 6. Handling Missing Product Names
**Decision:** Join to ProductDescription via ProductModelProductDescription bridge table to get product names  
**Rationale:** Product table doesn't contain a Name field directly. The name is stored in ProductDescription and linked via ProductModel. This denormalization into Dim_Product simplifies queries.  
**Alternative considered:** Leaving product name lookup to query time would defeat the purpose of denormalization.

### 7. Address Dimension Structure
**Decision:** Create single Address dimension with role/type indicator (Ship-To, Bill-To)  
**Rationale:** Avoids duplication of address dimensions. Fact table will have two foreign keys (ShipToAddressKey, BillToAddressKey) pointing to the same dimension.  
**Alternative considered:** Separate dimensions for ship-to and bill-to would duplicate data unnecessarily.

### 8. SCD Strategy
**Decision:** Implement SCD Type 1 (overwrite) initially, structure prepared for Type 2  
**Rationale:** Historical dimension changes are not required in Phase 1. Using surrogate keys and maintaining source keys allows easy migration to Type 2 when needed.  
**Alternative considered:** Implementing Type 2 immediately would add complexity without current business requirement.

## Risks / Trade-offs

**[Risk: Source data quality issues]**  
→ **Mitigation:** Implement data profiling and validation checks in ETL. Log quality issues for investigation. Use COALESCE and IFNULL for handling nulls gracefully.

**[Risk: Performance degradation with data growth]**  
→ **Mitigation:** Delta tables support partitioning (can partition fact table by OrderDate year/month if needed). Monitor query performance and establish partitioning thresholds. Consider aggregates for historical data.

**[Risk: Late-arriving dimensions]**  
→ **Mitigation:** Load dimensions before facts. Validate foreign key relationships. Create dimension rows for orphaned fact records with "Unknown" indicator.

**[Trade-off: Full refresh simplicity vs incremental efficiency]**  
→ Full refresh is simpler to implement and maintain. Current data volume (~500 rows) makes this acceptable. Can evolve to incremental with Delta merge when volumes grow.

**[Trade-off: Denormalization increases storage but improves query performance]**  
→ Storage cost is negligible for this data volume. Query performance and simplicity gains far outweigh the marginal storage increase.

**[Risk: Schema drift in source mirrored database]**  
→ **Mitigation:** Implement schema validation in ETL. Use explicit column lists rather than SELECT *. Add alerts for schema changes.

## Migration Plan

**Phase 1: Initial Build**
1. Create empty dimension tables in SalesAnalytics.Lakehouse with surrogate keys
2. Create empty fact table with proper foreign key relationships (logical, not enforced)
3. Develop and test ETL for each dimension independently
4. Develop and test fact table ETL with dimension lookups
5. Execute full load in dependency order: Dimensions → Fact

**Phase 2: Validation**
1. Validate row counts match source expectations
2. Compare aggregates (total revenue, order counts) between source and star schema
3. Test sample queries for correctness and performance
4. Document query patterns and examples

**Phase 3: Productionize**
1. Schedule ETL notebook execution (e.g., daily overnight)
2. Implement error handling and retry logic
3. Add monitoring and alerting for ETL failures
4. Document refresh process and troubleshooting guide

**Rollback Strategy:**
If star schema is corrupted or ETL fails critically:
- Drop and recreate tables from ETL scripts (stored in Git)
- Re-run full load from source
- Delta time travel can restore previous table versions if needed

## Open Questions

1. **Fiscal calendar definition**: What is the fiscal year start month? (Assume January if not specified)
2. **Customer segmentation**: Should we add calculated customer segments (e.g., high-value, new, at-risk)?
3. **Product hierarchy naming**: ProductCategory table lacks a Name field - is there a separate table for category names?
4. **Refresh frequency**: How often should the star schema be refreshed? Daily, hourly, or on-demand?
5. **Historical data**: Should we preserve history before the first mirrored snapshot? (Current answer: No)
6. **Security**: Should row-level security be applied on dimensions or facts? (Current answer: Not in Phase 1)
