## 1. Environment Setup and Validation

- [ ] 1.1 Verify access to SalesLT workspace in Fabric
- [ ] 1.2 Confirm RvDSQL.MountedRelationalDatabase is accessible with all source tables
- [ ] 1.3 Confirm SalesAnalytics.Lakehouse exists and is writable
- [ ] 1.4 Create Spark notebook for ETL in SalesLT workspace
- [ ] 1.5 Document source-to-target mapping for all tables

## 2. Create Dimension Table Schemas

- [ ] 2.1 Create Dim_Date table schema with all calendar and fiscal attributes
- [ ] 2.2 Create Dim_Customer table schema with surrogate key and SCD Type 2 structure
- [ ] 2.3 Create Dim_Product table schema with denormalized category/model hierarchy
- [ ] 2.4 Create Dim_Address table schema with surrogate key
- [ ] 2.5 Add "Unknown" address row (AddressKey = -1) to Dim_Address

## 3. Create Fact Table Schema

- [ ] 3.1 Create Fact_Sales table schema with surrogate key and all measures
- [ ] 3.2 Define foreign key relationships to all dimensions (logical, not enforced)
- [ ] 3.3 Verify decimal precision for monetary amounts (DECIMAL 19,4)

## 4. Implement Date Dimension ETL

- [ ] 4.1 Write PySpark code to generate date range 2000-01-01 to 2030-12-31
- [ ] 4.2 Calculate DateKey in YYYYMMDD format
- [ ] 4.3 Calculate all calendar attributes (year, quarter, month, day, week)
- [ ] 4.4 Calculate day of week and weekend flag
- [ ] 4.5 Calculate fiscal calendar attributes (year, quarter, month)
- [ ] 4.6 Set IsHoliday to FALSE for all rows
- [ ] 4.7 Write Dim_Date to SalesAnalytics lakehouse in Delta format
- [ ] 4.8 Validate row count (approximately 11,300 rows)

## 5. Implement Customer Dimension ETL

- [ ] 5.1 Read SalesLT.Customer from mirrored database
- [ ] 5.2 Generate CustomerKey surrogate key starting from 1
- [ ] 5.3 Map CustomerID as natural key
- [ ] 5.4 Extract Title, FirstName, LastName, Suffix if derivable
- [ ] 5.5 Map CompanyName, SalesPerson, EmailAddress (allow NULLs)
- [ ] 5.6 Exclude PasswordHash and PasswordSalt for security
- [ ] 5.7 Set SCD Type 2 columns (IsCurrent=TRUE, EffectiveDate=current date, EndDate=NULL)
- [ ] 5.8 Write Dim_Customer to SalesAnalytics lakehouse in Delta format
- [ ] 5.9 Validate row count matches source (~847 customers)

## 6. Implement Product Dimension ETL

- [ ] 6.1 Read SalesLT.Product from mirrored database
- [ ] 6.2 Generate ProductKey surrogate key starting from 1
- [ ] 6.3 Map ProductID as natural key and ProductNumber
- [ ] 6.4 Join to ProductModelProductDescription and ProductDescription to get ProductName
- [ ] 6.5 Join to ProductCategory to get category name and ID
- [ ] 6.6 Self-join ProductCategory to get parent category name and ID
- [ ] 6.7 Join to ProductModel to get model name
- [ ] 6.8 Calculate IsDiscontinued flag (DiscontinuedDate IS NOT NULL)
- [ ] 6.9 Map product attributes (Color, Size, Weight, StandardCost, ListPrice, dates)
- [ ] 6.10 Set SCD Type 2 columns (IsCurrent=TRUE, EffectiveDate=current date, EndDate=NULL)
- [ ] 6.11 Write Dim_Product to SalesAnalytics lakehouse in Delta format
- [ ] 6.12 Validate product count and verify category hierarchy is flattened

## 7. Implement Address Dimension ETL

- [ ] 7.1 Read SalesLT.Address from mirrored database
- [ ] 7.2 Generate AddressKey surrogate key starting from 1
- [ ] 7.3 Map AddressID as natural key
- [ ] 7.4 Map AddressLine1, AddressLine2, City, PostalCode
- [ ] 7.5 Add StateProvince and CountryRegion columns (NULL for now, future enrichment)
- [ ] 7.6 Set SCD Type 2 columns (IsCurrent=TRUE, EffectiveDate=current date, EndDate=NULL)
- [ ] 7.7 Insert "Unknown" address row with AddressKey = -1
- [ ] 7.8 Write Dim_Address to SalesAnalytics lakehouse in Delta format
- [ ] 7.9 Validate row count matches source addresses

## 8. Implement Fact Table ETL

- [ ] 8.1 Read SalesLT.SalesOrderDetail from mirrored database
- [ ] 8.2 Join to SalesLT.SalesOrderHeader to get order-level attributes
- [ ] 8.3 Generate SalesKey surrogate key for each fact row
- [ ] 8.4 Lookup CustomerKey from Dim_Customer by joining on CustomerID
- [ ] 8.5 Lookup ProductKey from Dim_Product by joining on ProductID
- [ ] 8.6 Lookup OrderDateKey from Dim_Date by joining on OrderDate
- [ ] 8.7 Lookup DueDateKey from Dim_Date by joining on DueDate
- [ ] 8.8 Lookup ShipDateKey from Dim_Date by joining on ShipDate (handle NULLs)
- [ ] 8.9 Lookup ShipToAddressKey from Dim_Address by joining on ShipToAddressID (handle NULLs)
- [ ] 8.10 Lookup BillToAddressKey from Dim_Address by joining on BillToAddressID (handle NULLs)
- [ ] 8.11 Calculate LineTotal = OrderQty * UnitPrice * (1 - UnitPriceDiscount)
- [ ] 8.12 Lookup ProductStandardCost from Product table and calculate LineProfit
- [ ] 8.13 Calculate OrderTotal = SubTotal + TaxAmt + Freight
- [ ] 8.14 Map all measures and dimensions to fact table columns
- [ ] 8.15 Write Fact_Sales to SalesAnalytics lakehouse in Delta format
- [ ] 8.16 Validate row count matches source SalesOrderDetail (~542 rows)

## 9. ETL Orchestration and Error Handling

- [ ] 9.1 Organize ETL into functions for each dimension and fact table
- [ ] 9.2 Implement load order: Date → Customer → Product → Address → Fact
- [ ] 9.3 Add logging for start/end time and row counts for each table
- [ ] 9.4 Add error handling with try/except blocks for each load step
- [ ] 9.5 Validate foreign key relationships after fact load
- [ ] 9.6 Add row count comparison between source and target
- [ ] 9.7 Log any orphaned records or missing foreign key references

## 10. Data Validation and Quality Checks

- [ ] 10.1 Verify all dimension tables have unique surrogate keys
- [ ] 10.2 Verify Dim_Date covers expected date range with no gaps
- [ ] 10.3 Verify Dim_Customer row count matches source
- [ ] 10.4 Verify Dim_Product has denormalized category hierarchy
- [ ] 10.5 Verify Dim_Address includes "Unknown" row
- [ ] 10.6 Verify Fact_Sales row count matches source SalesOrderDetail
- [ ] 10.7 Verify all fact foreign keys reference valid dimension keys
- [ ] 10.8 Validate calculated measures (LineTotal, LineProfit, OrderTotal)
- [ ] 10.9 Compare aggregate revenue between source and star schema
- [ ] 10.10 Verify NULLs are handled correctly for optional foreign keys

## 11. Query Testing and Performance Validation

- [ ] 11.1 Write sample query: Total revenue by year
- [ ] 11.2 Write sample query: Top 10 products by revenue
- [ ] 11.3 Write sample query: Customer sales summary
- [ ] 11.4 Write sample query: Revenue by product category
- [ ] 11.5 Write sample query: Orders by month and quarter
- [ ] 11.6 Measure query performance and validate results match expectations
- [ ] 11.7 Document query patterns and examples for business users

## 12. Documentation and Productionization

- [ ] 12.1 Document ETL notebook with inline comments
- [ ] 12.2 Create README for star schema describing tables and relationships
- [ ] 12.3 Document refresh process and prerequisites
- [ ] 12.4 Create ERD diagram showing fact and dimension relationships
- [ ] 12.5 Document known limitations and future enhancements
- [ ] 12.6 Plan ETL schedule (e.g., daily overnight refresh)
- [ ] 12.7 Add monitoring and alerting for ETL failures
- [ ] 12.8 Create troubleshooting guide for common issues
