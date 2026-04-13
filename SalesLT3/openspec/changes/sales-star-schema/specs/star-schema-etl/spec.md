## ADDED Requirements

### Requirement: ETL orchestration
The ETL process SHALL load dimensions first, followed by the fact table, ensuring referential integrity.

#### Scenario: Dimensions are loaded before fact
- **WHEN** ETL execution begins
- **THEN** all dimension tables SHALL be populated completely
- **AND** the fact table load SHALL begin only after all dimensions are ready

#### Scenario: Load order is enforced
- **WHEN** executing the ETL pipeline
- **THEN** tables SHALL be loaded in this order:
  1. Dim_Date (generated, no source dependency)
  2. Dim_Customer
  3. Dim_Product
  4. Dim_Address
  5. Fact_Sales

### Requirement: Source data extraction
The ETL SHALL read source data from the mirrored database `RvDSQL.MountedRelationalDatabase` in the SalesLT schema.

#### Scenario: Source tables are accessed via OneLake path
- **WHEN** extracting source data
- **THEN** ETL SHALL use abfss:// paths to access mirrored database tables
- **AND** table namespace SHALL be `SalesLT`

#### Scenario: All required source tables are read
- **WHEN** executing ETL
- **THEN** the following source tables SHALL be accessed:
  - SalesLT.Customer
  - SalesLT.Product
  - SalesLT.ProductCategory
  - SalesLT.ProductModel
  - SalesLT.ProductDescription
  - SalesLT.ProductModelProductDescription
  - SalesLT.Address
  - SalesLT.SalesOrderHeader
  - SalesLT.SalesOrderDetail

### Requirement: Target data loading
The ETL SHALL write transformed data to the `SalesAnalytics.Lakehouse` in Delta format using overwrite mode.

#### Scenario: Target tables are created in SalesAnalytics lakehouse
- **WHEN** ETL writes dimension and fact data
- **THEN** tables SHALL be created or overwritten in the SalesAnalytics lakehouse
- **AND** all tables SHALL use managed Delta format

#### Scenario: Full refresh with overwrite mode
- **WHEN** ETL executes
- **THEN** existing target tables SHALL be completely replaced using mode("overwrite")
- **AND** transactional consistency SHALL be maintained via Delta ACID properties

### Requirement: Dimension surrogate key generation
The ETL SHALL generate sequential surrogate keys for all dimension tables using monotonically_increasing_id() or row_number().

#### Scenario: Surrogate keys start at 1
- **WHEN** generating surrogate keys for a dimension
- **THEN** keys SHALL start at 1 and increment sequentially
- **AND** keys SHALL be deterministic within a single load

#### Scenario: Surrogate keys are unique
- **WHEN** dimension rows are loaded
- **THEN** each surrogate key SHALL be unique within that dimension table

### Requirement: Fact table surrogate key generation
The ETL SHALL generate a unique SalesKey surrogate key for each fact row.

#### Scenario: SalesKey is unique per fact row
- **WHEN** creating fact rows
- **THEN** each SalesKey SHALL be unique
- **AND** SalesKey SHALL be generated using monotonically_increasing_id() starting from 1

### Requirement: Foreign key lookups
The ETL SHALL perform dimension lookups using natural keys from the source to populate foreign key columns in the fact table.

#### Scenario: Customer key is looked up
- **WHEN** loading fact rows
- **THEN** CustomerKey SHALL be determined by joining to Dim_Customer on CustomerID

#### Scenario: Product key is looked up
- **WHEN** loading fact rows
- **THEN** ProductKey SHALL be determined by joining to Dim_Product on ProductID

#### Scenario: Date keys are looked up
- **WHEN** loading fact rows
- **THEN** OrderDateKey, DueDateKey, and ShipDateKey SHALL be determined by joining to Dim_Date on date values

#### Scenario: Address keys are looked up
- **WHEN** loading fact rows
- **THEN** ShipToAddressKey and BillToAddressKey SHALL be determined by joining to Dim_Address on AddressID

### Requirement: NULL handling in foreign keys
The ETL SHALL preserve NULL values for optional foreign keys (ShipDateKey, ShipToAddressKey, BillToAddressKey).

#### Scenario: NULL ShipDate produces NULL ShipDateKey
- **WHEN** a source order has NULL ShipDate
- **THEN** the fact row ShipDateKey SHALL be NULL

#### Scenario: NULL address IDs produce NULL address keys
- **WHEN** a source order has NULL ShipToAddressID or BillToAddressID
- **THEN** the corresponding AddressKey SHALL be NULL in the fact row

### Requirement: Date dimension generation
The ETL SHALL generate the Dim_Date table programmatically for the date range 2000-01-01 to 2030-12-31.

#### Scenario: Date dimension is generated without source data
- **WHEN** loading the date dimension
- **THEN** all dates from 2000-01-01 to 2030-12-31 SHALL be generated
- **AND** all calendar and fiscal attributes SHALL be calculated

### Requirement: Calculated measures
The ETL SHALL compute all calculated measures (LineTotal, LineProfit, OrderTotal) and store them in the fact table.

#### Scenario: LineTotal is calculated during ETL
- **WHEN** loading fact rows
- **THEN** LineTotal SHALL be computed as OrderQty * UnitPrice * (1 - UnitPriceDiscount)

#### Scenario: LineProfit is calculated during ETL
- **WHEN** loading fact rows  
- **THEN** LineProfit SHALL be computed as LineTotal - (OrderQty * ProductStandardCost)

#### Scenario: OrderTotal is calculated during ETL
- **WHEN** loading fact rows
- **THEN** OrderTotal SHALL be computed as OrderSubTotal + OrderTaxAmt + OrderFreight

### Requirement: Product hierarchy denormalization
The ETL SHALL denormalize the product category hierarchy by joining Product to ProductCategory and parent categories.

#### Scenario: Product category name is populated
- **WHEN** loading Dim_Product
- **THEN** ProductCategoryName SHALL be looked up from ProductCategory table

#### Scenario: Parent category is populated when exists
- **WHEN** a product's category has a ParentProductCategoryID
- **THEN** ParentProductCategoryName SHALL be looked up from the parent ProductCategory row

### Requirement: Product name enrichment
The ETL SHALL populate ProductName by joining to ProductDescription via the ProductModelProductDescription bridge table.

#### Scenario: Product name is derived from description
- **WHEN** a product has a ProductModelID
- **THEN** ETL SHALL join to ProductModelProductDescription and ProductDescription
- **AND** select the description where CultureID = 'en' if available

### Requirement: Error handling and logging
The ETL SHALL log execution start, completion, row counts, and any errors encountered.

#### Scenario: Execution is logged
- **WHEN** ETL runs
- **THEN** start time, end time, and duration SHALL be logged

#### Scenario: Row counts are validated
- **WHEN** ETL completes
- **THEN** source row counts and target row counts SHALL be compared
- **AND** discrepancies SHALL be logged as warnings

#### Scenario: Errors are logged and halt execution
- **WHEN** an ETL step encounters an error
- **THEN** the error SHALL be logged with context
- **AND** execution SHALL halt to prevent partial loads

### Requirement: Idempotency
The ETL SHALL be idempotent, producing the same result when executed multiple times on the same source data.

#### Scenario: Re-running ETL produces consistent results
- **WHEN** ETL is executed twice on unchanged source data
- **THEN** the target tables SHALL contain identical data
- **AND** surrogate keys MAY differ but dimension membership SHALL be consistent

### Requirement: PySpark implementation
The ETL SHALL be implemented as a Spark notebook using PySpark for data transformations.

#### Scenario: Spark notebook is the ETL vehicle
- **WHEN** implementing the ETL
- **THEN** a Spark notebook SHALL be created
- **AND** PySpark DataFrame API SHALL be used for transformations

### Requirement: Delta format output
All target tables SHALL be written in Delta format with ACID transaction support.

#### Scenario: Tables support ACID transactions
- **WHEN** ETL writes target tables
- **THEN** all writes SHALL use Delta format
- **AND** each table write SHALL be atomic (all-or-nothing)
