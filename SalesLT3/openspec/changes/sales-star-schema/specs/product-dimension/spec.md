## ADDED Requirements

### Requirement: Product dimension schema
The Dim_Product table SHALL contain one row per product with denormalized category and model hierarchy.

#### Scenario: Table structure includes all required columns
- **WHEN** the Dim_Product table is created
- **THEN** it SHALL contain the following columns:
  - ProductKey (INT, surrogate primary key)
  - ProductID (INT, natural key from source)
  - ProductNumber (STRING, product SKU)
  - ProductName (STRING, product name from ProductDescription)
  - Color (STRING, product color, nullable)
  - Size (STRING, product size, nullable)
  - Weight (DECIMAL(8,2), product weight, nullable)
  - StandardCost (DECIMAL(19,4), standard cost)
  - ListPrice (DECIMAL(19,4), list price)
  - ProductCategoryID (INT, category natural key)
  - ProductCategoryName (STRING, category name, nullable)
  - ParentProductCategoryID (INT, parent category natural key, nullable)
  - ParentProductCategoryName (STRING, parent category name, nullable)
  - ProductModelID (INT, model natural key, nullable)
  - ProductModelName (STRING, model name, nullable)
  - SellStartDate (TIMESTAMP, product availability start)
  - SellEndDate (TIMESTAMP, product availability end, nullable)
  - DiscontinuedDate (TIMESTAMP, discontinued date, nullable)
  - IsDiscontinued (BOOLEAN, calculated: DiscontinuedDate IS NOT NULL)
  - ModifiedDate (TIMESTAMP, last modification timestamp)
  - IsCurrent (BOOLEAN, SCD Type 2 flag, default TRUE)
  - EffectiveDate (DATE, SCD Type 2 start date)
  - EndDate (DATE, SCD Type 2 end date, nullable)

### Requirement: Surrogate key generation
The ProductKey column SHALL be an auto-incrementing integer surrogate key that uniquely identifies each product dimension row.

#### Scenario: Surrogate keys are unique and sequential
- **WHEN** product rows are loaded
- **THEN** each row SHALL have a unique ProductKey value
- **AND** the ProductKey SHALL be generated sequentially starting from 1

### Requirement: Product hierarchy denormalization
The dimension SHALL denormalize the product category hierarchy by flattening ProductCategory and parent category relationships.

#### Scenario: Category hierarchy is flattened
- **WHEN** a product belongs to a category with a parent
- **THEN** both ProductCategoryName and ParentProductCategoryName SHALL be populated
- **AND** queries can filter/group by either level without joins

#### Scenario: Top-level categories have no parent
- **WHEN** a product's category has no parent (ParentProductCategoryID is NULL)
- **THEN** ParentProductCategoryID and ParentProductCategoryName SHALL be NULL

### Requirement: Product name lookup
Product names SHALL be derived from the ProductDescription table via the ProductModelProductDescription bridge table.

#### Scenario: Product name is populated from description
- **WHEN** a product has an associated ProductModel and ProductDescription
- **THEN** ProductName SHALL be populated from ProductDescription
- **AND** the description SHALL be in English (CultureID = 'en') if available

#### Scenario: Missing product name is allowed
- **WHEN** no ProductDescription exists for a product
- **THEN** ProductName SHALL be NULL
- **AND** the product row SHALL still be created

### Requirement: Model name lookup  
Product model names SHALL be derived from the ProductModel table if available.

#### Scenario: Model name is populated when available
- **WHEN** a product has a ProductModelID
- **THEN** ProductModelName SHALL be looked up from ProductModel
- **AND** populated in the dimension row

#### Scenario: Products without models are supported
- **WHEN** a product has NULL ProductModelID
- **THEN** ProductModelID and ProductModelName SHALL be NULL in the dimension

### Requirement: Discontinued product flag
The IsDiscontinued boolean SHALL be calculated based on the DiscontinuedDate value.

#### Scenario: Discontinued products are flagged
- **WHEN** DiscontinuedDate is NOT NULL
- **THEN** IsDiscontinued SHALL be TRUE

#### Scenario: Active products are not flagged
- **WHEN** DiscontinuedDate is NULL
- **THEN** IsDiscontinued SHALL be FALSE

### Requirement: Pricing attributes
StandardCost and ListPrice SHALL be included to enable margin and profitability analysis.

#### Scenario: Cost and price are maintained
- **WHEN** product data is loaded
- **THEN** StandardCost and ListPrice SHALL be copied from the source Product table
- **AND** both SHALL maintain DECIMAL(19,4) precision

### Requirement: Nullable attributes
Color, Size, Weight, ProductCategoryName, ParentProductCategoryID, ParentProductCategoryName, ProductModelID, ProductModelName, SellEndDate, and DiscontinuedDate SHALL allow NULL values.

#### Scenario: Missing attributes are stored as NULL
- **WHEN** source product data lacks optional attributes
- **THEN** those columns SHALL be NULL in the dimension row

### Requirement: SCD Type 2 structure
The dimension SHALL include columns to support Slowly Changing Dimension Type 2 tracking for future use.

#### Scenario: Initial load sets SCD columns
- **WHEN** products are initially loaded
- **THEN** IsCurrent SHALL be set to TRUE
- **AND** EffectiveDate SHALL be set to the current load date
- **AND** EndDate SHALL be NULL

### Requirement: Delta table format
The Dim_Product table SHALL be stored in Delta Lake format in the SalesAnalytics Lakehouse.

#### Scenario: Table is created as Delta format
- **WHEN** the product dimension is created
- **THEN** it SHALL be stored as a managed Delta table
- **AND** it SHALL support ACID transactions and time travel
