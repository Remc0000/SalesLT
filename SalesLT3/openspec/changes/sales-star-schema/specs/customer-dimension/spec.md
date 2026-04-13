## ADDED Requirements

### Requirement: Customer dimension schema
The Dim_Customer table SHALL contain one row per customer with surrogate key and all customer attributes.

#### Scenario: Table structure includes all required columns
- **WHEN** the Dim_Customer table is created
- **THEN** it SHALL contain the following columns:
  - CustomerKey (INT, surrogate primary key)
  - CustomerID (INT, natural key from source)
  - Title (STRING, customer title, nullable)
  - FirstName (STRING, customer first name, derived from source if available)
  - LastName (STRING, customer last name, derived from source if available)
  - Suffix (STRING, name suffix, nullable)
  - CompanyName (STRING, company name, nullable)
  - SalesPerson (STRING, assigned sales person, nullable)
  - EmailAddress (STRING, customer email, nullable)
  - ModifiedDate (TIMESTAMP, last modification timestamp)
  - IsCurrent (BOOLEAN, SCD Type 2 flag, default TRUE)
  - EffectiveDate (DATE, SCD Type 2 start date)
  - EndDate (DATE, SCD Type 2 end date, nullable)

### Requirement: Surrogate key generation
The CustomerKey column SHALL be an auto-incrementing integer surrogate key that uniquely identifies each customer dimension row.

#### Scenario: Surrogate keys are unique and sequential
- **WHEN** customer rows are loaded
- **THEN** each row SHALL have a unique CustomerKey value
- **AND** the CustomerKey SHALL be generated sequentially starting from 1

### Requirement: Natural key preservation
The CustomerID SHALL be preserved from the source system to enable source-to-dimension lookups.

#### Scenario: Natural key is unique for current records
- **WHEN** loading active customers (IsCurrent = TRUE)
- **THEN** each CustomerID SHALL appear exactly once among current records

### Requirement: SCD Type 2 structure
The dimension SHALL include columns to support Slowly Changing Dimension Type 2 tracking for future use.

#### Scenario: Initial load sets SCD columns
- **WHEN** customers are initially loaded
- **THEN** IsCurrent SHALL be set to TRUE
- **AND** EffectiveDate SHALL be set to the current load date
- **AND** EndDate SHALL be NULL

#### Scenario: Structure supports future historical tracking
- **WHEN** a customer attribute changes in the future
- **THEN** the dimension structure SHALL support creating a new row with updated EffectiveDate
- **AND** the previous row can be end-dated with EndDate and IsCurrent = FALSE

### Requirement: Name handling
The dimension SHALL contain separate FirstName and LastName fields if derivable from source data.

#### Scenario: Names are populated when available
- **WHEN** customer data is loaded
- **THEN** FirstName and LastName SHALL be populated if available in source
- **AND** both SHALL be NULL if not derivable

### Requirement: Nullable attributes
Title, Suffix, CompanyName, SalesPerson, and EmailAddress SHALL allow NULL values.

#### Scenario: Missing attributes are stored as NULL
- **WHEN** source customer data lacks Title, Suffix, CompanyName, SalesPerson, or EmailAddress
- **THEN** those columns SHALL be NULL in the dimension row

### Requirement: Data security
Sensitive customer data (PasswordHash, PasswordSalt) SHALL NOT be included in the dimension.

#### Scenario: Password fields are excluded
- **WHEN** the customer dimension is populated
- **THEN** PasswordHash and PasswordSalt from the source SHALL NOT be loaded
- **AND** only business-relevant attributes SHALL be included

### Requirement: Delta table format
The Dim_Customer table SHALL be stored in Delta Lake format in the SalesAnalytics Lakehouse.

#### Scenario: Table is created as Delta format
- **WHEN** the customer dimension is created
- **THEN** it SHALL be stored as a managed Delta table
- **AND** it SHALL support ACID transactions and time travel
