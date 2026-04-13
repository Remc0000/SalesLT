## ADDED Requirements

### Requirement: Address dimension schema
The Dim_Address table SHALL contain one row per unique address with all address attributes.

#### Scenario: Table structure includes all required columns
- **WHEN** the Dim_Address table is created
- **THEN** it SHALL contain the following columns:
  - AddressKey (INT, surrogate primary key)
  - AddressID (INT, natural key from source)
  - AddressLine1 (STRING, primary address line)
  - AddressLine2 (STRING, secondary address line, nullable)
  - City (STRING, city name)
  - StateProvince (STRING, state or province name, nullable)
  - CountryRegion (STRING, country or region name, nullable)
  - PostalCode (STRING, postal/ZIP code)
  - ModifiedDate (TIMESTAMP, last modification timestamp)
  - IsCurrent (BOOLEAN, SCD Type 2 flag, default TRUE)
  - EffectiveDate (DATE, SCD Type 2 start date)
  - EndDate (DATE, SCD Type 2 end date, nullable)

### Requirement: Surrogate key generation
The AddressKey column SHALL be an auto-incrementing integer surrogate key that uniquely identifies each address dimension row.

#### Scenario: Surrogate keys are unique and sequential
- **WHEN** address rows are loaded
- **THEN** each row SHALL have a unique AddressKey value
- **AND** the AddressKey SHALL be generated sequentially starting from 1

### Requirement: Natural key preservation
The AddressID SHALL be preserved from the source system to enable source-to-dimension lookups.

#### Scenario: Natural key is unique for current records
- **WHEN** loading active addresses (IsCurrent = TRUE)
- **THEN** each AddressID SHALL appear exactly once among current records

### Requirement: Role-agnostic structure
The dimension SHALL support multiple address roles (ship-to, bill-to) through foreign key relationships in the fact table rather than role-specific dimensions.

#### Scenario: Single address supports multiple roles
- **WHEN** an address is used for both shipping and billing
- **THEN** the same AddressKey SHALL be referenced by both ShipToAddressKey and BillToAddressKey in the fact table

### Requirement: Nullable attributes
AddressLine2, StateProvince, and CountryRegion SHALL allow NULL values for addresses where these details are not provided.

#### Scenario: Missing address details are stored as NULL
- **WHEN** source address data lacks AddressLine2, StateProvince, or CountryRegion
- **THEN** those columns SHALL be NULL in the dimension row

### Requirement: Geographic hierarchy support
StateProvince and CountryRegion SHALL be included to support geographic hierarchy analysis even if not present in source.

#### Scenario: Geographic attributes enable reporting
- **WHEN** addresses include StateProvince and CountryRegion
- **THEN** queries can aggregate by City, StateProvince, or CountryRegion
- **AND** geographic hierarchy drill-down is supported

#### Scenario: Missing geographic attributes default to NULL
- **WHEN** source address lacks StateProvince or CountryRegion
- **THEN** these SHALL be NULL pending future enrichment

### Requirement: Address line format
AddressLine1 and AddressLine2 SHALL maintain the original format from the source system without normalization.

#### Scenario: Address lines are preserved as-is
- **WHEN** source address has specific formatting
- **THEN** AddressLine1 and AddressLine2 SHALL retain original formatting
- **AND** no standardization or parsing SHALL occur

### Requirement: SCD Type 2 structure
The dimension SHALL include columns to support Slowly Changing Dimension Type 2 tracking for future use.

#### Scenario: Initial load sets SCD columns
- **WHEN** addresses are initially loaded
- **THEN** IsCurrent SHALL be set to TRUE
- **AND** EffectiveDate SHALL be set to the current load date
- **AND** EndDate SHALL be NULL

#### Scenario: Structure supports future historical tracking
- **WHEN** an address attribute changes in the future
- **THEN** the dimension structure SHALL support creating a new row with updated EffectiveDate
- **AND** the previous row can be end-dated with EndDate and IsCurrent = FALSE

### Requirement: Orphaned address handling
The dimension SHALL include an "Unknown" address row for fact records with missing address references.

#### Scenario: Unknown address row exists
- **WHEN** the dimension is created
- **THEN** an "Unknown" address row SHALL exist with AddressKey = -1
- **AND** all address attributes SHALL indicate unknown/missing data

#### Scenario: Fact rows with missing addresses reference unknown row
- **WHEN** a source order lacks address information
- **THEN** the fact row MAY reference AddressKey = -1 or have NULL AddressKey

### Requirement: Delta table format
The Dim_Address table SHALL be stored in Delta Lake format in the SalesAnalytics Lakehouse.

#### Scenario: Table is created as Delta format
- **WHEN** the address dimension is created
- **THEN** it SHALL be stored as a managed Delta table
- **AND** it SHALL support ACID transactions and time travel
