## ADDED Requirements

### Requirement: Fact table schema
The Fact_Sales table SHALL contain one row per sales order line item with surrogate foreign keys to all dimensions and numeric measures for analysis.

#### Scenario: Table structure includes all required columns
- **WHEN** the Fact_Sales table is created
- **THEN** it SHALL contain the following columns:
  - SalesKey (INT, surrogate primary key)
  - SalesOrderID (INT, natural key from source)
  - SalesOrderDetailID (INT, natural key from source)
  - CustomerKey (INT, foreign key to Dim_Customer)
  - ProductKey (INT, foreign key to Dim_Product)
  - OrderDateKey (INT, foreign key to Dim_Date)
  - DueDateKey (INT, foreign key to Dim_Date)
  - ShipDateKey (INT, foreign key to Dim_Date, nullable)
  - ShipToAddressKey (INT, foreign key to Dim_Address, nullable)
  - BillToAddressKey (INT, foreign key to Dim_Address, nullable)
  - OrderQty (INT, quantity ordered)
  - UnitPrice (DECIMAL(19,4), unit price)
  - UnitPriceDiscount (DECIMAL(19,4), discount per unit)
  - LineTotal (DECIMAL(19,4), calculated: OrderQty * UnitPrice * (1 - UnitPriceDiscount))
  - ProductStandardCost (DECIMAL(19,4), standard cost at order time)
  - LineProfit (DECIMAL(19,4), calculated: LineTotal - (OrderQty * ProductStandardCost))
  - OrderSubTotal (DECIMAL(19,4), subtotal from order header)
  - OrderTaxAmt (DECIMAL(19,4), tax amount from order header)
  - OrderFreight (DECIMAL(19,4), freight cost from order header)
  - OrderTotal (DECIMAL(19,4), calculated: SubTotal + TaxAmt + Freight)
  - OrderStatus (INT, order status code)
  - ShipMethod (STRING, shipping method name)
  - RevisionNumber (INT, order revision number)

### Requirement: Surrogate key generation
The SalesKey column SHALL be an auto-incrementing integer surrogate key that uniquely identifies each fact row.

#### Scenario: Surrogate keys are unique and sequential
- **WHEN** fact rows are loaded
- **THEN** each row SHALL have a unique SalesKey value
- **AND** the SalesKey SHALL be generated sequentially starting from 1

### Requirement: Grain definition
The Fact_Sales table SHALL maintain a grain of one row per order line item (SalesOrderDetail).

#### Scenario: One fact row per order line item
- **WHEN** the source contains a SalesOrderDetail record
- **THEN** exactly one row SHALL exist in Fact_Sales for that SalesOrderDetailID
- **AND** all measures and dimensions SHALL be associated with that line item

### Requirement: Measure calculations
All calculated measures SHALL be computed and stored in the fact table for query performance.

#### Scenario: LineTotal is correctly calculated
- **WHEN** a fact row is inserted
- **THEN** LineTotal SHALL equal OrderQty * UnitPrice * (1 - UnitPriceDiscount)

#### Scenario: LineProfit is correctly calculated
- **WHEN** a fact row is inserted
- **THEN** LineProfit SHALL equal LineTotal - (OrderQty * ProductStandardCost)

#### Scenario: OrderTotal is correctly calculated
- **WHEN** a fact row is inserted
- **THEN** OrderTotal SHALL equal OrderSubTotal + OrderTaxAmt + OrderFreight

### Requirement: Foreign key relationships
All dimension foreign keys SHALL reference valid surrogate keys in their respective dimension tables.

#### Scenario: Customer key references valid dimension
- **WHEN** a fact row is inserted with CustomerKey
- **THEN** that CustomerKey SHALL exist in Dim_Customer.CustomerKey

#### Scenario: Product key references valid dimension
- **WHEN** a fact row is inserted with ProductKey
- **THEN** that ProductKey SHALL exist in Dim_Product.ProductKey

#### Scenario: Date keys reference valid dimension
- **WHEN** a fact row is inserted with OrderDateKey, DueDateKey, or ShipDateKey
- **THEN** those keys SHALL exist in Dim_Date.DateKey

#### Scenario: Address keys reference valid dimension when not null
- **WHEN** a fact row is inserted with non-null ShipToAddressKey or BillToAddressKey
- **THEN** those keys SHALL exist in Dim_Address.AddressKey

### Requirement: Nullable foreign keys
The ShipDateKey, ShipToAddressKey, and BillToAddressKey columns SHALL allow NULL values to handle orders not yet shipped or missing address information.

#### Scenario: Unshipped orders have null ShipDateKey
- **WHEN** a source order has NULL ShipDate
- **THEN** the fact row SHALL have NULL ShipDateKey

#### Scenario: Missing address information allows null keys
- **WHEN** a source order has NULL ShipToAddressID or BillToAddressID
- **THEN** the corresponding AddressKey SHALL be NULL in the fact row

### Requirement: Data types and precision
All numeric measures SHALL use DECIMAL(19,4) to match source precision and prevent rounding errors in aggregations.

#### Scenario: Monetary amounts preserve precision
- **WHEN** monetary values are loaded from source
- **THEN** all DECIMAL values SHALL maintain 4 decimal places of precision
- **AND** calculations SHALL not introduce rounding errors

### Requirement: Delta table format
The Fact_Sales table SHALL be stored in Delta Lake format in the SalesAnalytics Lakehouse.

#### Scenario: Table is created as Delta format
- **WHEN** the fact table is created
- **THEN** it SHALL be stored as a managed Delta table
- **AND** it SHALL support ACID transactions and time travel
