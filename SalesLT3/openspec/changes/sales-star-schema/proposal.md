## Why

The current SalesLT mirrored database contains normalized transactional tables that are not optimized for analytical queries and reporting. Business users need fast, intuitive access to sales data for revenue analysis, customer insights, and product performance tracking. A star schema will provide simplified querying, improved performance through denormalization, and support for efficient aggregations across time, customer, and product dimensions.

## What Changes

- Create a fact table (`Fact_Sales`) that combines order header and detail data with calculated measures for revenue analysis
- Build dimension tables for Customer, Product (with category/model hierarchy), Date, and Address
- Implement surrogate keys for all dimensions to support slowly changing dimensions (SCD Type 2) future requirements
- Denormalize product hierarchy (category and model) into the product dimension for simplified querying
- Generate a complete date dimension covering the sales date range with fiscal and calendar hierarchies
- Create ETL/ELT logic using Spark to populate the star schema from the mirrored SalesLT tables
- Deploy the star schema to the `SalesAnalytics.Lakehouse` in Delta format for optimal query performance

## Capabilities

### New Capabilities
<!-- Capabilities being introduced. Replace <name> with kebab-case identifier (e.g., user-auth, data-export, api-rate-limiting). Each creates specs/<name>/spec.md -->
- `sales-fact-table`: Central fact table containing sales transaction metrics (quantity, prices, discounts, revenue) with foreign keys to all dimensions
- `customer-dimension`: Customer dimension containing customer attributes including name, company, sales person, and contact information
- `product-dimension`: Product dimension with denormalized category and model hierarchy, including product attributes like color, size, weight, and pricing
- `date-dimension`: Comprehensive date dimension with calendar attributes (year, quarter, month, day, weekday) and fiscal period support
- `address-dimension`: Address dimension containing shipping and billing address details with type indicators
- `star-schema-etl`: Spark-based ETL pipeline to extract from mirrored SalesLT tables, transform, and load into the star schema tables

### Modified Capabilities
<!-- Existing capabilities whose REQUIREMENTS are changing (not just implementation).
     Only list here if spec-level behavior changes. Each needs a delta spec file.
     Use existing spec names from openspec/specs/. Leave empty if no requirement changes. -->

## Impact

**Affected Systems:**
- Source: `RvDSQL.MountedRelationalDatabase` mirrored database (read-only access)
- Target: `SalesAnalytics.Lakehouse` in the SalesLT workspace
- Processing: Spark notebooks for ETL execution

**Data Volumes:**
- 32 sales orders, 542 order line items, 847 customers
- Initial load followed by incremental refresh strategy

**Dependencies:**
- Microsoft Fabric Lakehouse (SalesAnalytics)
- Spark compute for ETL processing
- Delta Lake format for all tables

**Breaking Changes:**
None - this is a new analytical layer that does not modify source systems
