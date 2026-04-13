## ADDED Requirements

### Requirement: Date dimension schema
The Dim_Date table SHALL contain one row per calendar day with comprehensive date attributes for time-based analysis.

#### Scenario: Table structure includes all required columns
- **WHEN** the Dim_Date table is created
- **THEN** it SHALL contain the following columns:
  - DateKey (INT, surrogate primary key in YYYYMMDD format)
  - Date (DATE, actual date value)
  - Year (INT, four-digit year)
  - Quarter (INT, quarter number 1-4)
  - QuarterName (STRING, e.g., 'Q1', 'Q2')
  - Month (INT, month number 1-12)
  - MonthName (STRING, full month name)
  - MonthNameShort (STRING, three-letter month abbreviation)
  - DayOfMonth (INT, day of month 1-31)
  - DayOfWeek (INT, day of week 1-7, Monday=1)
  - DayOfWeekName (STRING, full weekday name)
  - DayOfWeekNameShort (STRING, three-letter weekday abbreviation)
  - DayOfYear (INT, day of year 1-366)
  - WeekOfYear (INT, ISO week number 1-53)
  - IsWeekend (BOOLEAN, TRUE for Saturday and Sunday)
  - IsHoliday (BOOLEAN, placeholder for future holiday definitions)
  - FiscalYear (INT, fiscal year)
  - FiscalQuarter (INT, fiscal quarter 1-4)
  - FiscalMonth (INT, fiscal month 1-12)

### Requirement: Surrogate key format
The DateKey SHALL be an integer in YYYYMMDD format to enable easy date range filtering.

#### Scenario: DateKey follows YYYYMMDD format
- **WHEN** a date row is created for January 15, 2023
- **THEN** DateKey SHALL be 20230115

#### Scenario: DateKey is unique
- **WHEN** date rows are generated
- **THEN** each DateKey SHALL be unique across all rows

### Requirement: Date range coverage
The dimension SHALL contain rows for all dates from 2000-01-01 to 2030-12-31.

#### Scenario: Historical dates are included
- **WHEN** querying the dimension for dates from 2000-2007
- **THEN** all calendar days in that range SHALL exist

#### Scenario: Future dates are included
- **WHEN** querying the dimension for dates through 2030
- **THEN** all calendar days through 2030-12-31 SHALL exist

### Requirement: Calendar attributes
All standard calendar attributes SHALL be accurately calculated for each date.

#### Scenario: Year is correct
- **WHEN** a date is January 15, 2023
- **THEN** Year SHALL be 2023

#### Scenario: Quarter is correct
- **WHEN** a date is in January, February, or March
- **THEN** Quarter SHALL be 1

#### Scenario: Month name is correct
- **WHEN** a date is in January
- **THEN** MonthName SHALL be 'January' and MonthNameShort SHALL be 'Jan'

#### Scenario: Day of week is correct
- **WHEN** a date falls on a Monday
- **THEN** DayOfWeek SHALL be 1 and DayOfWeekName SHALL be 'Monday'

#### Scenario: Weekend flag is correct
- **WHEN** a date falls on Saturday or Sunday
- **THEN** IsWeekend SHALL be TRUE

### Requirement: ISO 8601 week numbering
The WeekOfYear SHALL follow ISO 8601 week numbering standards.

#### Scenario: Week numbering follows ISO 8601
- **WHEN** calculating week numbers
- **THEN** weeks SHALL start on Monday
- **AND** week 1 SHALL be the week containing the first Thursday of the year

### Requirement: Fiscal calendar support
Fiscal year attributes SHALL be included to support fiscal period reporting.

#### Scenario: Fiscal year is configurable
- **WHEN** fiscal year is defined (default: January start, same as calendar year)
- **THEN** FiscalYear, FiscalQuarter, and FiscalMonth SHALL be calculated accordingly

#### Scenario: Custom fiscal year start is supported
- **WHEN** fiscal year starts in July
- **THEN** July 1, 2023 SHALL have FiscalYear = 2024 (FY2024)
- **AND** June 30, 2024 SHALL have FiscalYear = 2024

### Requirement: Holiday placeholder
The IsHoliday flag SHALL default to FALSE for all dates with provision for future holiday definitions.

#### Scenario: Holiday flag defaults to false
- **WHEN** date rows are initially generated
- **THEN** IsHoliday SHALL be FALSE for all dates

#### Scenario: Holiday flag is updateable
- **WHEN** holidays need to be defined
- **THEN** specific dates can have IsHoliday updated to TRUE

### Requirement: No duplicates
The dimension SHALL contain exactly one row per calendar day within the date range.

#### Scenario: Each date appears once
- **WHEN** querying the dimension
- **THEN** each distinct date value SHALL appear in exactly one row

### Requirement: Delta table format
The Dim_Date table SHALL be stored in Delta Lake format in the SalesAnalytics Lakehouse.

#### Scenario: Table is created as Delta format
- **WHEN** the date dimension is created
- **THEN** it SHALL be stored as a managed Delta table
- **AND** it SHALL support ACID transactions and time travel

### Requirement: Performance optimization
The date dimension SHALL be fully materialized to eliminate runtime date calculations and improve query performance.

#### Scenario: All attributes are pre-calculated
- **WHEN** the dimension is loaded
- **THEN** all date attributes SHALL be computed and stored
- **AND** queries SHALL NOT need to calculate date attributes at runtime
