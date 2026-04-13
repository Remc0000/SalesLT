# Lessons Learned: SalesLT Medallion Architecture Implementation

**Project:** SalesLT Star Schema (Bronze → Silver → Gold)  
**Date:** April 2026  
**Workspace:** SalesLT (Fabric)  
**Pipeline:** Medallion_Pipeline.DataPipeline  

---

## 1. Schema Assumptions Were Wrong

### What happened
The OpenSpec specs assumed column names based on typical SalesLT schemas (e.g., AdventureWorksLT). The actual mirrored database had **different columns** than expected.

### Specific mismatches

| Table | Spec assumed | Actual schema |
|-------|-------------|---------------|
| ProductCategory | `Name` column exists | **No `Name` column** — only ProductCategoryID, ParentProductCategoryID, rowguid, ModifiedDate |
| ProductModel | `Name` column exists | **No `Name` column** — only ProductModelID, rowguid, ModifiedDate |
| Address | `StateProvince`, `CountryRegion` columns exist | **Neither exists** — only AddressID, AddressLine1, AddressLine2, City, PostalCode, rowguid, ModifiedDate |
| SalesOrderDetail | `LineTotal` column exists | **Does not exist** — must be calculated as `OrderQty * UnitPrice * (1 - UnitPriceDiscount)` |
| SalesOrderHeader | `TotalDue` column exists | **Does not exist** — must be calculated as `SubTotal + TaxAmt + Freight` |
| Customer | `FirstName`, `LastName` columns exist | **Neither exists** — only Title, Suffix, CompanyName, SalesPerson, etc. |

### Root cause
- Specs were written based on assumptions about the SalesLT schema without **actually querying the source tables first**
- The mirrored database schema was a subset of the full AdventureWorksLT schema

### Fix for next time
- **ALWAYS inspect actual source schemas before writing specs**
- Add a "Schema Discovery" task as Task 0 in every ETL spec
- Use `fab table schema "Workspace/Lakehouse/Tables/tablename"` to get actual column definitions
- Document discovered schemas in the spec as a verified baseline
- Never assume standard schemas — even well-known sample databases vary by version and mirroring method

---

## 2. PySpark Wildcard Imports Override Python Builtins

### What happened
`from pyspark.sql.functions import *` silently overrides Python's built-in `sum()` function. Code using `sum([list])` (Python builtin) instead calls PySpark's `sum()` (column aggregation), causing `PySparkTypeError`.

### The error
```
PySparkTypeError: [NOT_COLUMN_OR_STR] Argument `col` should be a Column or str, got int.
```

### Fix
```python
# BAD - overrides builtins
from pyspark.sql.functions import *

# GOOD - explicit imports
from pyspark.sql.functions import col, lit, when, sum as spark_sum, ...
import builtins
builtins_sum = builtins.sum  # if you need Python's sum()
```

### Fix for next time
- **Never use wildcard imports in PySpark notebooks** — add this as a coding standard in specs
- Always use explicit imports with aliases for conflicting names (`sum as spark_sum`)
- Include import patterns in the ETL spec's "Coding Standards" section

---

## 3. `createDataFrame` with None Values Fails Type Inference

### What happened
PySpark's `spark.createDataFrame()` cannot infer types when a column contains only `None` values. The "Unknown" address row used `None` for `AddressLine2` and `EndDate`, causing:

```
PySparkValueError: [CANNOT_DETERMINE_TYPE] Some of types cannot be determined after inferring
```

### Fix
Provide an explicit `StructType` schema:
```python
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ...

schema = StructType([
    StructField("AddressKey", IntegerType(), False),
    StructField("AddressLine2", StringType(), True),  # nullable
    StructField("EndDate", DateType(), True),          # nullable
    ...
])
unknown_row = spark.createDataFrame([(-1, None, None, ...)], schema)
```

### Fix for next time
- Spec any "Unknown" / sentinel dimension rows with explicit schema definitions
- Add a coding standard: "Always provide StructType when createDataFrame contains None values"

---

## 4. Ambiguous Column References in PySpark Joins

### What happened
When joining `fact_sales` (which has `OrderDate`) with `dim_date` aliased as `OrderDate`, PySpark throws:

```
[AMBIGUOUS_REFERENCE] Reference 'OrderDate' is ambiguous
```

### Fix
Use non-colliding alias names and DataFrame-qualified drops:
```python
# BAD
dim_date.select(col("Date").alias("OrderDate"), ...)
# after join, "OrderDate" is ambiguous

# GOOD
dim_date.select(col("Date").alias("_join_order_date"), ...)
# join on _join_order_date, then drop it + the original
.drop("_join_order_date").drop(fact_sales.OrderDate)
```

### Fix for next time
- In specs, define join strategies explicitly — specify alias naming conventions
- Standard pattern: prefix join-only columns with `_join_` to avoid collisions
- Always drop both sides of non-equi join columns after the join

---

## 5. fab CLI Notebook Upload Requires Directory Format

### What happened
Attempting to `fab import` a raw `.py` file as a notebook fails. The fab CLI expects a **directory structure**:

```
NotebookName.Notebook/
├── .platform          # JSON: {"$schema":"...", "metadata":{...}, "config":{"type":"Notebook"}}
└── notebook-content.ipynb  # Jupyter notebook format (JSON)
```

Uploading a `.py` file directly creates an **empty notebook** in Fabric with no cells.

### How we fixed it
Created `convert_for_fabric.py` — a converter that:
1. Parses Databricks-format `.py` files (with `# COMMAND ----------` separators)
2. Detects cell types (`# MAGIC %md` → markdown, `# MAGIC %sql` → SQL, else → code)
3. Generates proper `notebook-content.ipynb` in Jupyter format
4. Creates `.platform` metadata file
5. Outputs into `fabric_import/<Name>.Notebook/` directories

### Fix for next time
- **Include `convert_for_fabric.py` in every project that deploys notebooks via fab CLI**
- Or better: author notebooks directly in `.ipynb` format from the start
- Add deployment steps to the spec's tasks, not just the code logic
- Validate uploaded notebooks are non-empty before running pipelines

### How to verify notebooks are not empty after upload
```powershell
# Export and check after import
fab export "Workspace/NotebookName.Notebook" -o ./verify_export -f
$content = Get-Content "./verify_export/NotebookName.Notebook/notebook-content.ipynb" | ConvertFrom-Json
$cellCount = $content.cells.Count
if ($cellCount -eq 0) {
    Write-Error "EMPTY NOTEBOOK DETECTED: NotebookName has 0 cells!"
} else {
    Write-Host "OK: NotebookName has $cellCount cells"
}
Remove-Item ./verify_export -Recurse -Force
```

### Deployment checklist (prevent empty notebooks)
1. Run `convert_for_fabric.py` to generate `fabric_import/` directories
2. Verify each `.Notebook/notebook-content.ipynb` has non-zero `cells` array
3. Run `fab import "Workspace/Name.Notebook" -i "fabric_import/Name.Notebook" -f`
4. After import, export back and confirm cell count matches
5. Only then proceed to pipeline execution

---

## 6. Pipeline Debugging Is Slow — Fail Fast

### What happened
Each pipeline run took ~13-15 minutes (Bronze ~5min, Silver ~4min, Gold ~5min). With 5 iterations to fix errors, that's over an hour of pipeline execution time.

Errors were only visible **after** the failing notebook completed, and the fab CLI only shows `InProgress` / `Failed` — not the actual error.

### Fix for next time
- **Test notebooks individually** before running the full pipeline
- Add `try/except` blocks with clear error messages in each notebook function
- Use signal tables (`_sig_name_ok` / `_sig_name_err`) to expose errors when stdout isn't visible
- Add a "dry run" mode that validates schemas and column existence without writing data
- Consider running Gold layer independently (with `fab job run` on the notebook) after Bronze+Silver succeed

---

## 7. What Was Missing in the OpenSpec Specs

### Missing from the specs entirely

| Gap | Impact | Recommendation |
|-----|--------|----------------|
| **Actual source schema discovery** | All column assumptions were wrong | Add "Schema Discovery" as Task 0 |
| **Deployment procedure** | No guidance on how to get code into Fabric | Add deployment tasks with fab CLI commands |
| **fab CLI format requirements** | Empty notebooks uploaded, wasted hours | Document directory structure requirement |
| **PySpark coding standards** | Wildcard import bug, None type inference | Add coding standards section to ETL spec |
| **Join strategy patterns** | Ambiguous reference errors | Specify alias conventions in design.md |
| **Calculated field definitions** | LineTotal, TotalDue missing from source | Explicitly document which fields need calculation vs. exist in source |
| **Validation/smoke test criteria** | No way to verify correctness without portal | Add expected row counts, sample queries |
| **Error handling patterns** | Silent failures, unhelpful errors | Mandate try/except with logging standards |
| **Pipeline retry strategy** | Manual fix-deploy-run cycle | Document automated deployment script |

### What the specs got right
- Star schema design (dimensions + fact) was sound
- Surrogate key strategy was correct
- SCD Type 2 preparation structure was appropriate
- Medallion architecture layering (Bronze/Silver/Gold) was correct
- The overall data flow and transformation logic was right

---

## 8. Recommended Spec Template Additions

### Add to every ETL OpenSpec change

#### Task 0: Schema Discovery
```markdown
- [ ] Query all source tables and document actual schemas
- [ ] Run `fab table schema` for each source table
- [ ] Compare discovered schema with spec assumptions
- [ ] Update specs to match actual schema
- [ ] Document calculated fields (what exists vs. what must be derived)
```

#### Coding Standards Section (in design.md)
```markdown
## Coding Standards
- NO wildcard imports (`from pyspark.sql.functions import *`)
- Use explicit imports with aliases for conflicting names
- Always provide StructType schema for createDataFrame with nullable columns
- Use `_join_` prefix for temporary join columns
- Wrap table writes in try/except with logging
- Use `mode("overwrite").saveAsTable()` for idempotent writes
```

#### Deployment Tasks Section (in tasks.md)
```markdown
## Deployment Tasks
- [ ] Convert notebooks to Fabric directory format
- [ ] Validate notebook cell counts (not empty)
- [ ] Upload via `fab import` with `-f` flag
- [ ] Verify upload by exporting and checking
- [ ] Create/update pipeline definition
- [ ] Test each notebook individually before pipeline run
- [ ] Run full pipeline and validate all tables created
```

---

## 9. fab CLI Reference (Key Commands)

### Notebook workflow
```powershell
# Convert .py to Fabric format
python convert_for_fabric.py

# Upload notebook (create or update)
fab import "Workspace/Name.Notebook" -i "fabric_import/Name.Notebook" -f

# Export notebook to inspect
fab export "Workspace/Name.Notebook" -o ./export_dir -f

# List workspace items
fab ls "Workspace"

# Check table schemas
fab table schema "Workspace/Lakehouse.Lakehouse/Tables/table_name"
```

### Pipeline workflow
```powershell
# Run pipeline synchronously with timeout
fab job run "Workspace/Pipeline.DataPipeline" --timeout 7200 --polling_interval 15

# Run individual notebook
fab job run "Workspace/Notebook.Notebook" --timeout 1800 --polling_interval 10

# Delete items
fab rm "Workspace/ItemName.ItemType" -f
```

### Important fab CLI v1.5.0 notes
- `--body` flag no longer works — use `-i` for request body input
- `fab import` expects a **directory**, not a single file
- Notebook directory must contain `.platform` + `notebook-content.ipynb`
- Pipeline definition directory must contain `.platform` + `pipeline-content.json`

---

## 10. Integration with Fabric Skills and OpenSpec

### Using OpenSpec for Fabric projects

OpenSpec works well for defining the **what** (dimensional model, transforms, business rules) but needs augmentation for the **how** (deployment, Fabric-specific patterns).

#### Recommended OpenSpec workflow for Fabric ETL:
1. **Explore** → Use `openspec-explore` to investigate source schemas interactively
2. **Discover** → Query actual source tables via fab CLI before proposing
3. **Propose** → Use `openspec-propose` with discovered schemas as input
4. **Validate** → Review specs against actual data (not assumed schemas)
5. **Implement** → Use `openspec-apply-change` with Fabric-specific coding standards
6. **Deploy** → Follow deployment checklist (convert → validate → upload → verify)
7. **Test** → Run notebooks individually, then pipeline
8. **Archive** → Use `openspec-archive-change` after successful pipeline run

#### Fabric Skills integration:
- Use the **Fabric agent** (`@Fabric`) for workspace/item operations
- Use the **FabricNotebook agent** (`@FabricNotebook`) for notebook authoring and debugging  
- Use **Fabric MCP tools** (if available) for direct API access from chat
- Fall back to **fab CLI** for deployment when MCP tools hit limitations

### Key principle
> **Specs should be grounded in discovered reality, not assumptions.** Always run schema discovery before writing transformation specs. The 30 minutes spent discovering schemas upfront saves hours of debugging mismatches later.

---

## Summary: Top 5 Rules for Next Time

1. **Discover first, spec second** — Always query source schemas before writing specs
2. **No wildcard PySpark imports** — Use explicit imports, alias conflicts
3. **Validate deployments** — Verify notebooks have cells after upload, never trust blind imports
4. **Test layers independently** — Run Bronze → verify → Silver → verify → Gold → verify
5. **Include deployment in specs** — fab CLI commands, directory format, pipeline definition are part of the deliverable
