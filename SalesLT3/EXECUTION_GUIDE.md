# Medallion Architecture - Execution Guide

## 🚀 Quick Start: Execute in Microsoft Fabric

### Option 1: Upload Notebooks via Fabric UI (Recommended)

1. **Open your SalesLT workspace in Fabric**
   - Navigate to: https://app.fabric.microsoft.com/groups/ec826010-6b5e-4526-a88c-38b9511e2927

2. **Upload the notebooks****
   - Click "Upload" → "Notebook"
   - Upload in this order:
     1. `01_Bronze_Layer_Ingestion.py`
     2. `02_Silver_Layer_Transform.py`
     3. `03_Gold_Layer_StarSchema.py`
     4. `00_Orchestrator_Medallion.py`

3. **Execute the pipeline**
   - Open `00_Orchestrator_Medallion` notebook
   - Click "Run all" or execute cells sequentially
   
   OR execute individually:
   - Run `01_Bronze_Layer_Ingestion` (15-30 mins)
   - Run `02_Silver_Layer_Transform` (10-20 mins)
   - Run `03_Gold_Layer_StarSchema` (15-25 mins)

### Option 2: Use Fabric REST API (Advanced)

```powershell
# Set variables
$workspaceId = "ec826010-6b5e-4526-a88c-38b9511e2927"
$token = az account get-access-token --resource https://api.fabric.microsoft.com --query accessToken -o tsv

# Upload notebook (example for Bronze layer)
$headers = @{
    "Authorization" = "Bearer $token"
    "Content-Type" = "application/json"
}

$body = @{
    displayName = "01_Bronze_Layer_Ingestion"
    type = "Notebook"
} | ConvertTo-Json

Invoke-RestMethod -Method Post -Uri "https://api.fabric.microsoft.com/v1/workspaces/$workspaceId/notebooks" -Headers $headers -Body $body
```

### Option 3: Execute from Command Line

```powershell
# Authenticate to Fabric
az login

# Get access token
$token = az account get-access-token --resource https://api.fabric.microsoft.com --query accessToken -o tsv

# Execute notebook via API
$notebookId = "<your-notebook-id>"
Invoke-RestMethod -Method Post `
    -Uri "https://api.fabric.microsoft.com/v1/workspaces/$workspaceId/notebooks/$notebookId/jobs/instances" `
    -Headers @{ "Authorization" = "Bearer $token" }
```

## 📋 Prerequisites

### ✅ Verify Before Execution

1. **Source Data Available**
   ```sql
   -- Run in Fabric SQL Endpoint
   SELECT COUNT(*) FROM RvDSQL.MountedRelationalDatabase.SalesLT.Customer;
   SELECT COUNT(*) FROM RvDSQL.MountedRelationalDatabase.SalesLT.Product;
   SELECT COUNT(*) FROM RvDSQL.MountedRelationalDatabase.SalesLT.SalesOrderDetail;
   ```
   
   Expected: Customer ~847, Product ~295, SalesOrderDetail ~542

2. **Target Lakehouse Exists**
   - Verify `SalesAnalytics` lakehouse exists in SalesLT workspace
   - If not, create it via Fabric UI

3. **Capacity Available**
   - Ensure workspace has compute capacity assigned
   - Check capacity isn't paused

## 🏗️ Execution Architecture

```
┌─────────────────────────────────────────┐
│  00_Orchestrator_Medallion             │
│  (Master controller)                    │
└───────────┬─────────────────────────────┘
            │
            ├──► 01_Bronze_Layer_Ingestion
            │    • Reads from RvDSQL.MountedRelationalDatabase
            │    • Writes to bronze_* tables
            │    • Duration: ~15-30 mins
            │    • Output: 10 tables, ~2K rows
            │
            ├──► 02_Silver_Layer_Transform
            │    • Reads from bronze_* tables
            │    • Applies data quality rules
            │    • Writes to silver_* tables
            │    • Duration: ~10-20 mins
            │    • Output: 7 tables with quality flags
            │
            └──► 03_Gold_Layer_StarSchema
                 • Reads from silver_* tables
                 • Builds dimensional model
                 • Writes to gold_* tables
                 • Duration: ~15-25 mins
                 • Output: 5 tables (4 dims + 1 fact)
```

## 📊 Expected Execution Timeline

| Layer  | Duration | Tables Created | Total Rows  |
|--------|----------|----------------|-------------|
| Bronze | 15-30min | 10             | ~2,000      |
| Silver | 10-20min | 7              | ~1,500      |
| Gold   | 15-25min | 5              | ~13,000     |
| **Total** | **40-75min** | **22** | **~16,500** |

## 🔍 Monitoring Execution

### Check Progress

1. **Via Fabric UI**
   - Open notebook
   - View "Run history" tab
   - Check cell outputs for row counts

2. **Via SQL Query**
   ```sql
   -- Check Bronze layer
   SELECT 'bronze_Customer' as table_name, COUNT(*) as row_count 
   FROM SalesAnalytics.bronze_Customer
   UNION ALL
   SELECT 'bronze_Product', COUNT(*) FROM SalesAnalytics.bronze_Product
   UNION ALL
   SELECT 'bronze_SalesOrderDetail', COUNT(*) FROM SalesAnalytics.bronze_SalesOrderDetail;
   
   -- Check Gold layer (final output)
   SELECT 'gold_Fact_Sales' as table_name, COUNT(*) as row_count 
   FROM SalesAnalytics.gold_Fact_Sales
   UNION ALL
   SELECT 'gold_Dim_Customer', COUNT(*) FROM SalesAnalytics.gold_Dim_Customer
   UNION ALL
   SELECT 'gold_Dim_Product', COUNT(*) FROM SalesAnalytics.gold_Dim_Product;
   ```

### View Logs

Each notebook includes logging output:
```python
# Example log output
[INFO] Bronze layer configuration loaded. Will ingest 10 tables.
[INFO] Starting Bronze ingestion for Customer...
[INFO]   Read 847 rows from source Customer
[INFO] ✓ bronze_Customer: 847 rows ingested in 12.34s
```

## ✅ Validation Queries (After Execution)

### Verify Data Quality

```sql
-- 1. Check all layers have data
SELECT 'Bronze' as layer, COUNT(*) as table_count
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_NAME LIKE 'bronze_%'
UNION ALL
SELECT 'Silver', COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'silver_%'
UNION ALL  
SELECT 'Gold', COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'gold_%';

-- 2. Validate fact table metrics
SELECT 
    COUNT(*) as total_row_count,
    COUNT(DISTINCT SalesOrderID) as unique_orders,
    SUM(LineTotal) as total_revenue,
    SUM(LineProfit) as total_profit,
    (SUM(LineProfit) / SUM(LineTotal) * 100) as profit_margin_pct
FROM SalesAnalytics.gold_Fact_Sales;

-- 3. Check dimension row counts
SELECT 'Dim_Date' as dimension, COUNT(*) FROM SalesAnalytics.gold_Dim_Date
UNION ALL SELECT 'Dim_Customer', COUNT(*) FROM SalesAnalytics.gold_Dim_Customer
UNION ALL SELECT 'Dim_Product', COUNT(*) FROM SalesAnalytics.gold_Dim_Product
UNION ALL SELECT 'Dim_Address', COUNT(*) FROM SalesAnalytics.gold_Dim_Address;

-- Expected results:
--   Dim_Date: ~11,300 rows
--   Dim_Customer: ~847 rows  
--   Dim_Product: ~295 rowsDim_Address: ~450 rows
```

### Test Analytics Queries

```sql
-- Revenue by year
SELECT 
    d.Year,
    COUNT(DISTINCT f.SalesOrderID) as Orders,
    SUM(f.LineTotal) as Revenue,
    SUM(f.LineProfit) as Profit
FROM SalesAnalytics.gold_Fact_Sales f
JOIN SalesAnalytics.gold_Dim_Date d ON f.OrderDateKey = d.DateKey
GROUP BY d.Year
ORDER BY d.Year;

-- Top 10 products
SELECT TOP 10
    p.ProductNumber,
    p.ProductCategoryName,
    SUM(f.OrderQty) as Quantity,
    SUM(f.LineTotal) as Revenue
FROM SalesAnalytics.gold_Fact_Sales f
JOIN SalesAnalytics.gold_Dim_Product p ON f.ProductKey = p.ProductKey
GROUP BY p.ProductNumber, p.ProductCategoryName
ORDER BY Revenue DESC;
```

## 🔧 Troubleshooting

### Common Issues

| Issue | Solution |
|-------|----------|
| "Table not found" error | 1. Verify source mirrored database is accessible<br>2. Check lakehouse connection<br>3. Ensure previous layer completed |
| "No compute available" | 1. Check workspace capacity<br>2. Ensure capacity isn't paused<br>3. Wait for resources to free up |
| "Permission denied" | 1. Verify workspace access<br>2. Check lakehouse write permissions<br>3. Ensure you're a workspace admin/contributor |
| Slow execution | 1. Normal for first run (cold start)<br>2. Check capacity size<br>3. Consider incremental loads for future runs |

### Debug Mode

To enable verbose logging:
```python
# Add to top of any notebook
import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
```

## 🔄 Scheduling for Production

### Option 1: Fabric Data Pipeline

1. Create Data Pipeline in Fabric workspace
2. Add "Notebook" activities for each layer:
   - Notebook 1: 01_Bronze_Layer_Ingestion
   - Notebook 2: 02_Silver_Layer_Transform
   - Notebook 3: 03_Gold_Layer_StarSchema
3. Set dependencies (sequential execution)
4. Configure schedule (e.g., daily at 2 AM)

### Option 2: Orchestrator Notebook + Schedule

1. Open `00_Orchestrator_Medallion` notebook
2. Click "Schedule" in toolbar
3. Set recurrence:
   - Frequency: Daily
   - Time: 02:00 AM
   - Time zone: Your local timezone
4. Save schedule

## 📈 Performance Optimization (Future)

After initial successful run, consider:

1. **Incremental Loading**
   - Modify Bronze to use ModifiedDate filter
   - Implement Delta MERGE in Silver/Gold

2. **Partitioning**
   - Partition Gold fact by Year/Month
   - Optimize for date range queries

3. **Caching**
   - Cache frequently used dimensions
   - Use Fabric semantic models

4. **Parallel Execution**
   - Run independent dimensions in parallel
   - Use notebook concurrency features

## 📞 Support

If execution fails:
1. Check execution logs in notebook run history
2. Verify prerequisites checklist above
3. Run validation queries to identify failing layer
4. Review error messages in cell outputs

## ✨ Success Indicators

After successful execution, you should see:

✅ 22 tables created (10 Bronze, 7 Silver, 5 Gold)  
✅ ~16,500 total rows across all layers  
✅ Gold star schema ready for analytics  
✅ Sample queries return valid results  
✅ No errors in execution logs  

**Ready to build Power BI reports on the Gold layer!** 🎉
