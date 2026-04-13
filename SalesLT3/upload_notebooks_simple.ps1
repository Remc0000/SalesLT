# Upload notebooks to Fabric using fab import
$notebooks = @(
    "00_Orchestrator_Medallion",
    "01_Bronze_Layer_Ingestion",
    "02_Silver_Layer_Transform",
    "03_Gold_Layer_StarSchema"
)

Write-Host "📤 Uploading notebooks to Fabric workspace: SalesLT" -ForegroundColor Cyan
Write-Host ""

foreach ($nb in $notebooks) {
    $ipynbFile = "$nb.ipynb"
    
    if (-not (Test-Path $ipynbFile)) {
        Write-Host "✗ File not found: $ipynbFile" -ForegroundColor Red
        continue
    }
    
    Write-Host "⬆ Uploading: $nb.Notebook" -ForegroundColor Yellow
    Write-Host "  Source: $ipynbFile" -ForegroundColor Gray
    
    fab import "SalesLT.Workspace/$nb.Notebook" -i $ipynbFile -f
    
    Write-Host ""
}

Write-Host "✅ Upload complete!" -ForegroundColor Green
Write-Host ""
Write-Host "Verify notebooks in workspace:" -ForegroundColor Cyan
fab ls SalesLT.Workspace -l
