# Execute medallion notebooks in sequence using fab job run
$lakehouseId = "3f7f544a-bac8-49a1-9585-ad008b10d9cb"  # SalesAnalytics

$notebooks = @(
    "01_Bronze_Layer_Ingestion",
    "02_Silver_Layer_Transform",
    "03_Gold_Layer_StarSchema"
)

Write-Host "🚀 Starting Medallion Pipeline Execution" -ForegroundColor Green
Write-Host "Lakehouse: SalesAnalytics ($lakehouseId)" -ForegroundColor Cyan
Write-Host ""

# Create lakehouse config for notebooks
$config = @{
    defaultLakehouse = @{
        name = "SalesAnalytics"
        id = $lakehouseId
        workspaceId = "ec826010-6b5e-4526-a88c-38b9511e2927"
    }
} | ConvertTo-Json -Depth 5

$configFile = "notebook_config.json"
$config | Out-File -FilePath $configFile -Encoding utf8

foreach ($notebook in $notebooks) {
    Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray
    Write-Host "▶ Executing: $notebook.Notebook" -ForegroundColor Yellow
    Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray
    
    # Run notebook synchronously (will wait for completion)
    Write-Host "$(Get-Date -Format 'HH:mm:ss') - Starting execution..." -ForegroundColor Cyan
    $startTime = Get-Date
    
    fab job run "SalesLT.Workspace/$notebook.Notebook" -C $configFile --timeout 1800 --polling_interval 10
    
    $duration = ((Get-Date) - $startTime).TotalSeconds
    Write-Host "$(Get-Date -Format 'HH:mm:ss') - Completed in $([math]::Round($duration, 1)) seconds" -ForegroundColor Green
    Write-Host ""
}

Remove-Item $configFile -ErrorAction SilentlyContinue

Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray
Write-Host "✅ All notebooks executed successfully!" -ForegroundColor Green
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray
