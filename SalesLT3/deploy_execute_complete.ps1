# Deploy and Execute Medallion Architecture using PowerShell
$workspaceId = "ec826010-6b5e-4526-a88c-38b9511e2927"
$lakehouseId = "3f7f544a-bac8-49a1-9585-ad008b10d9cb"

$notebooks = @(
    @{ name = "00_Orchestrator_Medallion"; file = "00_Orchestrator_Medallion.ipynb" },
    @{ name = "01_Bronze_Layer_Ingestion"; file = "01_Bronze_Layer_Ingestion.ipynb" },
    @{ name = "02_Silver_Layer_Transform"; file = "02_Silver_Layer_Transform.ipynb" },
    @{ name = "03_Gold_Layer_StarSchema"; file = "03_Gold_Layer_StarSchema.ipynb" }
)

Write-Host "🚀 Fabric Medallion Architecture Deployment" -ForegroundColor Green
Write-Host "=" * 60 -ForegroundColor Gray

$notebookIds = @{}

foreach ($nb in $notebooks) {
    Write-Host "`n============================================================" -ForegroundColor Gray
    Write-Host "📘 Creating: $($nb.name)" -ForegroundColor Cyan
    Write-Host "============================================================" -ForegroundColor Gray
    
    # Step 1: Create empty notebook
    Write-Host "Step 1: Creating empty notebook..." -ForegroundColor Yellow
    $createPayload = @{
        displayName = $nb.name
        type = "Notebook"
    } | ConvertTo-Json -Compress
    
    $createPayload | Out-File "create_$($nb.name).json" -Encoding utf8
    
    $createResult = fab api "workspaces/$workspaceId/items" -X post -i "create_$($nb.name).json" --output_format json | ConvertFrom-Json
    
    if ($createResult.status_code -ne 201) {
        Write-Host "  ✗ Creation failed: $($createResult.text.message)" -ForegroundColor Red
        continue
    }
    
    $notebookId = $createResult.text.id
    Write-Host "  ✓ Notebook created: $notebookId" -ForegroundColor Green
    
    # Wait for notebook initialization
    Write-Host "  Waiting 3s for initialization..." -ForegroundColor Gray
    Start-Sleep -Seconds 3
    
    # Step 2: Read and encode content
    Write-Host "Step 2: Reading $($nb.file)..." -ForegroundColor Yellow
    if (-not (Test-Path $nb.file)) {
        Write-Host "  ✗ File not found: $($nb.file)" -ForegroundColor Red
        continue
    }
    
    $contentBytes = [System.IO.File]::ReadAllBytes($nb.file)
    $base64Content = [System.Convert]::ToBase64String($contentBytes)
    Write-Host "  ✓ Content encoded ($($base64Content.Length) chars)" -ForegroundColor Green
    
    # Step 3: Update definition
    Write-Host "Step 3: Updating notebook content..." -ForegroundColor Yellow
    $updatePayload = @{
        definition = @{
            parts = @(
                @{
                    path = "notebook-content.ipynb"
                    payload = $base64Content
                    payloadType = "InlineBase64"
                }
            )
        }
    } | ConvertTo-Json -Depth 10
    
    $updatePayload | Out-File "update_$($nb.name).json" -Encoding utf8
    
    $updateResult = fab api "workspaces/$workspaceId/items/$notebookId/updateDefinition" -X post -i "update_$($nb.name).json" --output_format json | ConvertFrom-Json
    
    if ($updateResult.status_code -eq 202) {
        Write-Host "  ✓ Update accepted (202), waiting for LRO..." -ForegroundColor Green
        Start-Sleep -Seconds 15
    } elseif ($updateResult.status_code -eq 200) {
        Write-Host "  ✓ Update completed (200)" -ForegroundColor Green
    } else {
        Write-Host "  ⚠ Update status: $($updateResult.status_code)" -ForegroundColor Yellow
    }
    
    $notebookIds[$nb.name] = $notebookId
    Write-Host "✅ $($nb.name) created and populated!" -ForegroundColor Green
    
    # Clean up temp files
    Remove-Item "create_$($nb.name).json" -ErrorAction SilentlyContinue
    Remove-Item "update_$($nb.name).json" -ErrorAction SilentlyContinue
}

Write-Host "`n============================================================" -ForegroundColor Gray
Write-Host "✅ $($notebookIds.Count)/4 notebooks created successfully" -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Gray

# Configure lakehouse
$config = @{
    defaultLakehouse = @{
        name = "SalesAnalytics"
        id = $lakehouseId
        workspaceId = $workspaceId
    }
} | ConvertTo-Json -Depth 5

$config | Out-File "nb_config.json" -Encoding utf8

# Execute notebooks
$executionOrder = @(
    "01_Bronze_Layer_Ingestion",
    "02_Silver_Layer_Transform",
    "03_Gold_Layer_StarSchema"
)

Write-Host "`n🎯 Executing Medallion Pipeline" -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Gray

foreach ($nbName in $executionOrder) {
    if (-not $notebookIds.ContainsKey($nbName)) {
        Write-Host "`n⚠ Skipping $nbName - not created" -ForegroundColor Yellow
        continue
    }
    
    Write-Host "`n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray
    Write-Host "▶ Executing: $nbName" -ForegroundColor Cyan
    Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray
    
    $startTime = Get-Date
    fab job run "SalesLT.Workspace/$nbName.Notebook" -C nb_config.json --timeout 1800 --polling_interval 10
    
    $duration = ((Get-Date) - $startTime).TotalSeconds
    Write-Host "  Completed in $([math]::Round($duration, 1)) seconds" -ForegroundColor Gray
}

Remove-Item "nb_config.json" -ErrorAction SilentlyContinue

Write-Host "`n============================================================" -ForegroundColor Gray
Write-Host "✅ Deployment and execution complete!" -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Gray
