# Update notebook definitions with content
$workspaceId = "ec826010-6b5e-4526-a88c-38b9511e2927"

$notebooks = @(
    @{ name = "00_Orchestrator_Medallion"; id = "d07d55a3-a096-45b5-808f-4a16f91c08db" },
    @{ name = "01_Bronze_Layer_Ingestion"; id = "8cd4338e-127a-4eb1-87c5-f82fe3ac1961" },
    @{ name = "02_Silver_Layer_Transform"; id = "097a697f-d0f9-41c0-8f91-39048913497d" },
    @{ name = "03_Gold_Layer_StarSchema"; id = "58ce4821-0f3f-4a2d-a7c7-8e42da2c9f4f" }
)

foreach ($notebook in $notebooks) {
    Write-Host "`n📝 Updating $($notebook.name)..." -ForegroundColor Cyan
    
    # Read the .ipynb file
    $ipynbPath = "$($notebook.name).ipynb"
    if (-not (Test-Path $ipynbPath)) {
        Write-Host "❌ File not found: $ipynbPath" -ForegroundColor Red
        continue
    }
    
    # Read and base64 encode the notebook content
    $notebookBytes = [System.IO.File]::ReadAllBytes($ipynbPath)
    $base64Content = [System.Convert]::ToBase64String($notebookBytes)
    
    # Create the updateDefinition payload
    $payload = @{
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
    
    # Save payload to temp file
    $payloadFile = "update_payload_$($notebook.name).json"
    $payload | Out-File -FilePath $payloadFile -Encoding utf8
    
    # Update notebook using fab api
    Write-Host "Updating $($notebook.name) in Fabric..." -ForegroundColor Yellow
    $endpoint = "workspaces/$workspaceId/items/$($notebook.id)/updateDefinition"
    fab api $endpoint -X post -i $payloadFile
    
    # Clean up temp file
    Remove-Item $payloadFile -ErrorAction SilentlyContinue
    
    Start-Sleep -Seconds 2
}

Write-Host "`n✅ All notebooks updated with content!" -ForegroundColor Green
