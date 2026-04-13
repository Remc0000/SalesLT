# Upload notebooks to Fabric using REST API
$workspaceId = "ec826010-6b5e-4526-a88c-38b9511e2927"

$notebooks = @(
    "00_Orchestrator_Medallion",
    "01_Bronze_Layer_Ingestion",
    "02_Silver_Layer_Transform",
    "03_Gold_Layer_StarSchema"
)

foreach ($notebook in $notebooks) {
    Write-Host "`n📤 Uploading $notebook..." -ForegroundColor Cyan
    
    # Read the .ipynb file
    $ipynbPath = "$notebook.ipynb"
    if (-not (Test-Path $ipynbPath)) {
        Write-Host "❌ File not found: $ipynbPath" -ForegroundColor Red
        continue
    }
    
    # Read and base64 encode the notebook content
    $notebookBytes = [System.IO.File]::ReadAllBytes($ipynbPath)
    $base64Content = [System.Convert]::ToBase64String($notebookBytes)
    
    # Create the payload for notebook creation
    $payload = @{
        displayName = $notebook
        type = "Notebook"
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
    $payloadFile = "payload_$notebook.json"
    $payload | Out-File -FilePath $payloadFile -Encoding utf8
    
    # Create notebook using fab api
    Write-Host "Creating $notebook in Fabric..." -ForegroundColor Yellow
    fab api "workspaces/$workspaceId/items" -X post -i $payloadFile
    
    # Clean up temp file
    Remove-Item $payloadFile -ErrorAction SilentlyContinue
    
    Start-Sleep -Seconds 2
}

Write-Host "`n✅ All notebooks uploaded!" -ForegroundColor Green
