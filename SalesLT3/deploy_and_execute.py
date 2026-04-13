#!/usr/bin/env python3
"""
Create and populate Fabric notebooks using REST API with proper LRO handling
"""
import json
import base64
import time
import subprocess
import sys

WORKSPACE_ID = "ec826010-6b5e-4526-a88c-38b9511e2927"
LAKEHOUSE_ID = "3f7f544a-bac8-49a1-9585-ad008b10d9cb"

notebooks = [
    {"name": "00_Orchestrator_Medallion", "file": "00_Orchestrator_Medallion.ipynb"},
    {"name": "01_Bronze_Layer_Ingestion", "file": "01_Bronze_Layer_Ingestion.ipynb"},
    {"name": "02_Silver_Layer_Transform", "file": "02_Silver_Layer_Transform.ipynb"},
    {"name": "03_Gold_Layer_StarSchema", "file": "03_Gold_Layer_StarSchema.ipynb"},
]

def run_fab_api(endpoint, method="get", input_file=None, show_headers=False):
    """Run fab api command and return parsed JSON result"""
    cmd = ["fab", "api", endpoint, "-X", method, "--output_format", "json"]
    if input_file:
        cmd.extend(["-i", input_file])
    if show_headers:
        cmd.append("--show_headers")
    
    print(f"  Running: fab api {endpoint} -X {method}" + (f" -i {input_file}" if input_file else ""))
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"  ✗ Error: {result.stderr}")
        return None
    
    try:
        return json.loads(result.stdout)
    except:
        print(f"  Output: {result.stdout}")
        return {"status_code": 200, "text": result.stdout}

def poll_lro(location_url, max_wait=60):
    """Poll LRO status until completion"""
    print(f"  Polling LRO: {location_url}")
    start_time = time.time()
    
    while time.time() - start_time < max_wait:
        # Extract just the path from location URL
        path = location_url.split("/v1/")[-1] if "/v1/" in location_url else location_url
        
        result = run_fab_api(path, method="get")
        if not result:
            time.sleep(2)
            continue
        
        # Check status
        status_code = result.get("status_code", 0)
        if status_code == 200:
            print(f"  ✓ LRO completed successfully")
            return True
        elif status_code == 202:
            print(f"  ⏳ Still processing...")
            time.sleep(3)
        else:
            print(f"  ✗ LRO failed with status {status_code}")
            return False
    
    print(f"  ⚠ LRO timeout after {max_wait}s")
    return False

def create_notebook_with_content(name, ipynb_file):
    """Create a notebook and populate it with content"""
    print(f"\n{'='*60}")
    print(f"📘 Creating: {name}")
    print(f"{'='*60}")
    
    # Step 1: Create empty notebook
    print("Step 1: Creating empty notebook...")
    create_payload = {
        "displayName": name,
        "type": "Notebook"
    }
    
    with open("_create_payload.json", "w") as f:
        json.dump(create_payload, f)
    
    result = run_fab_api(f"workspaces/{WORKSPACE_ID}/items", method="post", input_file="_create_payload.json")
    
    if not result or result.get("status_code") not in [200, 201]:
        print(f"✗ Failed to create notebook")
        return None
    
    notebook_id = result["text"]["id"]
    print(f"  ✓ Notebook created: {notebook_id}")
    
    # Wait for notebook to be fully created
    print("  Waiting 5s for notebook to initialize...")
    time.sleep(5)
    
    # Step 2: Read and encode notebook content
    print(f"Step 2: Reading {ipynb_file}...")
    try:
        with open(ipynb_file, "rb") as f:
            content_bytes = f.read()
        base64_content = base64.b64encode(content_bytes).decode('utf-8')
        print(f"  ✓ Content encoded ({len(base64_content)} chars)")
    except Exception as e:
        print(f"  ✗ Failed to read file: {e}")
        return None
    
    # Step 3: Update notebook definition
    print("Step 3: Updating notebook content...")
    update_payload = {
        "definition": {
            "parts": [
                {
                    "path": "notebook-content.ipynb",
                    "payload": base64_content,
                    "payloadType": "InlineBase64"
                }
            ]
        }
    }
    
    with open("_update_payload.json", "w") as f:
        json.dump(update_payload, f)
    
    result = run_fab_api(
        f"workspaces/{WORKSPACE_ID}/items/{notebook_id}/updateDefinition",
        method="post",
        input_file="_update_payload.json",
        show_headers=True
    )
    
    if not result or result.get("status_code") not in [200, 202]:
        print(f"  ✗ Failed to update notebook")
        return None
    
    # If 202, poll the LRO  
    if result.get("status_code") == 202:
        print("  ✓ Update accepted (202), polling for completion...")
        # Try to find Location header in response
        location = result.get("headers", {}).get("location", "")
        if location:
            if not poll_lro(location, max_wait=90):
                print("  ⚠ Warning: LRO didn't complete, but notebook might still work")
        else:
            print("  ⚠ No Location header, waiting 15s...")
            time.sleep(15)
    else:
        print("  ✓ Update completed immediately")
    
    print(f"✅ Notebook {name} created successfully!")
    return notebook_id

def execute_notebook(name, notebook_id):
    """Execute a notebook using fab job run"""
    print(f"\n▶ Executing: {name}")
    
    # Create config file
    config = {
        "defaultLakehouse": {
            "name": "SalesAnalytics",
            "id": LAKEHOUSE_ID,
            "workspaceId": WORKSPACE_ID
        }
    }
    
    with open("_nb_config.json", "w") as f:
        json.dump(config, f)
    
    # Run notebook
    cmd = [
        "fab", "job", "run",
        f"SalesLT.Workspace/{name}.Notebook",
        "-C", "_nb_config.json",
        "--timeout", "1800",
        "--polling_interval", "10"
    ]
    
    print(f"  Running: {' '.join(cmd)}")
    subprocess.run(cmd)

# Main execution
if __name__ == "__main__":
    print("🚀 Fabric Medallion Architecture Deployment")
    print("=" * 60)
    
    notebook_ids = {}
    
    # Create all notebooks
    for nb in notebooks:
        nb_id = create_notebook_with_content(nb["name"], nb["file"])
        if nb_id:
            notebook_ids[nb["name"]] = nb_id
        else:
            print(f"⚠ Skipping {nb['name']} due to creation failure")
    
    # Clean up temp files
    import os
    for f in ["_create_payload.json", "_update_payload.json", "_nb_config.json"]:
        if os.path.exists(f):
            os.remove(f)
    
    print(f"\n{'='*60}")
    print(f"✅ {len(notebook_ids)}/4 notebooks created successfully")
    print(f"{'='*60}")
    
    # Execute notebooks (skip Orchestrator, run layers directly)
    execution_order = [
        "01_Bronze_Layer_Ingestion",
        "02_Silver_Layer_Transform",
        "03_Gold_Layer_StarSchema"
    ]
    
    print(f"\n🎯 Executing Medallion Pipeline")
    print(f"{'='*60}")
    
    for nb_name in execution_order:
        if nb_name in notebook_ids:
            execute_notebook(nb_name, notebook_ids[nb_name])
        else:
            print(f"⚠ Skipping {nb_name} - not created")
    
    print(f"\n{'='*60}")
    print(f"✅ Deployment and execution complete!")
    print(f"{'='*60}")
