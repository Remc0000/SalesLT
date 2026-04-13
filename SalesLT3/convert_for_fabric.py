#!/usr/bin/env python3
"""
Convert Databricks-style .py notebooks to Fabric notebook directory structure.

fab import expects:
  <name>.Notebook/
    ├── .platform
    └── notebook-content.ipynb

The .py files use:
  # Databricks notebook source
  # MAGIC %md / # MAGIC %sql   → markdown/SQL cells
  # COMMAND ----------           → cell separator
"""
import json
import os
import re

LAKEHOUSE_ID = "3f7f544a-bac8-49a1-9585-ad008b10d9cb"

def parse_databricks_py(py_content):
    """Parse Databricks .py format into cells"""
    lines = py_content.split('\n')
    
    # Skip first line if it's the header
    start = 0
    if lines and lines[0].strip() == '# Databricks notebook source':
        start = 1
    
    # Split by # COMMAND ----------
    raw_cells = []
    current_cell = []
    
    for line in lines[start:]:
        if line.strip() == '# COMMAND ----------':
            if current_cell:
                raw_cells.append(current_cell)
            current_cell = []
        else:
            current_cell.append(line)
    
    if current_cell:
        raw_cells.append(current_cell)
    
    # Convert each raw cell to notebook cell
    cells = []
    for raw_cell in raw_cells:
        # Strip leading/trailing empty lines
        while raw_cell and raw_cell[0].strip() == '':
            raw_cell = raw_cell[1:]
        while raw_cell and raw_cell[-1].strip() == '':
            raw_cell = raw_cell[:-1]
        
        if not raw_cell:
            continue
        
        # Detect cell type by checking if ALL lines are MAGIC %md
        is_markdown = all(
            l.startswith('# MAGIC %md') or l.startswith('# MAGIC ') or l.strip() == ''
            for l in raw_cell
        ) and any(l.startswith('# MAGIC %md') for l in raw_cell)
        
        is_sql = all(
            l.startswith('# MAGIC %sql') or l.startswith('# MAGIC ') or l.strip() == ''
            for l in raw_cell
        ) and any(l.startswith('# MAGIC %sql') for l in raw_cell)
        
        if is_markdown:
            # Extract markdown content - strip "# MAGIC %md" and "# MAGIC "
            md_lines = []
            first_md = True
            for l in raw_cell:
                if l.startswith('# MAGIC %md') and first_md:
                    # First line with %md - skip the marker
                    rest = l[len('# MAGIC %md'):].strip()
                    if rest:
                        md_lines.append(rest)
                    first_md = False
                elif l.startswith('# MAGIC '):
                    md_lines.append(l[len('# MAGIC '):])
                elif l.strip() == '':
                    md_lines.append('')
            
            source = '\n'.join(md_lines).strip()
            cells.append({
                "cell_type": "markdown",
                "metadata": {"nteract": {"transient": {"deleting": False}}},
                "source": [s + '\n' for s in source.split('\n')[:-1]] + [source.split('\n')[-1]]
            })
        
        elif is_sql:
            # Extract SQL content
            sql_lines = []
            first_sql = True
            for l in raw_cell:
                if l.startswith('# MAGIC %sql') and first_sql:
                    first_sql = False
                    continue
                elif l.startswith('# MAGIC '):
                    sql_lines.append(l[len('# MAGIC '):])
                elif l.strip() == '':
                    sql_lines.append('')
            
            source = '%%sql\n' + '\n'.join(sql_lines).strip()
            cells.append({
                "cell_type": "code",
                "metadata": {"microsoft": {"language": "sparksql"}},
                "outputs": [],
                "source": [s + '\n' for s in source.split('\n')[:-1]] + [source.split('\n')[-1]]
            })
        
        else:
            # Python code cell
            source = '\n'.join(raw_cell)
            cells.append({
                "cell_type": "code",
                "metadata": {},
                "outputs": [],
                "source": [s + '\n' for s in source.split('\n')[:-1]] + [source.split('\n')[-1]]
            })
    
    return cells


def create_notebook_dir(name, py_file, output_base='.'):
    """Create a Fabric notebook directory structure"""
    print(f"\n📘 Converting: {py_file} → {name}.Notebook/")
    
    # Read source
    with open(py_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Parse cells
    cells = parse_databricks_py(content)
    print(f"   Parsed {len(cells)} cells ({sum(1 for c in cells if c['cell_type']=='code')} code, " +
          f"{sum(1 for c in cells if c['cell_type']=='markdown')} markdown)")
    
    # Build .ipynb
    notebook = {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {
            "kernel_info": {
                "name": "synapse_pyspark"
            },
            "kernelspec": {
                "display_name": "Synapse PySpark",
                "language": "Python",
                "name": "synapse_pyspark"
            },
            "language_info": {
                "name": "python"
            },
            "microsoft": {
                "language": "python",
                "ms_spell_check": {"ms_spell_check_language": "en"}
            },
            "nteract": {
                "version": "nteract-front-end@1.0.0"
            },
            "spark_compute": {
                "compute_id": "/trident/default"
            },
            "dependencies": {
                "lakehouse": {
                    "default_lakehouse": LAKEHOUSE_ID,
                    "default_lakehouse_name": "SalesAnalytics",
                    "default_lakehouse_workspace_id": "ec826010-6b5e-4526-a88c-38b9511e2927"
                }
            }
        },
        "cells": cells
    }
    
    # Build .platform
    platform = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
        "metadata": {
            "type": "Notebook",
            "displayName": name
        },
        "config": {
            "version": "2.0",
            "logicalId": "00000000-0000-0000-0000-000000000000"
        }
    }
    
    # Create directory
    nb_dir = os.path.join(output_base, f"{name}.Notebook")
    os.makedirs(nb_dir, exist_ok=True)
    
    # Write files
    ipynb_path = os.path.join(nb_dir, "notebook-content.ipynb")
    with open(ipynb_path, 'w', encoding='utf-8') as f:
        json.dump(notebook, f, indent=1, ensure_ascii=False)
    
    platform_path = os.path.join(nb_dir, ".platform")
    with open(platform_path, 'w', encoding='utf-8') as f:
        json.dump(platform, f, indent=2, ensure_ascii=False)
    
    print(f"   ✓ Created {nb_dir}/")
    print(f"     - notebook-content.ipynb ({os.path.getsize(ipynb_path):,} bytes)")
    print(f"     - .platform ({os.path.getsize(platform_path):,} bytes)")
    
    return nb_dir


# Convert all notebooks
notebooks = [
    ("00_Orchestrator_Medallion", "00_Orchestrator_Medallion.py"),
    ("01_Bronze_Layer_Ingestion", "01_Bronze_Layer_Ingestion.py"),
    ("02_Silver_Layer_Transform", "02_Silver_Layer_Transform.py"),
    ("03_Gold_Layer_StarSchema",  "03_Gold_Layer_StarSchema.py"),
]

output_dir = "fabric_import"
os.makedirs(output_dir, exist_ok=True)

print("=" * 60)
print("Converting .py notebooks to Fabric notebook format")
print("=" * 60)

for name, py_file in notebooks:
    if os.path.exists(py_file):
        create_notebook_dir(name, py_file, output_dir)
    else:
        print(f"✗ File not found: {py_file}")

print("\n" + "=" * 60)
print("✅ All notebooks converted!")
print(f"Output directory: {output_dir}/")
print("=" * 60)
