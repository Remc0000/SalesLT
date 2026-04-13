#!/usr/bin/env python3
"""Convert Python scripts to Jupyter notebooks"""
import json
import os

def py_to_notebook(py_file, nb_file):
    """Convert Python file to Jupyter notebook format"""
    with open(py_file, 'r', encoding='utf-8') as f:
        code = f.read()
    
    # Split into cells by double newlines or keep as single cell
    cells = []
    
    # Create a single code cell with all the content
    cells.append({
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": code.split('\n')
    })
    
    notebook = {
        "cells": cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Synapse PySpark",
                "language": "Python",
                "name": "synapse_pyspark"
            },
            "language_info": {
                "name": "python"
            }
        },
        "nbformat": 4,
        "nbformat_minor": 2
    }
    
    with open(nb_file, 'w', encoding='utf-8') as f:
        json.dump(notebook, f, indent=2)
    
    print(f"✓ Converted {py_file} → {nb_file}")

# Convert all medallion notebooks
notebooks = [
    "00_Orchestrator_Medallion",
    "01_Bronze_Layer_Ingestion",
    "02_Silver_Layer_Transform",
    "03_Gold_Layer_StarSchema"
]

for nb_name in notebooks:
    py_file = f"{nb_name}.py"
    nb_file = f"{nb_name}.ipynb"
    if os.path.exists(py_file):
        py_to_notebook(py_file, nb_file)
    else:
        print(f"✗ File not found: {py_file}")

print("\n✓ All notebooks converted!")
