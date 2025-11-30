import json
import copy
import os
from pathlib import Path

# Folder where this Python file is located
CURRENT_DIR = Path(__file__).resolve().parent

# Build path to target folder relative to this file
BASE_DIR = CURRENT_DIR / "target"

INPUT_FILE = os.path.join(BASE_DIR, "manifest.json")
OUTPUT_FILE = os.path.join(BASE_DIR, "manifest.json")

TARGET_SCHEMA = "default_marts"
TARGET_NAMES = {"dim_date", "dim_venue", "dim_weather", "dim_team", "fact_match"}
NEW_DATABASE = "default"

with open(INPUT_FILE, "r", encoding="utf-8") as f:
    data = json.load(f)

nodes = data.get("nodes", {})

for node_id, node in nodes.items():
    # ainult dbt modelid
    if not node_id.startswith("model."):
        continue

    name = node.get("name")
    schema = node.get("schema")
    database = node.get("database")

    if schema == TARGET_SCHEMA and name in TARGET_NAMES:
        if database == "" or database is None:
            print(f"Updating database for {node_id} → '{NEW_DATABASE}'")
            node["database"] = NEW_DATABASE

# salvesta tagasi
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(data, f, indent=2, ensure_ascii=False)

print(f"Done. New file created: {OUTPUT_FILE}")
