# Databricks notebook source
# MAGIC %md
# MAGIC # Governance inventory snapshot
# MAGIC
# MAGIC Loads the enabled asset adapters, snapshots the active requirements, and
# MAGIC records observed assets and tags at a daily grain.

# COMMAND ----------

import hashlib
import json
import os
import runpy
import sys
import uuid
from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "finops_observability")

CATALOG_SCHEMA = f"{dbutils.widgets.get('catalog')}.{dbutils.widgets.get('schema')}"

_notebook_dir = os.getcwd()
if _notebook_dir not in sys.path:
    sys.path.insert(0, _notebook_dir)

from _collectors import COLLECTORS
from _normalize import merge_discovery_and_enrichment
from _system_discovery import SYSTEM_DISCOVERERS

# A gitignored config/governance_assets_local.py takes precedence when present,
# so customer-specific requirements never need to be committed (same pattern as
# config/excluded_clusters.py).
_config_path = os.path.abspath(os.path.join(
    _notebook_dir, "..", "..", "config", "governance_assets_local.py"))
if not os.path.exists(_config_path):
    _config_path = os.path.abspath(os.path.join(
        _notebook_dir, "..", "..", "config", "governance_assets.py"))
print(f"Loading asset requirements from {os.path.basename(_config_path)}")
ASSET_TYPES = runpy.run_path(_config_path)["ASSET_TYPES"]

w = WorkspaceClient()
workspace_id = str(w.get_workspace_id())
collection_run_id = str(uuid.uuid4())
snapshot_ts = datetime.now(timezone.utc)
snapshot_date = snapshot_ts.date().isoformat()

# COMMAND ----------

# System tables establish the complete asset universe. Workspace APIs are optional
# enrichment: permission gaps may reduce policy/live-state coverage but never remove
# assets or their cost from the model.
asset_rows = []
run_rows = []
requirement_rows = []

for asset_type, config in sorted(ASSET_TYPES.items()):
    canonical = json.dumps(config, sort_keys=True, default=str)
    requirement_rows.append({
        "workspace_id": workspace_id,
        "collection_run_id": collection_run_id,
        "snapshot_ts": snapshot_ts,
        "product": config["product"],
        "asset_type": asset_type,
        "enabled": bool(config.get("enabled", False)),
        "collector": config["collector"],
        "cost_resolver": config["cost_resolver"],
        "required_tags": [str(v).strip().casefold() for v in config.get("required_tags", [])],
        "required_policies": [],
        "config_hash": hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
        "raw_config": canonical,
    })
    if not config.get("enabled", False):
        continue
    collector_name = config["collector"]
    if collector_name not in SYSTEM_DISCOVERERS or collector_name not in COLLECTORS:
        raise ValueError(f"Unknown collector '{collector_name}' for asset_type '{asset_type}'")
    context = {
        "workspace_id": workspace_id,
        "run_id": collection_run_id,
        "snapshot_ts": snapshot_ts,
        "product": config["product"],
        "asset_type": asset_type,
    }
    discovered = SYSTEM_DISCOVERERS[collector_name](spark, context)
    enrichment_error = None
    enriched = []
    if config.get("api_enrichment", True):
        try:
            enriched = COLLECTORS[collector_name](w, context)
        except Exception as error:
            enrichment_error = f"API enrichment failed: {type(error).__name__}: {error}"
            print(enrichment_error)
    collected = merge_discovery_and_enrichment(discovered, enriched)
    # Policy attachment and definition collection are outside this pipeline's scope.
    # Keep compatibility columns empty until the schema is deliberately simplified.
    for asset in collected:
        asset["policies"] = []
        asset["policy_observation_complete"] = False
    asset_rows.extend(collected)
    run_rows.append({
        "workspace_id": workspace_id,
        "collection_run_id": collection_run_id,
        "snapshot_ts": snapshot_ts,
        "asset_type": asset_type,
        "status": "SUCCESS",
        "asset_count": len(collected),
        "error_message": enrichment_error,
    })
    print(
        f"Collected {len(collected)} {asset_type} assets "
        f"({len(discovered)} system, {len(enriched)} API enrichment rows)"
    )

# COMMAND ----------

tag_rows = [
    {
        "workspace_id": asset["workspace_id"],
        "collection_run_id": collection_run_id,
        "snapshot_ts": snapshot_ts,
        "asset_type": asset["asset_type"],
        "asset_id": asset["asset_id"],
        "tag_key": key,
        "tag_value": value,
    }
    for asset in asset_rows
    for key, value in asset["tags"].items()
]

# COMMAND ----------

# One canonical asset/tag snapshot per UTC date. Replacement happens only after
# all configured asset collectors have completed successfully.
for table in (
    "governance_silver_collection_run",
    "governance_silver_requirement_snapshot",
    "governance_silver_asset_inventory_snapshot",
    "governance_silver_asset_tag_snapshot",
):
    spark.sql(f"""
    DELETE FROM {CATALOG_SCHEMA}.{table}
    WHERE workspace_id = '{workspace_id}'
      AND snapshot_date = DATE '{snapshot_date}'
    """)

schemas = {
    "governance_silver_collection_run": """
      workspace_id STRING, collection_run_id STRING, snapshot_ts TIMESTAMP,
      asset_type STRING, status STRING, asset_count BIGINT, error_message STRING
    """,
    "governance_silver_requirement_snapshot": """
      workspace_id STRING, collection_run_id STRING, snapshot_ts TIMESTAMP,
      product STRING, asset_type STRING, enabled BOOLEAN, collector STRING,
      cost_resolver STRING, required_tags ARRAY<STRING>, required_policies ARRAY<STRING>,
      config_hash STRING, raw_config STRING
    """,
    "governance_silver_asset_inventory_snapshot": """
      workspace_id STRING, collection_run_id STRING, snapshot_ts TIMESTAMP,
      product STRING, asset_type STRING, asset_id STRING, asset_name STRING,
      owner STRING, lifecycle_state STRING, tags MAP<STRING, STRING>,
      policies ARRAY<STRUCT<policy_type:STRING, policy_id:STRING, policy_name:STRING>>,
      discovery_source STRING, api_enriched BOOLEAN,
      tag_observation_complete BOOLEAN, policy_observation_complete BOOLEAN,
      raw_payload STRING
    """,
    "governance_silver_asset_tag_snapshot": """
      workspace_id STRING, collection_run_id STRING, snapshot_ts TIMESTAMP,
      asset_type STRING, asset_id STRING, tag_key STRING, tag_value STRING
    """,
}

rows_by_table = {
    "governance_silver_collection_run": run_rows,
    "governance_silver_requirement_snapshot": requirement_rows,
    "governance_silver_asset_inventory_snapshot": asset_rows,
    "governance_silver_asset_tag_snapshot": tag_rows,
}

for table, rows in rows_by_table.items():
    if rows:
        (spark.createDataFrame(rows, schema=schemas[table])
            .write.mode("append")
            .saveAsTable(f"{CATALOG_SCHEMA}.{table}"))
    print(f"Wrote {len(rows)} rows to {CATALOG_SCHEMA}.{table}")
