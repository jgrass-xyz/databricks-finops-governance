# Databricks notebook source
# MAGIC %md
# MAGIC # Governance inventory snapshot
# MAGIC
# MAGIC Loads the enabled asset adapters, snapshots the active requirements, and
# MAGIC records observed assets, tags, and policies at a daily grain.

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
from _policy_terms import flatten_policy_terms, merge_policy_definitions, parse_definition
from _system_discovery import SYSTEM_DISCOVERERS

_config_path = os.path.abspath(os.path.join(
    _notebook_dir, "..", "..", "config", "governance_assets.py"))
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
        "required_policies": [str(v).strip().upper() for v in config.get("required_policies", [])],
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
    try:
        enriched = COLLECTORS[collector_name](w, context)
    except Exception as error:
        enriched = []
        enrichment_error = f"API enrichment failed: {type(error).__name__}: {error}"
        print(enrichment_error)
    collected = merge_discovery_and_enrichment(discovered, enriched)
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

# Snapshot every current compute policy, not only attached policies. Other policy
# types currently expose IDs through their assets but not definitions in WorkspaceClient.
policy_rows_by_key = {}
term_rows = []
compute_policy_names = {}

try:
    for listed_policy in w.cluster_policies.list():
        policy_id = str(listed_policy.policy_id)
        policy = w.cluster_policies.get(policy_id).as_dict()
        if policy.get("policy_family_id"):
            family = w.policy_families.get(policy["policy_family_id"]).as_dict()
            definition = merge_policy_definitions(
                family.get("definition"), policy.get("policy_family_definition_overrides"))
            policy["resolved_policy_family"] = family
        else:
            definition = parse_definition(policy.get("definition"))
        compute_policy_names[policy_id] = policy.get("name")
        policy_rows_by_key[("COMPUTE_POLICY", policy_id)] = {
            "workspace_id": workspace_id,
            "collection_run_id": collection_run_id,
            "snapshot_ts": snapshot_ts,
            "policy_type": "COMPUTE_POLICY",
            "policy_id": policy_id,
            "policy_name": policy.get("name"),
            "description": policy.get("description"),
            "definition_json": json.dumps(definition, sort_keys=True),
            "policy_family_id": policy.get("policy_family_id"),
            "raw_payload": json.dumps(policy, sort_keys=True, default=str),
        }
        for term in flatten_policy_terms(definition):
            term_rows.append({
                "workspace_id": workspace_id,
                "collection_run_id": collection_run_id,
                "snapshot_ts": snapshot_ts,
                "policy_type": "COMPUTE_POLICY",
                "policy_id": policy_id,
                **term,
            })
except Exception as error:
    print(f"Policy definition enrichment failed: {type(error).__name__}: {error}")

# Add policy IDs observed on jobs/endpoints even when their definition API is not
# exposed by this workspace client. This preserves attachment history immediately.
for asset in asset_rows:
    for policy in asset["policies"]:
        if policy["policy_type"] == "COMPUTE_POLICY":
            policy["policy_name"] = compute_policy_names.get(policy["policy_id"])
        key = (policy["policy_type"], policy["policy_id"])
        policy_rows_by_key.setdefault(key, {
            "workspace_id": workspace_id,
            "collection_run_id": collection_run_id,
            "snapshot_ts": snapshot_ts,
            "policy_type": policy["policy_type"],
            "policy_id": policy["policy_id"],
            "policy_name": policy.get("policy_name"),
            "description": None,
            "definition_json": None,
            "policy_family_id": None,
            "raw_payload": None,
        })

policy_rows = list(policy_rows_by_key.values())

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

# One canonical snapshot per UTC date. Replacement happens only after all collectors
# and policy lookups have completed successfully.
for table in (
    "governance_silver_collection_run",
    "governance_silver_requirement_snapshot",
    "governance_silver_asset_inventory_snapshot",
    "governance_silver_asset_tag_snapshot",
    "governance_silver_policy_snapshot",
    "governance_silver_policy_term_snapshot",
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
    "governance_silver_policy_snapshot": """
      workspace_id STRING, collection_run_id STRING, snapshot_ts TIMESTAMP,
      policy_type STRING, policy_id STRING, policy_name STRING, description STRING,
      definition_json STRING, policy_family_id STRING, raw_payload STRING
    """,
    "governance_silver_policy_term_snapshot": """
      workspace_id STRING, collection_run_id STRING, snapshot_ts TIMESTAMP,
      policy_type STRING, policy_id STRING, definition_path STRING, rule_type STRING,
      rule_json STRING, hidden BOOLEAN, is_optional BOOLEAN
    """,
}

rows_by_table = {
    "governance_silver_collection_run": run_rows,
    "governance_silver_requirement_snapshot": requirement_rows,
    "governance_silver_asset_inventory_snapshot": asset_rows,
    "governance_silver_asset_tag_snapshot": tag_rows,
    "governance_silver_policy_snapshot": policy_rows,
    "governance_silver_policy_term_snapshot": term_rows,
}

for table, rows in rows_by_table.items():
    if rows:
        (spark.createDataFrame(rows, schema=schemas[table])
            .write.mode("append")
            .saveAsTable(f"{CATALOG_SCHEMA}.{table}"))
    print(f"Wrote {len(rows)} rows to {CATALOG_SCHEMA}.{table}")
