# Databricks notebook source
# MAGIC %md
# MAGIC # Destructive governance cleanup
# MAGIC
# MAGIC Manually drops every table and view managed by this governance module.
# MAGIC This notebook is intentionally not part of any scheduled job.

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "finops_observability")
dbutils.widgets.text("confirm", "")

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = dbutils.widgets.get("schema").strip()
CONFIRM = dbutils.widgets.get("confirm").strip()
EXPECTED_CONFIRMATION = f"DROP {CATALOG}.{SCHEMA}"

if CONFIRM != EXPECTED_CONFIRMATION:
    raise ValueError(
        f"Destructive cleanup refused. Set confirm exactly to: {EXPECTED_CONFIRMATION}"
    )

CATALOG_SCHEMA = f"{CATALOG}.{SCHEMA}"

# Views must be removed before the tables they reference.
VIEWS = [
    "visibility_asset_cost_daily",
    "visibility_assets_current",
    "governance_gold_observed_tags_current",
    "governance_gold_observed_tags_policies_current",
    "governance_gold_config_history",
    "governance_gold_cost_coverage_daily",
    "governance_gold_asset_daily",
    "governance_gold_asset_current",
    "governance_gold_asset_history",
    # Pre-standardization compatibility names, if an old deployment created them.
    "gold_observed_tags_policies_current",
    "gold_governance_config_history",
    "gold_asset_governance_daily",
    "gold_asset_governance_current",
    "gold_asset_governance_history",
]

TABLES = [
    "visibility_service_principals_current",
    "governance_silver_cost_refresh_state",
    "governance_silver_asset_tag_cost_daily",
    "governance_silver_asset_cost_daily",
    "governance_silver_policy_term_snapshot",
    "governance_silver_policy_snapshot",
    "governance_silver_asset_tag_snapshot",
    "governance_silver_asset_inventory_snapshot",
    "governance_silver_requirement_snapshot",
    "governance_silver_collection_run",
    # Pre-standardization table names, if an old deployment created them.
    "governance_cost_refresh_state",
    "asset_tag_cost_daily",
    "asset_cost_daily",
    "policy_term_snapshot",
    "policy_snapshot",
    "asset_tag_snapshot",
    "asset_inventory_snapshot",
    "governance_requirement_snapshot",
    "governance_collection_run",
]

for view in VIEWS:
    spark.sql(f"DROP VIEW IF EXISTS {CATALOG_SCHEMA}.{view}")
    print(f"Dropped view if present: {CATALOG_SCHEMA}.{view}")

for table in TABLES:
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG_SCHEMA}.{table}")
    print(f"Dropped table if present: {CATALOG_SCHEMA}.{table}")

print(f"Cleanup complete for {CATALOG_SCHEMA}; schema retained")
