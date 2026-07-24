# Databricks notebook source
# MAGIC %md
# MAGIC # Migration: remove legacy policy columns
# MAGIC
# MAGIC Removes columns retired from the governance inventory contract. This is an
# MAGIC explicit, idempotent migration for schemas created before policy collection
# MAGIC was removed; normal setup intentionally does not mutate existing tables.

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "finops_observability")

CATALOG_SCHEMA = f"{dbutils.widgets.get('catalog')}.{dbutils.widgets.get('schema')}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_SCHEMA}")

LEGACY_COLUMNS = {
    "governance_silver_requirement_snapshot": ["required_policies"],
    "governance_silver_asset_inventory_snapshot": [
        "policies",
        "policy_observation_complete",
    ],
}


def table_exists(qualified_name):
    try:
        spark.table(qualified_name)
        return True
    except Exception as error:
        # Avoid swallowing permission and connectivity failures as "not found".
        if "TABLE_OR_VIEW_NOT_FOUND" in str(error):
            return False
        raise


for table, legacy_columns in LEGACY_COLUMNS.items():
    qualified_name = f"{CATALOG_SCHEMA}.{table}"
    if not table_exists(qualified_name):
        print(f"Skipped absent table: {qualified_name}")
        continue

    existing_columns = set(spark.table(qualified_name).columns)
    columns_to_drop = [column for column in legacy_columns if column in existing_columns]
    if not columns_to_drop:
        print(f"Already migrated: {qualified_name}")
        continue

    # Delta metadata-only column drops require name-based column mapping. Setting
    # this property is idempotent and preserves existing rows, table identity, and
    # grants while allowing the retired NOT NULL columns to be removed.
    spark.sql(f"""
      ALTER TABLE {qualified_name}
      SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
    """)
    for column in columns_to_drop:
        spark.sql(f"ALTER TABLE {qualified_name} DROP COLUMN `{column}`")
        print(f"Dropped legacy column: {qualified_name}.{column}")

print(f"Legacy governance policy-column migration complete in {CATALOG_SCHEMA}")
