"""Shared management of the cross-silo ``event_log`` table.

Single source of truth for the schema, constraints, and allowed value sets
of the unified event log. Every detector silo's setup notebook calls
``ensure(spark, fq_table_name)`` so the table exists with the same shape
and constraints regardless of which silo a customer deployed first.

Idempotent: safe to call from every silo's setup, every run. Re-runs do not
fail on "constraint already exists" or "column already NOT NULL".
"""

# Allowed values exposed as constants
EVENT_TYPES = (
    "message_sent",         # we DM'd or channel-posted about this asset
    "message_suppressed",   # we would have sent, but suppression window blocked it (not written today; reserved for future use)
    "message_failed",       # the Slack send raised; row preserves the attempt (reserved)
)

# Columns guaranteed populated on every row regardless of event_type.
_REQUIRED_COLUMNS = (
  "event_ts", "event_type", "detector_name", "workspace_id",
  "record_id", "asset_type", "asset_id", "asset_name",
)

_EVENT_TYPE_REQUIREMENTS = {
  # Columns required when event_type='message_sent'
  "message_sent": (
    "severity", "recipient", "message_ts", "routing",
    "plain", "blocks", "details_json",
  ),
  # Columns required when event_type='message_failed'
  "message_failed": (
    "severity", "recipient", "routing", "plain",
    "blocks", "details_json",
  )
}


def ensure(spark, fq_table_name: str) -> None:
    """Create the ``event_log`` table if missing and bring its constraints up to spec.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session from the calling notebook.
    fq_table_name : str
        Fully-qualified table name, e.g. ``"main.finops_observability.event_log"``.
    """
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {fq_table_name} (
      event_ts      TIMESTAMP NOT NULL,
      event_type    STRING    NOT NULL,
      detector_name STRING    NOT NULL,
      workspace_id  BIGINT    NOT NULL,
      record_id     STRING    NOT NULL,
      asset_type    STRING    NOT NULL,
      asset_id      STRING    NOT NULL,
      asset_name    STRING    NOT NULL,
      severity      STRING,
      recipient     STRING,
      routing       STRING,
      message_ts    STRING,
      plain         STRING,
      blocks        STRING,
      details_json  STRING
    )
    USING DELTA
    """)

    # SET NOT NULL to surface bad data rather than silently mask it
    for col in _REQUIRED_COLUMNS:
        spark.sql(f"ALTER TABLE {fq_table_name} ALTER COLUMN {col} SET NOT NULL")

    # Look up constraints so we can add the missing ones (ADD CONSTRAINT isn't idempotent)
    existing_constraints = {
        row["key"] for row in spark.sql(
            f"SHOW TBLPROPERTIES {fq_table_name}"
        ).collect()
    }

    # Add missing constraints
    if "delta.constraints.event_type_recognized" not in existing_constraints:
        allowed_list = ", ".join(f"'{t}'" for t in EVENT_TYPES)
        spark.sql(f"""
            ALTER TABLE {fq_table_name} ADD CONSTRAINT event_type_recognized
            CHECK (event_type IN ({allowed_list}))
        """)
        print(f"Added CHECK constraint event_type_recognized on {fq_table_name}")

    for event_type, required_cols in _EVENT_TYPE_REQUIREMENTS.items():
        constraint_name = f"{event_type}_complete"
        if f"delta.constraints.{constraint_name}" not in existing_constraints:
            clauses = " AND ".join(f"{col} IS NOT NULL" for col in required_cols)
            spark.sql(f"""
                ALTER TABLE {fq_table_name} ADD CONSTRAINT {constraint_name}
                CHECK (event_type != '{event_type}' OR ({clauses}))
            """)
            print(f"Added CHECK constraint {constraint_name} on {fq_table_name}")
