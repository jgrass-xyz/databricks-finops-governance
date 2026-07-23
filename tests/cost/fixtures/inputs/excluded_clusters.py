"""Test variant of config/excluded_clusters.py.

Same format as the prod config — the harness loads it via runpy and feeds the
lists to the exclusion_reason callable that _scoring.score_cluster invokes.

Each list exercises one path through exclusion_reason() in src/cost/_scoring.py:
- CLUSTER_IDS         → exact cluster_id match (path 1)
- CLUSTER_NAME_PATTERNS → fnmatch against cluster_name (path 2)
- CREATORS            → exact creator_user_name match (path 3)

Each path has at least one matching cluster in inputs/clusters_live.csv so a
regression in any path surfaces as an EXCLUDED-vs-expected mismatch.
"""

CLUSTER_IDS = [
    "c-excluded-by-id-013",      # exact cluster_id match
]

CLUSTER_NAME_PATTERNS = [
    "prod-etl-*",                # matches c-excluded-006 ("prod-etl-shared")
]

CREATORS = [
    "service-bot@example.com",   # matches c-excluded-by-creator-014
]
