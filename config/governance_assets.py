"""Asset types enabled for governance inventory and cost attribution.

Requirements are intentionally configuration, not code. Every inventory run
snapshots this file into Delta so historical governance results retain the rules
that were active on that date.
"""

ASSET_TYPES = {
    "cluster": {
        "enabled": True,
        "product": "COMPUTE",
        "collector": "clusters",
        "cost_resolver": "cluster",
        "required_tags": [
            "cost_center", "business_unit", "product", "environment", "owner",
        ],
    },
    "job": {
        "enabled": True,
        "product": "WORKFLOWS",
        "collector": "jobs",
        "cost_resolver": "job",
        "required_tags": ["cost_center", "product", "owner"],
    },
    "serving_endpoint": {
        "enabled": True,
        "product": "MODEL_SERVING",
        "collector": "serving_endpoints",
        "cost_resolver": "serving_endpoint",
        "required_tags": ["cost_center", "product", "owner"],
    },
    "warehouse": {
        "enabled": True,
        "product": "SQL",
        "collector": "warehouses",
        "cost_resolver": "warehouse",
        "required_tags": ["cost_center", "product", "owner"],
    },
}
