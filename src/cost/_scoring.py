"""Pure scoring helper shared by src/cost/01_monitor.py and tests/test_with_fixtures.py.

Single source of truth for the signal-tiering math + record_id assignment.
No Spark, no Databricks SDK, no I/O — takes plain dicts/values, returns a dict.
"""

import math
import uuid


def _lookup_rates(node_type, photon, pricing):
    """Pricing-table lookup with photon-flag fallback.

    Mirrors the prod helper that lived inline in 01_monitor.py: try the exact
    (node_type, photon) pair first, then the opposite photon flag (pricing
    coverage gaps), then give up.
    """
    hit = pricing.get((node_type, photon))
    if hit:
        return hit
    hit = pricing.get((node_type, not photon))
    if hit:
        return hit
    return (None, None, None)


def score_cluster(
    cluster_facts,
    pricing,
    sku_by_photon,
    baseline,
    thresholds,
    exclusion_reason_fn,
    now,
    workspace_id,
):
    """Score one cluster against the workspace baseline + budget."""
    cluster_id = cluster_facts["cluster_id"]
    cluster_name = cluster_facts["cluster_name"]
    creator = cluster_facts.get("creator_user_name")
    cluster_source = cluster_facts.get("cluster_source")
    driver_type = cluster_facts["driver_node_type_id"]
    worker_type = cluster_facts["node_type_id"]
    workers = cluster_facts["num_workers"]
    photon = cluster_facts["photon"]

    sku = sku_by_photon.get(photon)
    budget = thresholds["budget"]
    z_warn = thresholds["z_warning"]
    z_crit = thresholds["z_critical"]

    baseline_mean = float(baseline.get("mean_daily_dollars") or 0.0)
    baseline_std = float(baseline.get("stddev_daily_dollars") or 0.0)
    baseline_mean_log = float(baseline.get("mean_log_daily_dollars") or 0.0)
    baseline_std_log = float(baseline.get("stddev_log_daily_dollars") or 0.0)

    record_id = str(uuid.uuid4())

    drv_dbu, drv_dph, _ = _lookup_rates(driver_type, photon, pricing)
    wkr_dbu, wkr_dph, _ = _lookup_rates(worker_type, photon, pricing)

    excl = exclusion_reason_fn(cluster_id, cluster_name, creator)
    if excl:
        # Excluded clusters skip scoring but still get a row written for auditability.
        dbu_per_hr = None
        dollars_per_hr = None
        projected = None
        z = None
        signal = "EXCLUDED"
        reason = excl
    elif drv_dbu is None or wkr_dbu is None:
        dbu_per_hr = None
        dollars_per_hr = None
        projected = None
        z = None
        signal = "UNKNOWN"
        reason = f"Missing pricing for driver={driver_type} worker={worker_type} photon={photon}"
    else:
        dbu_per_hr = drv_dbu + workers * wkr_dbu
        dollars_per_hr = drv_dph + workers * wkr_dph
        projected = dollars_per_hr * 24.0
        # Z-score in log space (workspace baseline distribution is heavily right-skewed).
        # projected stays in raw $ on the output row.
        z = (
            (math.log(projected) - baseline_mean_log) / baseline_std_log
            if (baseline_std_log > 0 and projected > 0)
            else None
        )

        reasons = []
        if projected > budget:
            reasons.append(f"projected_daily ${projected:,.0f} > budget ${budget:,.0f}")
        if z is not None and z > z_crit:
            reasons.append(f"log-z={z:.1f} > critical {z_crit}")
        elif z is not None and z > z_warn:
            reasons.append(f"log-z={z:.1f} > warning {z_warn}")

        if projected > budget or (z is not None and z > z_crit):
            signal = "CRITICAL"
        elif z is not None and z > z_warn:
            signal = "WARNING"
        else:
            signal = "OK"
        reason = "; ".join(reasons) if reasons else "within thresholds"

    return {
        "record_id": record_id,
        "workspace_id": workspace_id,
        "snapshot_ts": now,
        "cluster_id": cluster_id,
        "cluster_name": cluster_name,
        "creator": creator,
        "cluster_source": cluster_source,
        "node_type": worker_type,
        "driver_node_type": driver_type,
        "num_workers": workers,
        "photon": photon,
        "sku_name": sku,
        "dbu_per_hour": dbu_per_hr,
        "dollars_per_hour": dollars_per_hr,
        "projected_daily_dollars": projected,
        "baseline_mean": baseline_mean,
        "baseline_stddev": baseline_std,
        "z_score": z,
        "signal": signal,
        "reason": reason,
    }
