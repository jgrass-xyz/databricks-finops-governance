# databricks-finops-governance

Databricks FinOps and asset-governance toolkit. It combines system-table cost
attribution, tag coverage, historical asset inventory, billing
reconciliation, classic-compute anomaly detection, and optional Slack alerting.
The governance adapters currently cover classic clusters, jobs, Model
Serving endpoints, and SQL warehouses.

All deployment values are generic and every schedule is paused by default. See
[`SECURITY.md`](SECURITY.md) before adding workspace-specific configuration.

## What gets created

Under `${catalog}.${schema}` (defaults `main.finops_observability`):

| Object | What it is |
|---|---|
| `pricing_rates` | Snapshot of all-purpose DBU rates pulled from `databricks.com/en-pricing-assets/data/pricing/*.json`. Refreshed daily. |
| `workspace_baseline` | One row of population stats (mean / stddev / percentiles) on daily $/cluster over the last 30 days. |
| `cluster_scores` | Append-only. One row per (cluster_id, 15-min snapshot) with projected daily $, z-score, and `signal` ∈ {`OK`, `WARNING`, `CRITICAL`, `UNKNOWN`, `EXCLUDED`}. |
| `cluster_alerts_latest` (view) | Most recent row per cluster **where signal is WARNING or CRITICAL** AND snapshot is within the last 30 minutes. The query target the alert router reads. |
| `event_log` | Append-only audit of routing events across every detector silo. Cost router filters on `detector_name='cluster_cost'` for its per-(cluster, severity) suppression. |
| `backtest_results` | Output of the backtest notebook — one row per historical bad cluster-day with outcome & lead time. |
| `governance_silver_requirement_snapshot` | Requirements active for each collection run, including their configuration hash. |
| `governance_silver_asset_inventory_snapshot` / `governance_silver_asset_tag_snapshot` | Daily asset, owner, and tag observations. |
| `governance_silver_asset_cost_daily` / `governance_silver_asset_tag_cost_daily` | Persisted daily asset and tag cost rollups behind the governance views. |

## Jobs

| Job | Trigger | What it does |
|---|---|---|
| `baseline_refresh` | daily 2 AM | Rebuild `pricing_rates` + `workspace_baseline`; ensure `cluster_scores`, `event_log`, and `cluster_alerts_latest` exist. |
| `monitor` | every 15 min | List running all-purpose clusters, score each, append to `cluster_scores`. |
| `route_alerts` | every 30 min (PAUSED until validated) | Read `cluster_alerts_latest`, apply per-(cluster, severity) suppression, post Slack alerts. See [Slack alerting](#slack-alerting). |
| `backtest` | manual | Replay last 90d of `system.billing.usage` hour-by-hour, write per-cluster-day rows to `backtest_results` for ad-hoc query. |
| `teardown` | manual | Drops the schema and everything in it. Safety lock: `confirm` widget must be set to `YES` or it no-ops. Does not delete bundle jobs — use `databricks bundle destroy` for that. |
| `asset_governance_refresh` | daily 3 AM (PAUSED until validated) | Snapshots configured asset types and rebuilds the rolling cost-attribution window. |
| `governance_schema_smoke_test` | manual | Recreates setup in a disposable schema, asserts exact table/view column contracts, and cleans up even after failure. |

## Asset governance views

The governance module currently enables classic clusters, jobs, serving
endpoints, and SQL warehouses in `config/governance_assets.py`. Its Silver inventory, requirement, and cost snapshots are Delta tables; its Gold dashboard objects are
regular views:

System tables provide the authoritative asset inventory and billing spine. The
workspace APIs only enrich assets with live state and tag information
they are permitted to return. API visibility never determines whether an asset
or its cost appears in Gold.

| View | Purpose |
|---|---|
| `governance_gold_asset_current` | Latest owner, tags, missing requirements, status, and cost per active asset. |
| `governance_gold_asset_history` | Historical asset observations evaluated against the requirements from the same collection run. |
| `governance_gold_asset_daily` | Daily asset counts and dollars grouped by tag status. |
| `governance_gold_cost_coverage_daily` | Billed, inventory-matched, and billing-only assets/dollars with coverage percentages. |
| `governance_gold_config_history` | Requirement/configuration history across collection runs. |
| `governance_gold_observed_tags_current` | Current observed tag values with asset counts and trailing cost. |

### Compact asset visibility outputs

The final `visibility` task is a compact presentation layer. It
reuses the governance inventory and billing rollups rather than changing their
contracts:

| Output | Purpose |
|---|---|
| `visibility_service_principals_current` | Current workspace service principals (SDK only): principal/application IDs, display name, active state, direct owners, and explicit owner-resolution status/error. |
| `visibility_assets_current` | Current configured assets with `ARRAY<STRUCT<tag_name,tag_value>>` tags and a direct owner-to-service-principal match by principal ID, application ID, or display name. |
| `visibility_asset_cost_daily` | Historical daily configured-asset costs, selected billing tag, inventory status, and owner/service-principal identity. Billing-only and missing-tag rows are retained. |

Set `visibility_billing_tag_key` (default `application`) to choose the custom
billing tag projected into the daily output. Cost correction lookback remains 35
days by default; initial history follows `governance_cost_initial_backfill_days`.
The dev and production schedule remains `PAUSED`.

Direct-owner lookup uses `AccountClient.access_control` to read direct
`roles/servicePrincipal.manager` grants; groups are retained without expansion.
Set `visibility_account_id` when it is not available from account credentials.
SDK version, account credentials, and cloud support vary, so absence or permission
failure does not fail principal collection. Manager lookup is requested only for
service principals that currently own a configured asset, avoiding one account API
call for every unrelated workspace principal. `owner_resolution_status` is
`NOT_REQUESTED`, `UNAVAILABLE`, `ERROR`, or `RESOLVED`, with details in
`owner_resolution_error`. Empty `direct_owners` must therefore not be interpreted
as proof that no owner exists.

Empty `required_tags` arrays put tag enforcement in discovery mode (`NOT_CONFIGURED`)
without hiding observed values. Metadata that cannot be observed without API enrichment
is `UNKNOWN`, not incorrectly `NOT_APPLIED`.
Recently billed assets missing from current inventory remain in the current view as
`BILLING_ONLY` for 30 days with their full cost. Deleted assets remain in history.
Tag and requirement changes follow the same snapshot behavior.

`governance_gold_asset_current` exposes `current_day_dollars` (partial),
`previous_day_dollars` (completed), and completed trailing 7/30/90-day windows.
`actual_daily_dollars` remains as a compatibility alias for `previous_day_dollars`.

### Destructive governance reset

`src/governance/99_cleanup.py` is a manual destructive cleanup notebook and is
not part of any scheduled production workflow. Run it before the refresh when accepting a full rebuild after
a breaking schema change. Set `catalog`, `schema`, and the exact confirmation value
`DROP <catalog>.<schema>`; it drops governance/visibility views first and then their
tables while retaining the schema. The next `asset_governance_refresh` recreates the
current contract and rebuilds cost history according to the configured backfill.

The manual `governance_schema_smoke_test` job automates a safe disposable test:
`cleanup_before → setup → inventory_write → assert_schema → cleanup_after`. It runs
the real inventory writer so Delta `NOT NULL`, generated-column, and write-schema
incompatibilities fail the test instead of escaping schema-only validation. The
assertion notebook then checks exact ordered columns for every setup-created Silver
table and Gold view, plus non-empty inventory output. `cleanup_after` uses `ALL_DONE`,
so disposable objects are removed even when setup, writing, or assertions fail.
Override `governance_schema_test_schema` if needed; never point it at a schema
containing real data.

## Deploying to a new workspace

1. **Install + authenticate the Databricks CLI** against the target workspace. Either:
   ```bash
   databricks auth login --host https://<workspace>.cloud.databricks.com
   ```
   or set up a profile in `~/.databrickscfg`.

2. **Review `databricks.yml` and edit the variable defaults** to fit the customer:
   - `catalog` + `schema` — where tables land
   - `budget_daily_dollars` — absolute $/day per cluster alert threshold (default `5000`)
   - `z_warning` / `z_critical` — statistical thresholds (default `2.0` / `3.0`)
   - `baseline_lookback_days` — how much history feeds the workspace baseline (default `30`)
   - `notifications_email` — job-failure emails (default empty — disabled)
   - `slack_secret_scope` / `slack_secret_key` — where the Slack bot token lives (default `databricks-cost-alerts` / `slack-bot-token`)
   - `slack_routing_mode` — `channel_only` (default during testing) or `dm_with_fallback`
   - `slack_alert_channel` — channel name (`#cost-alerts`) or ID (`C0...`); **must be set** before `route_alerts` runs
   - `warning_suppression_hours` / `critical_suppression_hours` — re-nudge cadence per tier (default `24` / `4`)

3. **Review `resources/jobs.yml` → `job_clusters` block** (anchored on the first job). Adjust `node_type_id`, `driver_node_type_id`, and `aws_attributes` / `azure_attributes` / `gcp_attributes` to match the target cloud. The default mirrors a single-node AWS `r5d.2xlarge` Photon cluster.

4. **Deploy**:
   ```bash
   cd databricks-finops-governance
   DATABRICKS_TF_EXEC_PATH=$(which terraform) \
   DATABRICKS_TF_VERSION=$(terraform --version | head -1 | awk '{print $2}' | tr -d v) \
   DATABRICKS_TF_CLI_CONFIG_FILE=/dev/null \
     databricks bundle deploy -t dev --profile=<your-profile>
   ```
   (TF env vars work around an expired PGP key in the embedded TF — omit if not on a machine that hits it.)

5. **Run the jobs manually in order** from the workspace Jobs UI:
   1. **`baseline_refresh`** — creates pricing + baseline + scores + event_log + view.
   2. **`monitor`** — scores currently-running clusters against the baseline.
   3. **`backtest`** (may take a few minutes) — replays history, writes outcomes to `backtest_results`.
   4. **`route_alerts`** — sends Slack messages for flagged clusters. Requires Slack token + scopes set up first ([Slack alerting](#slack-alerting)).

   Every schedule ships `PAUSED`. Validate each workflow, then explicitly enable
   only the schedules you intend to operate in `resources/*.yml` and redeploy.

### Block-specific development deployment

A deployment directly from a Databricks Git folder uses the generic bundle defaults,
including the `main` catalog. For Block development, use the adjacent local overlay
instead. It copies the Git-ignored Block configuration into the repository and passes
Block's deployment variables to the bundle:

```bash
cd "/Users/jgrass/goose artifacts"

git -C databricks-finops-governance switch feat/asset-owner-visibility
git -C databricks-finops-governance pull

./block-overlay/deploy.sh dev
```

The default development destination is:

```text
justin_grass.finops_observability_dev
```

The compact visibility outputs are:

```text
justin_grass.finops_observability_dev.visibility_service_principals_current
justin_grass.finops_observability_dev.visibility_assets_current
justin_grass.finops_observability_dev.visibility_asset_cost_daily
```

Override the destination or CLI profile without changing tracked bundle defaults:

```bash
CATALOG=another_catalog \
SCHEMA=another_schema \
DATABRICKS_CONFIG_PROFILE=another-profile \
./block-overlay/deploy.sh dev
```

The Block overlay is intentionally stored outside this generic repository. Deploying
only the Git folder does not include `block-overlay/governance_assets_local.py` and,
unless `catalog` is overridden separately, writes to the generic `main` catalog.

## Config (DAB variables)

| Variable | Default | Notes |
|---|---|---|
| `catalog` | `main` | UC catalog to write to. |
| `schema` | `finops_observability` | UC schema (auto-created). |
| `budget_daily_dollars` | `5000` | Absolute backstop. Any cluster projected above this flips to CRITICAL. |
| `z_warning` | `2.0` | Z-score threshold for WARNING tier. |
| `z_critical` | `3.0` | Z-score threshold for CRITICAL tier. |
| `baseline_lookback_days` | `30` | History window used for the baseline distribution. |
| `governance_cost_lookback_days` | `35` | Rolling usage window rebuilt to absorb corrections and late billing records. |
| `governance_cost_initial_backfill_days` | `365` | History loaded once when an asset type is first enabled. Reduce this before first deployment if desired. |
| `visibility_billing_tag_key` | `application` | Selected custom billing tag in `visibility_asset_cost_daily`. |
| `visibility_account_id` | empty | Optional account ID for resolving direct service-principal managers. |
| `governance_schema_test_schema` | `finops_governance_schema_smoke_test` | Disposable schema for the manual setup/schema contract test. |
| `slack_secret_scope` | `databricks-cost-alerts` | Databricks secret scope holding the Slack bot token. |
| `slack_secret_key` | `slack-bot-token` | Key inside the scope holding the `xoxb-...` token. |
| `slack_routing_mode` | `channel_only` | `channel_only` posts every alert to `slack_alert_channel`. `dm_with_fallback` DMs cluster owners and falls back to the channel for unmatched emails. |
| `slack_alert_channel` | *(empty — must set)* | Channel name (`#name`) or ID (`C0...`). Required. Bot must be a member unless it has `chat:write.public`. |
| `warning_suppression_hours` | `24` | Hours to suppress repeat WARNING messages for the same cluster. |
| `critical_suppression_hours` | `4` | Hours to suppress repeat CRITICAL messages for the same cluster. |
| `slack_workspace_host` | *(empty — auto-detect)* | Workspace host (e.g. `example-workspace.cloud.databricks.com`) used to build cluster deep-link URLs. Leave empty to auto-detect; set explicitly for PrivateLink / custom domains or when auto-detection is wrong. |

Override per deploy, e.g. `databricks bundle deploy -t dev --var="budget_daily_dollars=10000"`.

## Signal logic

At each monitor run, per live cluster:

```
dbu_per_hr = driver_dbu + num_workers * worker_dbu    # from pricing_rates
dollars_per_hr = driver_$/hr + num_workers * worker_$/hr
projected_daily_dollars = dollars_per_hr * 24
z_score = (projected_daily_dollars - baseline_mean) / baseline_stddev

signal = CRITICAL  if projected_daily_dollars > budget_daily_dollars
                   or z_score > z_critical
signal = WARNING   elif z_score > z_warning
signal = OK        else
```

Two independent signals — statistical catches "big for this workspace," budget catches "big in absolute dollars regardless of workspace norms."

## Testing-phase notes

- **Job-failure email notifications are commented out in `resources/jobs.yml`** — no emails will go out during testing. Uncomment the `email_notifications` blocks before prod.
- **Every schedule is `PAUSED` by default.** Validate configuration, permissions,
  destinations, and cost before enabling any recurring trigger.
- **The Slack bot token is read from a Databricks secret scope.** No token is provisioned by the bundle — see [Slack alerting](#slack-alerting) for setup.

## Slack alerting

`03_slack_alerting` reads `cluster_alerts_latest` every 30 minutes, applies per-(cluster, severity) suppression so the same cluster doesn't trigger a message every run, and posts via Slack. Two routing modes:

| Mode | Behavior | When to use |
|---|---|---|
| `channel_only` *(default)* | Every alert posts to `slack_alert_channel`. Owner email is shown in the message body. | Initial rollout — safe, doesn't surprise end users. Validate suppression + format before flipping the switch. |
| `dm_with_fallback` | DM each cluster owner via `users.lookupByEmail`. Owners with no Slack match (or service-principal-owned clusters) fall back to `slack_alert_channel`. | After the alert quality is validated — owners notice DMs more than channel mentions. |

Flip with `slack_routing_mode` in `databricks.yml` (or per-deploy `--var=slack_routing_mode=dm_with_fallback`).

### Suppression model

Each `(cluster_id, signal)` pair has its own suppression window:

| Severity | Default window | Why |
|---|---|---|
| WARNING | 24h | One nudge per day per cluster — owner has time to address; not catastrophic. |
| CRITICAL | 4h | Persistent pressure until the cluster is killed/right-sized. $5k+/day deserves it. |

Tunable via `warning_suppression_hours` / `critical_suppression_hours`. A cluster that escalates from WARNING to CRITICAL still fires a CRITICAL message immediately — windows are independent per tier.

### Provisioning the Slack bot

1. **Create or reuse a Slack app** in the customer's Slack workspace.
2. **Add Bot Token Scopes**:
   - `chat:write` — send messages
   - `users:read`, `users:read.email` — translate cluster owner emails to Slack user IDs (needed for `dm_with_fallback`)
   - `channels:read`, `groups:read`, `im:read`, `mpim:read` — resolve channel names to IDs
   - *(optional)* `chat:write.public` — post to public channels without `/invite`
3. **Install (or reinstall) the app to the workspace** so a token is issued covering the full scope set. Adding scopes to an already-installed app does not retroactively update existing tokens.
4. **Copy the Bot User OAuth Token** (`xoxb-...`) from the OAuth & Permissions page.
5. **Store the token in a Databricks secret scope:**
   ```bash
   databricks secrets create-scope databricks-cost-alerts
   databricks secrets put-secret databricks-cost-alerts slack-bot-token
   # paste xoxb-... into the editor
   ```
6. **Grant the job's run-as principal `READ` on the scope:**
   ```bash
   databricks secrets put-acl databricks-cost-alerts <run-as-sp-application-id> READ
   ```
7. **Invite the bot to `slack_alert_channel`** (unless the bot has `chat:write.public`):
   ```
   /invite @your-bot-name
   ```
8. **Trigger `route_alerts` manually once** to verify end-to-end before unpausing the schedule.

### Operational notes

- **Heartbeat alert**: `cluster_alerts_latest` is empty if `01_monitor` is broken or hasn't run in 30 minutes — which would silently mute alerts. Pair with a separate DBSQL Alert on `(SELECT max(snapshot_ts) FROM cluster_scores) < current_timestamp() - INTERVAL 30 MINUTES`.
- **Audit trail**: every message is logged in `event_log` with `detector_name`, `asset_id`, `severity`, `recipient`, `routing` (`channel_only` / `dm` / `fallback_channel`), and Slack `message_ts`. Filter `WHERE detector_name='cluster_cost'` to answer cost-only questions like "who got pinged about cluster X" or "how often did this fire."
- **Owner ↔ Slack matching** *(only relevant in `dm_with_fallback`)*: the cluster's `creator_user_name` from the Clusters API is used as-is to call `users.lookupByEmail`. Matching only works if the owner's Databricks email matches their Slack email. Service-principal-owned clusters never match and route to the fallback channel.

## Source layout

```
databricks-finops-governance/
├── databricks.yml
├── resources/
│   ├── jobs.yml
│   └── governance_jobs.yml
├── config/
│   ├── fleet_aliases.py         # AWS Fleet type → canonical EC2 family mapping (see below)
│   ├── excluded_clusters.example.py # Safe template; copy locally before customization
│   ├── pricing_AWS.json         # Pre-staged pricing snapshot (see "Air-gapped workspaces" below)
│   ├── pricing_Azure.json
│   └── pricing_GCP.json
├── src/
│   ├── cost/                    # real-time detector, routing, backtest, and teardown
│   └── governance/              # daily inventory, tags, and cost-attribution module
├── tests/
│   ├── cost/
│   └── governance/
└── README.md
```

## Air-gapped customer workspaces (no `databricks.com` outbound)

If the target workspace's subnet blocks outbound `databricks.com`, `00_baseline_refresh` can't fetch the pricing JSON at runtime. To handle that, the bundle ships pricing snapshots in `config/pricing_<CLOUD>.json` and `00` reads them directly — no network call needed.

To **refresh** those snapshots when Databricks updates list pricing:

1. Run `_fetch_pricing` (utility notebook) in a workspace that *does* have internet (typically your own FE workspace, not the customer's). It writes `pricing_<CLOUD>.json` files into the bundle's `config/` directory.
2. From your laptop, pull the updated config dir down:
   ```bash
   databricks workspace export-dir \
     /Workspace/Users/<you>/<bundle-deploy-path>/files/config \
     ./config --overwrite --profile=<your-profile>
   ```
3. `git add config/pricing_*.json && git commit -m "refresh pricing snapshots" && git push`
4. Customer pulls the repo, redeploys the bundle, runs `00_baseline_refresh`. It loads the pricing from the local file with no outbound call.

If `config/pricing_<CLOUD>.json` is absent, `00` falls back to fetching from `databricks.com` — so workspaces *with* internet access don't need the snapshots committed.

## `config/excluded_clusters.py`

Customers often have known-large clusters that are valid by design (production ETL, shared BI warehouses, training jobs). Without filtering, these clusters do two unwanted things:

- **Skew the baseline**: their daily $ inflate the workspace mean and standard deviation, making everything else look "normal" and dropping detection sensitivity.
- **Generate noise alerts**: every monitor run flags them as outliers even though they're expected.

Copy `config/excluded_clusters.example.py` to the Git-ignored
`config/excluded_clusters.py`, then edit the local file. All three pipeline
notebooks prefer the local file and fall back to the empty example template.
Any single match excludes a cluster:

```python
CLUSTER_IDS = [
    "0123-456789-abcdefgh",   # exact match, most precise
]

CLUSTER_NAME_PATTERNS = [
    "prod-etl-*",             # glob against cluster_name
    "shared-bi-*",
]

CREATORS = [
    "service-prod@example.com",   # exact match on cluster owner email
]
```

What happens at each layer:

- **`00_baseline_refresh`** — excluded cluster-days drop out of the workspace baseline, so their costs don't pull the mean/stddev up.
- **`01_monitor`** — excluded clusters get a row in `cluster_scores` with `signal='EXCLUDED'` and the matching rule named in `reason`. They never become alerts but stay auditable: `SELECT * FROM cluster_scores WHERE signal = 'EXCLUDED'` answers "what's being filtered and why."
- **`02_backtest`** — same exclusions apply, so the catch-rate numbers reflect the rules you'd actually run with in production.

## Z-score normalization

Daily $/cluster is heavily right-skewed: most clusters cost $20-$200/day, a handful cost thousands. Z-scores on raw dollars get dominated by the long tail and underweight smaller-but-still-anomalous clusters.

The bundle computes z-scores in **log space** (`LN(daily_dollars)`) instead. Concretely:

- `workspace_baseline` carries both raw-dollar stats (`mean_daily_dollars`, `stddev_daily_dollars`, percentiles) for human reading **and** log-space stats (`mean_log_daily_dollars`, `stddev_log_daily_dollars`).
- `01_monitor` and `02_backtest` use the log-space stats to compute `z = (LN(projected_daily_dollars) - mean_log) / stddev_log`.
- All dollar values written to tables (`projected_daily_dollars`, `baseline_mean`, `baseline_stddev`, `actual_daily_dollars`) remain in raw dollars — only the z-score's interpretation changes.

This makes `z_warning=2.0` and `z_critical=3.0` correspond to multiplicative outliers ("~7× and ~20× the workspace mean") rather than absolute-dollar outliers, which is generally what you want for a long-tail distribution.

### Weekday-only baseline

The baseline (and the rolling baseline used by the backtest) is built from **Monday–Friday cluster-days only**. Weekend usage typically drops sharply, which would widen the distribution and dampen weekday z-scores. The monitor scores weekend clusters against the weekday baseline anyway — slightly aggressive on weekends, which is generally desirable since unusual weekend activity is worth flagging. The simulation in `02_backtest` still walks every hour of every day; only the rolling baseline used to score each simulated day is weekday-only.

## `config/fleet_aliases.py`

AWS Fleet instance types (`rd-fleet.xlarge`, `m-fleet.2xlarge`, etc.) are Databricks-side abstractions that map to a pool of underlying EC2 families. The public pricing JSON only lists concrete instance types, so a fleet name never matches on its own — `00_baseline_refresh` synthesizes alias rows using this dict.

Plain Python module loaded via `runpy`. Format is `{ "<fleet-prefix>": "<canonical-base-family>" }`:

```python
FLEET_BASE_MAP = {
    "rd-fleet": "r5d",   # memory-optimized with local NVMe SSD
    "m-fleet":  "m5",    # general-purpose
}
```

For every entry, the baseline job takes each pricing row whose instance starts with `<base>.` (e.g. `r5d.4xlarge`) and produces a synthetic row for `<fleet>.<size>` (e.g. `rd-fleet.4xlarge`) carrying the same DBU/hr rate.

**Adding a new fleet family** (e.g. when AWS ships one):
1. Figure out the canonical base family Databricks prices it against. Start from the pricing JSON — look at `dburate` for the concrete types and match specs.
2. Add one entry to `FLEET_BASE_MAP` with a brief comment on the workload profile.
3. Redeploy (`databricks bundle deploy`) and re-run `00_baseline_refresh`.

The file's own docstring carries the same explanation for anyone who opens it directly.

## Design choices

- **Synthetic DBU rate from pricing JSON, customer $/DBU from `system.billing.account_prices`.** The JSON is authoritative per-node for DBU/hr; `account_prices` captures the customer's actual contracted $/DBU (including negotiated discounts). `baseline_refresh` overrides `dollars_per_dbu` in `pricing_rates` with the rate from SKUs this workspace actually bills against in the last 30d, falling back to plan-name match for brand-new workspaces. The public rate is kept in `public_dollars_per_dbu` for reference.
- **Workspace-wide baseline**, not per-cluster. The customer's problem is people spinning up *new* huge clusters, which by definition have no per-cluster history. A workspace-wide distribution catches absolute outliers on first snapshot.
- **Projection = current $/hr × 24**. Simple, conservative, and consistent with what the backtest replays. If a cluster ramps slow and then explodes, the projection will rise over successive snapshots and the signal will escalate.
- **`system.billing.usage` is only used for historical baseline + backtest** — never for real-time scoring. Its multi-hour latency is the whole reason this system exists.

## Limitations

- **Azure workspaces are not supported today.** The public Databricks pricing JSON (`Azure.json`) contains only serverless / FM-API / SAP rows — no classic all-purpose DBU rates. Deploying against an Azure workspace would leave `pricing_rates` empty and flip every cluster to `UNKNOWN`. Azure all-purpose pricing is published outside this CDN (Azure Marketplace listing). If we need Azure later, options are: hand-maintained `config/azure_rates.json` fallback, deriving DBU/hr from `system.billing.usage` over a long window, or scraping the Azure pricing page.
- **GCP `e2_*` underscore naming mismatch (minor).** A handful of GCP pricing rows use underscores (`e2_standard_4`) where the Clusters API returns hyphens (`e2-standard-4`). Same class of bug as the Photon suffix — lookup would miss for the ~14 affected instance types. Not fixed preemptively since no GCP workspace is in scope yet; trivial to normalize in `00_baseline_refresh` if it ever bites.

## Out of scope

- Jobs / DLT / serverless compute.
- Per-cluster historical baselines.
- Auto-terminate / kill-switch actions — system warns only.
- Multi-workspace fan-out is *structured for* (tables carry `workspace_id`, config can be list-ified) but not wired.
