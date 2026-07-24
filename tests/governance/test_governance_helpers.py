import json
import os
import runpy
import sys
import unittest
from datetime import datetime, timezone


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
GOVERNANCE_SRC = os.path.join(ROOT, "src", "governance")
sys.path.insert(0, GOVERNANCE_SRC)

from _collectors import COST_RESOLVERS, collect_clusters, collect_jobs, collect_serving_endpoints, collect_warehouses
from _normalize import evaluate_requirements, merge_discovery_and_enrichment
from _policy_terms import flatten_policy_terms, merge_policy_definitions
from _system_discovery import _cluster_sql, _job_sql, _serving_endpoint_sql, _warehouse_sql
from _visibility import collect_service_principals, match_owner_to_principal, resolve_direct_owners


class Object:
    def __init__(self, value):
        self.value = value
        self.job_id = value.get("job_id")

    def as_dict(self):
        return self.value


class API:
    def __init__(self, values):
        self.values = values
        self.list_kwargs = None

    def list(self, **kwargs):
        self.list_kwargs = kwargs
        return iter(Object(value) for value in self.values)


class JobsAPI(API):
    def get(self, job_id):
        return Object(next(value for value in self.values if value["job_id"] == job_id))


class Client:
    def __init__(self, clusters=None, jobs=None, endpoints=None, warehouses=None):
        self.clusters = API(clusters or [])
        self.jobs = JobsAPI(jobs or [])
        self.serving_endpoints = API(endpoints or [])
        self.warehouses = API(warehouses or [])


CONTEXT = {
    "workspace_id": "123",
    "run_id": "run-1",
    "snapshot_ts": datetime(2026, 1, 1, tzinfo=timezone.utc),
}


class GovernanceHelperTests(unittest.TestCase):
    def test_discovery_mode_keeps_observations_without_enforcement(self):
        result = evaluate_requirements(
            {"team": "analytics"},
            [{"policy_type": "COMPUTE_POLICY", "policy_id": "p1"}],
            [],
            [],
        )
        self.assertEqual("NOT_CONFIGURED", result["tag_status"])
        self.assertEqual("NOT_CONFIGURED", result["policy_status"])
        self.assertEqual([], result["missing_required_tags"])
        self.assertEqual([], result["missing_required_policies"])

    def test_changed_requirements_change_only_the_new_evaluation(self):
        observed_tags = {"cost_center": "42"}
        old = evaluate_requirements(observed_tags, [], ["cost_center"], [])
        new = evaluate_requirements(observed_tags, [], ["cost_center", "owner"], [])
        self.assertEqual("APPLIED", old["tag_status"])
        self.assertEqual("PARTIAL", new["tag_status"])
        self.assertEqual(["owner"], new["missing_required_tags"])

    def test_removed_policy_is_missing_in_next_snapshot(self):
        applied = evaluate_requirements(
            {}, [{"policy_type": "BUDGET_POLICY", "policy_id": "b1"}],
            [], ["BUDGET_POLICY"])
        removed = evaluate_requirements({}, [], [], ["BUDGET_POLICY"])
        self.assertEqual("APPLIED", applied["policy_status"])
        self.assertEqual("NOT_APPLIED", removed["policy_status"])
        self.assertEqual(["BUDGET_POLICY"], removed["missing_required_policies"])

    def test_incomplete_api_enrichment_is_unknown_not_unapplied(self):
        result = evaluate_requirements(
            {}, [], ["owner"], ["BUDGET_POLICY"],
            tag_observation_complete=False,
            policy_observation_complete=False,
        )
        self.assertEqual("UNKNOWN", result["tag_status"])
        self.assertEqual("UNKNOWN", result["policy_status"])
        self.assertIsNone(result["missing_required_tags"])
        self.assertIsNone(result["missing_required_policies"])

    def test_cluster_collector(self):
        rows = collect_clusters(Client(clusters=[{
            "cluster_id": "c1", "cluster_name": "analytics",
            "creator_user_name": "owner@example.com", "cluster_source": "UI",
            "state": "RUNNING", "policy_id": "p1",
            "custom_tags": {"cost_center": "42"},
        }]), {**CONTEXT, "product": "COMPUTE", "asset_type": "cluster"})
        self.assertEqual(1, len(rows))
        self.assertEqual("c1", rows[0]["asset_id"])
        self.assertEqual("COMPUTE_POLICY", rows[0]["policies"][0]["policy_type"])

    def test_job_collector_collects_multiple_policy_types(self):
        rows = collect_jobs(Client(jobs=[{
            "job_id": 7,
            "creator_user_name": "creator@example.com",
            "run_as_user_name": "principal@example.com",
            "effective_budget_policy_id": "budget-1",
            "settings": {
                "name": "daily-load",
                "tags": {"product": "billing"},
                "job_clusters": [{"new_cluster": {"policy_id": "compute-1"}}],
            },
        }]), {**CONTEXT, "product": "WORKFLOWS", "asset_type": "job"})
        self.assertEqual("principal@example.com", rows[0]["owner"])
        self.assertEqual(
            ["BUDGET_POLICY", "COMPUTE_POLICY"],
            [policy["policy_type"] for policy in rows[0]["policies"]],
        )

    def test_serving_endpoint_collector(self):
        rows = collect_serving_endpoints(Client(endpoints=[{
            "id": "internal-id",
            "name": "fraud-model",
            "creator": "owner@example.com",
            "budget_policy_id": "budget-2",
            "tags": [{"key": "product", "value": "fraud"}],
            "state": {"ready": "READY"},
        }]), {**CONTEXT, "product": "MODEL_SERVING", "asset_type": "serving_endpoint"})
        self.assertEqual("fraud-model", rows[0]["asset_id"])
        self.assertEqual({"product": "fraud"}, rows[0]["tags"])
        self.assertEqual("BUDGET_POLICY", rows[0]["policies"][0]["policy_type"])

    def test_demo_config_enables_all_shipped_adapters(self):
        config = runpy.run_path(os.path.join(ROOT, "config", "governance_assets.py"))[
            "ASSET_TYPES"]
        self.assertEqual(
            {"cluster", "job", "serving_endpoint", "warehouse"},
            {key for key, value in config.items() if value["enabled"]},
        )

    def test_policy_terms_preserve_rule_json(self):
        terms = flatten_policy_terms(json.dumps({
            "spark_version": {"type": "fixed", "value": "auto:latest-lts", "hidden": True},
            "num_workers": {"type": "range", "maxValue": 10},
        }))
        self.assertEqual(2, len(terms))
        self.assertEqual("range", terms[0]["rule_type"])
        self.assertEqual("fixed", terms[1]["rule_type"])
        self.assertTrue(terms[1]["hidden"])

    def test_policy_family_overrides_replace_inherited_rule(self):
        merged = merge_policy_definitions(
            {"num_workers": {"type": "range", "maxValue": 20}},
            {"num_workers": {"type": "range", "maxValue": 5}},
        )
        self.assertEqual(5, merged["num_workers"]["maxValue"])

    def test_system_discovery_is_primary_and_api_enriches(self):
        system = [{
            "asset_type": "job", "asset_id": "7", "asset_name": "daily",
            "owner": "owner@example.com", "lifecycle_state": "ACTIVE",
            "tags": {"team": "finance"}, "policies": [],
            "discovery_source": "SYSTEM_TABLE", "api_enriched": False,
            "tag_observation_complete": True,
            "policy_observation_complete": False,
            "raw_payload": "system",
        }]
        api = [{
            "asset_type": "job", "asset_id": "7", "asset_name": "daily",
            "owner": "owner@example.com", "lifecycle_state": "PAUSED",
            "tags": {"product": "ledger"},
            "policies": [{"policy_type": "BUDGET_POLICY", "policy_id": "b1"}],
            "raw_payload": "api",
        }]
        merged = merge_discovery_and_enrichment(system, api)
        self.assertEqual(1, len(merged))
        self.assertEqual("SYSTEM_TABLE", merged[0]["discovery_source"])
        self.assertTrue(merged[0]["api_enriched"])
        self.assertTrue(merged[0]["policy_observation_complete"])
        self.assertEqual({"team": "finance", "product": "ledger"}, merged[0]["tags"])
        self.assertEqual("BUDGET_POLICY", merged[0]["policies"][0]["policy_type"])

    def test_warehouse_collector_normalizes_api_tag_shape(self):
        client = Client(warehouses=[{
            "id": "abc123",
            "name": "team-bi-warehouse",
            "creator_name": "owner@example.com",
            "state": "RUNNING",
            "tags": {"custom_tags": [
                {"key": "application", "value": "bi"},
                {"key": "", "value": "dropped"},
            ]},
        }])
        rows = collect_warehouses(client, dict(CONTEXT, product="SQL", asset_type="warehouse"))
        self.assertEqual(1, len(rows))
        self.assertEqual("abc123", rows[0]["asset_id"])
        self.assertEqual({"application": "bi"}, rows[0]["tags"])
        self.assertEqual([], rows[0]["policies"])

    def test_warehouse_cost_resolver_scopes_to_sql_product(self):
        resolver = COST_RESOLVERS["warehouse"]
        self.assertIn("'SQL'", resolver["extra_filter_sql"])
        self.assertIn("warehouse_id", resolver["asset_id_sql"])

    def test_system_discovery_queries_preserve_latest_non_deleted_assets(self):
        for sql, table in (
            (_cluster_sql("123"), "system.compute.clusters"),
            (_job_sql("123"), "system.lakeflow.jobs"),
            (_serving_endpoint_sql("123"), "system.serving.served_entities"),
            (_warehouse_sql("123"), "system.compute.warehouses"),
        ):
            self.assertIn(table, sql)
            self.assertIn("ROW_NUMBER()", sql)
            self.assertIn("IS NULL", sql)

    def test_serving_cost_resolver_excludes_other_endpoint_products(self):
        resolver = COST_RESOLVERS["serving_endpoint"]
        self.assertIn("MODEL_SERVING", resolver["extra_filter_sql"])
        self.assertIn("endpoint_name", resolver["extra_filter_sql"])

    def test_service_principal_collection_is_sdk_only_and_matches_aliases(self):
        class PrincipalAPI(API):
            def get(self, principal_id):
                return Object({
                    "id": principal_id, "application_id": "app-123",
                    "display_name": "daily-loader", "active": False,
                })

        class AccessControlAPI:
            def get_rule_set(self, name, etag):
                self.request = (name, etag)
                return {"grant_rules": [{
                    "role": "roles/servicePrincipal.manager",
                    "principals": ["users/u-1"],
                }]}

        class UserAPI:
            def get(self, user_id):
                return Object({"id": user_id, "user_name": "platform@example.com"})

        workspace = type("Workspace", (), {})()
        workspace.service_principals = PrincipalAPI([{
            "id": "sp-1", "application_id": "app-123",
            "display_name": "daily-loader", "active": False,
        }])
        account = type("Account", (), {
            "access_control": AccessControlAPI(), "users": UserAPI(),
        })()
        rows = collect_service_principals(workspace, account, "acct-1")
        self.assertEqual("sp-1", rows[0]["service_principal_id"])
        self.assertEqual(100, workspace.service_principals.list_kwargs["count"])
        self.assertIn("applicationId", workspace.service_principals.list_kwargs["attributes"])
        self.assertFalse(rows[0]["active"])
        self.assertEqual([{"id": "u-1", "name": "platform@example.com", "type": "USER"}],
                         rows[0]["direct_owners"])
        self.assertEqual("RESOLVED", rows[0]["owner_resolution_status"])
        self.assertEqual("sp-1", match_owner_to_principal("APP-123", rows)[
            "service_principal_id"])

    def test_manager_resolution_is_skipped_for_unrelated_principals(self):
        class PrincipalAPI(API):
            def get(self, _principal_id):
                raise AssertionError("per-principal get must not be called")

        workspace = type("Workspace", (), {})()
        workspace.service_principals = PrincipalAPI([{
            "id": "sp-1", "application_id": "app-1",
            "display_name": "unrelated-loader", "active": True,
        }])
        rows = collect_service_principals(
            workspace, object(), "acct-1", asset_owners=["different-owner"])
        self.assertEqual("NOT_REQUESTED", rows[0]["owner_resolution_status"])
        self.assertEqual([], rows[0]["direct_owners"])

    def test_manager_api_absence_degrades_visibly(self):
        owners, status, error = resolve_direct_owners(object(), "acct-1", "app-1")
        self.assertEqual([], owners)
        self.assertEqual("UNAVAILABLE", status)
        self.assertIn("access_control", error)

    def test_manager_api_failure_is_visible_not_fatal(self):
        class AccessControlAPI:
            def get_rule_set(self, **_kwargs):
                raise PermissionError("account admin required")

        account = type("Account", (), {"access_control": AccessControlAPI()})()
        owners, status, error = resolve_direct_owners(account, "acct-1", "app-1")
        self.assertEqual([], owners)
        self.assertEqual("ERROR", status)
        self.assertIn("PermissionError", error)


if __name__ == "__main__":
    unittest.main()
