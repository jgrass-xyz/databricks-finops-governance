"""Local unit tests for _alerting.resolve_send_targets — the alert fan-out logic.

Pure Python, no Spark/Databricks needed. Run directly:

    python3 tests/cost/test_send_targets.py

or via pytest. The Databricks golden harness (test_with_fixtures.py) still owns the
end-to-end scoring/suppression/event_log coverage.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src", "cost"))
import _alerting  # noqa: E402

CH = "TEST_CHANNEL"
UID = "TEST_OWNER"


def test_channel_only_ignores_owner_and_tee():
    assert _alerting.resolve_send_targets("channel_only", UID, CH, True) == [(CH, "channel_only")]
    assert _alerting.resolve_send_targets("channel_only", None, CH, False) == [(CH, "channel_only")]


def test_dm_with_tee_sends_to_both():
    # The default prod/dev behavior: owner DM + a copy to the central channel.
    assert _alerting.resolve_send_targets("dm_with_fallback", UID, CH, True) == [
        (UID, "dm"),
        (CH, "tee_channel"),
    ]


def test_dm_without_tee_is_dm_only():
    assert _alerting.resolve_send_targets("dm_with_fallback", UID, CH, False) == [(UID, "dm")]


def test_unresolved_owner_falls_back_to_channel_once():
    # No Slack match → channel only, never duplicated, regardless of tee.
    assert _alerting.resolve_send_targets("dm_with_fallback", None, CH, True) == [(CH, "fallback_channel")]
    assert _alerting.resolve_send_targets("dm_with_fallback", None, CH, False) == [(CH, "fallback_channel")]


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for t in tests:
        t()
        print(f"PASS  {t.__name__}")
    print(f"\n{len(tests)} passed")
