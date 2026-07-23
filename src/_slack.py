"""Slack API helpers shared by every detector's router.

Two functions:
- ``post(token, method, payload)`` for JSON-body methods (chat.postMessage, auth.test, ...)
- ``post_form(token, method, params)`` for form-encoded legacy methods (users.lookupByEmail)

Stateless module — pass the bot token to each call. Raises ``SlackError`` on
non-ok responses so callers can branch on ``e.body['error']``.

No Databricks SDK, no spark, no dbutils — keeps this module easy to unit-test
and reusable from any notebook or harness.
"""

import requests


class SlackError(RuntimeError):
    def __init__(self, method, body):
        self.method = method
        self.body = body
        super().__init__(f"Slack API {method} failed: {body.get('error')} (full: {body})")


def post(token, method, payload):
    """POST a JSON payload to ``https://slack.com/api/{method}``.

    Raises ``SlackError`` if Slack returns ``ok: false``.
    """
    r = requests.post(
        f"https://slack.com/api/{method}",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8",
        },
        json=payload,
        timeout=10,
    )
    r.raise_for_status()
    body = r.json()
    if not body.get("ok"):
        raise SlackError(method, body)
    return body


def post_form(token, method, params):
    """POST form-encoded params to ``https://slack.com/api/{method}``.

    Some legacy Slack methods (notably ``users.lookupByEmail``) reject
    JSON bodies and require ``application/x-www-form-urlencoded``.
    """
    r = requests.post(
        f"https://slack.com/api/{method}",
        headers={"Authorization": f"Bearer {token}"},
        data=params,
        timeout=10,
    )
    r.raise_for_status()
    body = r.json()
    if not body.get("ok"):
        raise SlackError(method, body)
    return body
