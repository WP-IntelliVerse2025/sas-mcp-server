"""Tiny on-disk store of dashboard build specs, keyed by SAS report id.

A dashboard is generated from an ``objects`` list + table coords. To EDIT it later
(add / remove / replace a tile) we regenerate from the original spec rather than
parse namespaced BIRD back into objects — so we persist the spec when the
dashboard is created. Single JSON file next to the server; adequate for the
single-instance MCP deployment (edits run through the same process). Best-effort:
any IO error degrades to "no saved spec" so a create never fails because of it.
"""
from __future__ import annotations

import json
import os
import threading

_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_dashboard_specs.json")
_LOCK = threading.Lock()


def _read() -> dict:
    try:
        with open(_PATH, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _write(data: dict) -> None:
    tmp = _PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f)
    os.replace(tmp, _PATH)


def save_spec(report_id: str, spec: dict) -> None:
    """Persist the build spec for a dashboard. Best-effort (never raises)."""
    try:
        with _LOCK:
            data = _read()
            data[report_id] = spec
            _write(data)
    except Exception:
        pass


def load_spec(report_id: str) -> dict | None:
    with _LOCK:
        return _read().get(report_id)


def delete_spec(report_id: str) -> None:
    try:
        with _LOCK:
            data = _read()
            if report_id in data:
                del data[report_id]
                _write(data)
    except Exception:
        pass
