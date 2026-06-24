"""
studio_client.py — SAS Studio / dataFlows API client.

Wraps the /studio/sessions/* and /dataFlows/* endpoints captured in
FLOW_3.har (query flows), Sas_studio.har (raw SAS code execution),
and flow_create_and_stored.har (SAS Program node flow save).
"""

from __future__ import annotations

import time
import uuid
from datetime import datetime, timezone
from typing import Optional

import httpx

# Stable step UUID for the "SAS Program" node type — confirmed via flow_create_and_stored.har
_SAS_PROGRAM_STEP_ID = "a7190700-f59c-4a94-afe2-214ce639fcde"


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


class SASStudioClient:
    """Thin HTTP client for SAS Studio session + dataFlow execution."""

    def __init__(self, base_url: str, access_token: str,
                 preset_session_id: Optional[str] = None):
        self.base_url = base_url.rstrip("/")
        self._token = access_token
        self._http = httpx.Client(timeout=120.0, follow_redirects=True, verify=False)
        # If a session ID is pre-configured (e.g. from SAS_STUDIO_SESSION_ID env var)
        # skip session creation and reuse it directly.
        self._studio_session_id: Optional[str] = preset_session_id

    # ── Auth ────────────────────────────────────────────────────────────────

    def _hdrs(self, content_type: Optional[str] = None, accept: Optional[str] = None) -> dict:
        h = {
            "Authorization": f"Bearer {self._token}",
            "Accept": accept or "application/json",
            # SAS Studio web app always sends these; some SAS Viya builds require them
            "X-Requested-With": "XMLHttpRequest",
            "SAS-Application-Name": "SAS Studio",
        }
        if content_type:
            h["Content-Type"] = content_type
        return h

    # ── Studio session ───────────────────────────────────────────────────────

    def _ensure_sassession(
        self,
        session_id: str,
        server_name: str = "SAS Studio compute context",
    ) -> dict:
        """Start (or reattach to) the SAS language session within a Studio session.

        Must be called once before the first foreground/submissions call.
        The server returns 201 on first init, 200/409 if already running —
        all treated as success.  Confirmed via cas_con.har:
          POST /studio/sessions/{id}/sassession  body={"serverName":"SAS Studio compute context"}
          -> 201 {"activeServer":"SAS Studio compute context","initializationState":"completed",...}
        """
        try:
            resp = self._http.post(
                f"{self.base_url}/studio/sessions/{session_id}/sassession",
                headers=self._hdrs("application/json"),
                json={"serverName": server_name},
                timeout=30.0,
            )
            if resp.status_code in (401, 403):
                raise RuntimeError(
                    f"Auth error on sassession: HTTP {resp.status_code}: {resp.text[:200]}"
                )
            # 201 = started, 200 = already running, 409 = already initialized
            if resp.content:
                try:
                    return resp.json()
                except Exception:
                    pass
            return {"_status": resp.status_code}
        except RuntimeError:
            raise
        except Exception as exc:
            # Don't hard-fail — the session may already be warm
            return {"_warn": str(exc)}

    def _discover_zone(self) -> list[str]:
        """Return candidate zone names/IDs to try when creating a Studio session.

        The /studio/sessions API requires a ``zone`` value that comes from
        /studio/zones, not from /compute/contexts.  We try several discovery
        paths in priority order and de-duplicate results.
        """
        zones: list[str] = []

        def _add(v: str) -> None:
            v = v.strip()
            if v and v not in zones:
                zones.append(v)

        # ── 1. /studio/zones — the correct source for Studio zone names
        try:
            r = self._http.get(
                f"{self.base_url}/studio/zones",
                headers=self._hdrs(),
                params={"limit": 50},
                timeout=10.0,
            )
            if r.is_success:
                data = r.json()
                for item in data.get("items", data if isinstance(data, list) else []):
                    _add(item.get("name", ""))
                    _add(item.get("id", ""))
        except Exception:
            pass

        # ── 2. /compute/providers/Compute/sources — lists SAS Studio context
        try:
            r = self._http.get(
                f"{self.base_url}/compute/providers/Compute/sources",
                headers=self._hdrs(),
                params={"limit": 50},
                timeout=10.0,
            )
            if r.is_success:
                for item in r.json().get("items", []):
                    _add(item.get("name", ""))
                    _add(item.get("id", ""))
                    # launchContext.contextId is also a valid zone identifier
                    lc = item.get("launchContext", {})
                    _add(lc.get("contextId", ""))
                    _add(lc.get("name", ""))
        except Exception:
            pass

        # ── 3. /compute/contexts — fetch IDs (not names) as last resort
        try:
            r = self._http.get(
                f"{self.base_url}/compute/contexts",
                headers=self._hdrs(),
                params={"limit": 50},
                timeout=10.0,
            )
            if r.is_success:
                for item in r.json().get("items", []):
                    # Only include context IDs here (names already failed)
                    _add(item.get("id", ""))
        except Exception:
            pass

        # ── 4. Hard-coded fallbacks (confirmed from cas_con.har sassession response)
        for fb in ["SAS Studio compute context", "SAS Studio", "default", "Compute", "Default"]:
            _add(fb)

        return [z for z in zones if z]  # strip any empty strings

    def _list_existing_sessions(self) -> list[str]:
        """Return IDs of Studio sessions that are still alive.

        GET /studio/sessions lists sessions owned by the current user.
        We ping each one's keepalive to filter to only truly active ones.
        """
        ids: list[str] = []
        try:
            r = self._http.get(
                f"{self.base_url}/studio/sessions",
                headers=self._hdrs(),
                params={"limit": 20},
                timeout=10.0,
            )
            if not r.is_success:
                return ids
            data = r.json()
            items = data.get("items", data if isinstance(data, list) else [])
            for item in items:
                sid = (item.get("id") or item.get("sessionId")
                       or item.get("sessionid") or "")
                if sid:
                    ids.append(sid)
        except Exception:
            pass
        return ids

    def _find_alive_session(self) -> Optional[str]:
        """Return the first existing Studio session that responds to keepalive."""
        for sid in self._list_existing_sessions():
            try:
                ping = self._http.get(
                    f"{self.base_url}/studio/sessions/{sid}/keepalive",
                    headers=self._hdrs(),
                    timeout=8.0,
                )
                if ping.status_code == 200:
                    return sid
            except Exception:
                continue
        return None

    def get_or_create_session(self) -> str:
        """Return a cached Studio session ID, reusing an existing one if possible.

        Strategy (in order):
        1. Preset SAS_STUDIO_SESSION_ID env var — fastest path.
        2. List GET /studio/sessions and ping each for keepalive — reuse any
           alive session without touching the zone API at all.
        3. Create a new session, trying bodies with no zone first (let the
           server pick the default), then discovered zone names.
        """
        # ── 1. Preset / cached session ────────────────────────────────────
        if self._studio_session_id:
            try:
                ping = self._http.get(
                    f"{self.base_url}/studio/sessions/{self._studio_session_id}/keepalive",
                    headers=self._hdrs(),
                    timeout=10.0,
                )
                if ping.status_code == 200:
                    return self._studio_session_id
            except Exception:
                pass
            self._studio_session_id = None

        # ── 2. Reuse any existing alive session (avoids zone API entirely) ─
        alive = self._find_alive_session()
        if alive:
            self._studio_session_id = alive
            return alive

        # ── 3. Create a new session ────────────────────────────────────────
        zones = self._discover_zone()
        errors: list[str] = []

        # Try no-zone bodies first (server picks the default context), then
        # discovered zone names.  This matches how the SAS Studio web app
        # behaves on a fresh browser tab when no zone is pre-selected.
        candidate_bodies: list[dict] = [
            {"version": 1},                    # no zone — server default
            {"version": 1, "contextName": "SAS Studio compute context"},
        ]
        for zone in zones:
            candidate_bodies.append({"version": 1, "zone": zone})
            candidate_bodies.append({"version": 1, "zone": zone, "contextName": zone})

        for body in candidate_bodies:
            try:
                resp = self._http.post(
                    f"{self.base_url}/studio/sessions",
                    headers=self._hdrs("application/json"),
                    json=body,
                    timeout=30.0,
                )
                if resp.status_code in (200, 201):
                    data = resp.json()
                    sid = (data.get("id") or data.get("sessionId")
                           or data.get("sessionid"))
                    if sid:
                        self._studio_session_id = sid
                        self._ensure_sassession(sid)
                        return sid
                if resp.status_code in (401, 403):
                    raise RuntimeError(
                        f"Auth failed (HTTP {resp.status_code}): {resp.text[:400]}"
                    )
                errors.append(
                    f"body={list(body.keys())} "
                    f"→ HTTP {resp.status_code}: {resp.text[:250]}"
                )
            except RuntimeError:
                raise
            except Exception as exc:
                errors.append(f"body={list(body.keys())}: {exc}")

        raise RuntimeError(
            "Could not create a SAS Studio session.\n"
            "Bodies tried: " + str([list(b.keys()) for b in candidate_bodies[:6]]) + "\n"
            "Errors:\n" + "\n".join(f"  {e}" for e in errors[:8])
        )

    def diagnose(self) -> dict:
        """Step-by-step connectivity check — call via sas_studio_diagnose tool."""
        report: dict = {"base_url": self.base_url, "steps": []}

        def _step(name: str, ok: bool, detail: str):
            report["steps"].append({"step": name, "ok": ok, "detail": detail})

        # 0. Check the preset session from SAS_STUDIO_SESSION_ID / constructor argument.
        #    This is the FASTEST path — if the browser session is alive, we're done here.
        preset = self._studio_session_id
        if preset:
            try:
                ping = self._http.get(
                    f"{self.base_url}/studio/sessions/{preset}/keepalive",
                    headers=self._hdrs(),
                    timeout=10.0,
                )
                ok = ping.status_code == 200
                _step(
                    "preset_session_keepalive",
                    ok,
                    f"session_id={preset} HTTP {ping.status_code}"
                    + ("" if ok else
                       " — session expired; get a fresh ID from browser DevTools "
                       "(Network tab → filter 'studio/sessions' → copy UUID from URL)"),
                )
                if ok:
                    report["session_id"] = preset
                    report["overall_ok"] = True
                    report["message"] = (
                        f"Preset session {preset!r} is alive. "
                        "All Studio tools will use this session."
                    )
                    return report
            except Exception as e:
                _step("preset_session_keepalive", False, str(e))
        else:
            _step("preset_session_keepalive", False,
                  "SAS_STUDIO_SESSION_ID not set. "
                  "Get session ID from browser DevTools: Network tab → filter "
                  "'studio/sessions' → copy UUID from URL → run sas_studio_set_session.")

        # 1. Auth check
        for path in ["/compute/contexts", "/identities/users/@currentUser"]:
            try:
                r = self._http.get(
                    f"{self.base_url}{path}",
                    headers=self._hdrs(),
                    params={"limit": 1},
                    timeout=10.0,
                )
                _step(f"auth_check ({path})", r.is_success,
                      f"HTTP {r.status_code}" + ("" if r.is_success else f" body={r.text[:300]}"))
                if r.is_success:
                    break
            except Exception as e:
                _step(f"auth_check ({path})", False, str(e))

        # 2. List existing sessions (primary recovery path — avoids zone guessing)
        try:
            r = self._http.get(
                f"{self.base_url}/studio/sessions",
                headers=self._hdrs(),
                params={"limit": 20},
                timeout=10.0,
            )
            _step("list_existing_sessions", r.is_success,
                  f"HTTP {r.status_code} body={r.text[:500]}")
        except Exception as e:
            _step("list_existing_sessions", False, str(e))

        # 3. Probe /studio/zones directly so the raw response is visible
        try:
            r = self._http.get(
                f"{self.base_url}/studio/zones",
                headers=self._hdrs(),
                params={"limit": 50},
                timeout=10.0,
            )
            _step("studio_zones_endpoint", r.is_success,
                  f"HTTP {r.status_code} body={r.text[:500]}")
        except Exception as e:
            _step("studio_zones_endpoint", False, str(e))

        # Also probe /compute/providers/Compute/sources
        try:
            r = self._http.get(
                f"{self.base_url}/compute/providers/Compute/sources",
                headers=self._hdrs(),
                params={"limit": 20},
                timeout=10.0,
            )
            _step("compute_providers_sources", r.is_success,
                  f"HTTP {r.status_code} body={r.text[:500]}")
        except Exception as e:
            _step("compute_providers_sources", False, str(e))

        zones = self._discover_zone()
        _step("discover_zones", True, f"zones_found={zones[:8]}")

        # 4. Try session creation — no-zone first, then discovered zones
        session_id: Optional[str] = None
        create_bodies = [{"version": 1}] + [{"version": 1, "zone": z} for z in zones[:4]]
        for body in create_bodies:
            try:
                r = self._http.post(
                    f"{self.base_url}/studio/sessions",
                    headers=self._hdrs("application/json"),
                    json=body,
                    timeout=20.0,
                )
                ok = r.status_code in (200, 201)
                label = "no-zone" if "zone" not in body else f"zone={body['zone']!r}"
                _step(
                    f"create_session ({label})",
                    ok,
                    f"HTTP {r.status_code} body={r.text[:400]}",
                )
                if ok:
                    data = r.json()
                    session_id = data.get("id") or data.get("sessionId")
                    break
            except Exception as e:
                _step(f"create_session (body={list(body.keys())})", False, str(e))

        # 5. Try reusing an existing alive session if creation failed
        if not session_id:
            alive = self._find_alive_session()
            if alive:
                session_id = alive
                _step("reuse_existing_session", True, f"session_id={alive}")
            else:
                _step("reuse_existing_session", False,
                      "No alive existing session found either")

        # 6. SAS session init (sassession) — must succeed before code submission
        if session_id:
            try:
                ss = self._ensure_sassession(session_id)
                init_state = ss.get("initializationState", ss.get("_status", "?"))
                _step("sassession_init", "_warn" not in ss,
                      f"initializationState={init_state} activeServer={ss.get('activeServer','')}")
            except Exception as e:
                _step("sassession_init", False, str(e))

        # 5. Code submission test (only if a session was created)
        if session_id:
            try:
                sub = self.submit_code(session_id, "%put diagnose ok;",
                                       label="diagnose.sas")
                sub_id = sub.get("id", "")
                _step("submit_code", bool(sub_id), f"submission_id={sub_id}")
                if sub_id:
                    state = self.wait_for_completion(
                        session_id, sub_id, is_flow=False, max_wait=30)
                    _step("wait_completion", state != "timeout", f"state={state}")
                    html = self.get_html_result(session_id, sub_id)
                    _step("get_html_result", bool(html),
                          f"html_len={len(html) if html else 0}")
            except Exception as e:
                _step("submit_code", False, str(e))

        report["overall_ok"] = all(s["ok"] for s in report["steps"])
        return report

    # ── Table metadata ───────────────────────────────────────────────────────

    def get_table_rows(
        self,
        session_id: str,
        libref: str,
        table: str,
        limit: int = 500,
    ) -> dict:
        """Fetch rows from a SAS library table as JSON via the Studio data API.

        Returns {"columns": [...], "rows": [[...], ...]} or {} on error.
        Uses POST /studio/sessions/{id}/data/libraries/{lib}/tables/{tbl}/rows
        confirmed in Sas_studio.har.
        """
        resp = self._http.post(
            f"{self.base_url}/studio/sessions/{session_id}"
            f"/data/libraries/{libref}/tables/{table}/rows",
            headers=self._hdrs("application/json"),
            params={
                "start": 0,
                "limit": limit,
                "applyFormats": "true",
                "formatMissingValues": "true",
            },
            json={},
            timeout=30.0,
        )
        if not resp.is_success:
            return {}
        data = resp.json()
        return {
            "columns": [c.get("name", "") for c in data.get("columns", [])],
            "rows":    [
                [r.get(c.get("name", ""), "") for c in data.get("columns", [])]
                for r in data.get("rows", [])
            ],
            "total": data.get("count", 0),
        }

    def rows_to_html(self, table_data: dict, title: str = "") -> str:
        """Convert get_table_rows() output to a simple styled HTML table."""
        columns = table_data.get("columns", [])
        rows    = table_data.get("rows", [])
        total   = table_data.get("total", len(rows))
        if not columns:
            return ""

        th = "".join(f"<th>{c}</th>" for c in columns)
        trs = "".join(
            "<tr>" + "".join(f"<td>{v}</td>" for v in row) + "</tr>"
            for row in rows
        )
        note = (f"<p style='font-size:11px;color:#888;margin:4px 0 0'>Showing "
                f"{len(rows)} of {total} rows</p>") if total > len(rows) else ""
        heading = f"<h3 style='margin:0 0 8px;font-size:13px'>{title}</h3>" if title else ""

        return f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<style>
  body{{font-family:Arial,sans-serif;font-size:13px;background:#fff;padding:10px}}
  table{{border-collapse:collapse;width:100%}}
  th{{background:#f0f0f0;font-weight:600;text-align:left;padding:5px 10px;
      border:1px solid #ccc;white-space:nowrap}}
  td{{padding:4px 10px;border:1px solid #ddd;white-space:nowrap}}
  tr:nth-child(even){{background:#f9f9f9}}
</style></head><body>
{heading}<table><thead><tr>{th}</tr></thead><tbody>{trs}</tbody></table>{note}
</body></html>"""

    def get_table_meta(self, session_id: str, libref: str, table: str) -> dict:
        resp = self._http.get(
            f"{self.base_url}/studio/sessions/{session_id}/data/libraries/{libref}/tables/{table}",
            headers=self._hdrs(),
        )
        resp.raise_for_status()
        return resp.json()

    def get_table_columns(self, session_id: str, libref: str, table: str) -> list:
        resp = self._http.post(
            f"{self.base_url}/studio/sessions/{session_id}/data/libraries/{libref}/tables/{table}/columns",
            headers=self._hdrs("application/json"),
            json={"start": 0, "limit": 1000},
        )
        resp.raise_for_status()
        return resp.json().get("items", [])

    def list_libraries(self, session_id: str) -> list:
        resp = self._http.get(
            f"{self.base_url}/studio/sessions/{session_id}/data/libraries",
            headers=self._hdrs(),
            params={"limit": 100},
        )
        resp.raise_for_status()
        return resp.json().get("items", [])

    def list_tables(self, session_id: str, libref: str) -> list:
        resp = self._http.get(
            f"{self.base_url}/studio/sessions/{session_id}/data/libraries/{libref}/tables",
            headers=self._hdrs(),
            params={"start": 0, "limit": 200, "sortBy": "name:ascending"},
        )
        resp.raise_for_status()
        return resp.json().get("items", [])

    # ── Flow submission ──────────────────────────────────────────────────────

    def submit_query_flow(
        self,
        session_id: str,
        input_libref: str,
        input_table: str,
        output_libref: str,
        output_table: str,
        selected_columns: Optional[list[str]] = None,
        sort_by: Optional[list[dict]] = None,
        filters: Optional[list[dict]] = None,
    ) -> dict:
        """
        Build and submit a Studio Query flow.

        sort_by:  [{"column": "Age", "ascending": False}, ...]
        filters:  [{"column": "Weight", "operator": "lessthan", "value": "60"}, ...]
          operator values: equals, notequals, lessthan, greaterthan,
                           lessthanorequals, greaterthanorequals, contains, startswith
        """
        # Ensure the SAS language engine is running before submitting the flow
        self._ensure_sassession(session_id)

        # Fetch table metadata so the parameters section is complete
        try:
            tbl_meta = self.get_table_meta(session_id, input_libref, input_table)
            cols_raw = self.get_table_columns(session_id, input_libref, input_table)
        except Exception:
            tbl_meta = {}
            cols_raw = []

        body = self._build_flow_body(
            session_id, input_libref, input_table, output_libref, output_table,
            tbl_meta, cols_raw, selected_columns, sort_by, filters,
        )
        resp = self._http.post(
            f"{self.base_url}/studio/sessions/{session_id}/foreground/submissions",
            headers=self._hdrs("application/json"),
            json=body,
        )
        resp.raise_for_status()
        return resp.json()

    def _build_flow_body(
        self,
        session_id: str,
        in_lib: str, in_tbl: str,
        out_lib: str, out_tbl: str,
        tbl_meta: dict, cols_raw: list,
        selected_columns, sort_by, filters,
    ) -> dict:
        src_id   = str(uuid.uuid4())
        query_id = str(uuid.uuid4())
        out_id   = str(uuid.uuid4())
        now      = _now()

        # Selected columns
        sel_cols = []
        col_names = selected_columns or [c.get("name", "") for c in cols_raw]
        for col in col_names:
            sel_cols.append({
                "inColumn": {"referenceType": "column", "port": "inTables",
                             "portIndex": 0, "columnName": col},
                "isModified": False, "modifiedColumn": None,
                "isCalculated": False, "calculatedColumn": None,
                "expressionText": None, "_uuid": str(uuid.uuid4()),
            })

        # Sort
        order_by = []
        for s in (sort_by or []):
            order_by.append({
                "isCalculated": False,
                "inColumn": {"referenceType": "column", "port": "inTables",
                             "portIndex": 0, "columnName": s["column"]},
                "expressionText": "", "calculatedColumnName": "",
                "ascending": s.get("ascending", True),
            })

        # Filters
        where_filters = []
        for i, f in enumerate(filters or []):
            where_filters.append({
                "_uuid": str(uuid.uuid4()), "index": i,
                "inColumn": {"referenceType": "column", "port": "inTables",
                             "portIndex": 0, "columnName": f["column"]},
                "value": [str(f["value"])],
                "operator": f.get("operator", "equals"),
                "logicalOperator": "AND",
                "allowMacros": False, "matchCase": False,
                "quoteStrings": True, "useRaw": True,
                "isCalculated": False, "calculatedColumn": None,
                "tableAlias": "t1",
            })

        # Column defs for parameters section
        col_defs = [
            {
                "name": c.get("name", ""), "version": 2, "index": i,
                "byteLength": c.get("byteLength", 8),
                "charLength": c.get("charLength", -1),
                "type": c.get("type", "Numeric"), "Aggregate": "",
                "rawLength": c.get("rawLength", 8), "formattedLength": 0,
            }
            for i, c in enumerate(cols_raw)
        ]

        return {
            "label": f"{in_tbl}_query_flow",
            "pathLabel": "", "uri": "",
            "dataFlowAndBindings": {
                "dataFlow": {
                    "id": None,
                    "connections": [
                        {"sourcePort": {"node": src_id,   "portName": "outTable",  "index": 0},
                         "targetPort": {"node": query_id, "portName": "inTables",  "index": 0}},
                        {"sourcePort": {"node": query_id, "portName": "outTable",  "index": 0},
                         "targetPort": {"node": out_id,   "portName": "inTable",   "index": 0}},
                    ],
                    "createdBy": "", "creationTimeStamp": now, "description": "",
                    "eTag": None, "extendedProperties": {}, "modifiedBy": "",
                    "modifiedTimeStamp": now, "name": f"{in_tbl}_flow",
                    "nodes": {
                        src_id: {
                            "description": "", "id": src_id, "name": in_tbl,
                            "nodeType": "table", "note": None, "priority": 1,
                            "properties": {}, "version": 1,
                            "tableReference": {"referenceType": "parameter",
                                               "parameterId": src_id},
                        },
                        query_id: {
                            "description": "", "id": query_id, "name": "Query",
                            "nodeType": "step", "note": None, "priority": 0,
                            "properties": {}, "version": 1,
                            "stepReference": {
                                "type": "uri",
                                # The Query step UUID is stable across SAS Viya deployments
                                "path": "/dataFlows/steps/c072ab75-0a02-4aa1-acf3-63fe9c17a711",
                            },
                            "arguments": {
                                "calculatedColumns": [], "dataSetOptions": "",
                                "explicitPassthroughEnabled": False,
                                "explicitSqlSource": False, "fedSQL": False,
                                "groupByColumns": [], "havingQuickFilters": [],
                                "inObs": 0, "joinFormattedFilter": "",
                                "orderByColumns": order_by,
                                "outObs": 0, "outputMode": "TABLE",
                                "queryJoins": None,
                                "selectedColumns": sel_cols,
                                "selectDistinct": False, "sessionId": "",
                                "useInObs": False, "useOutObs": False,
                                "whereQuickFilters": where_filters,
                                "inTables": [{"referenceType": "inputPort",
                                              "portName": "inTables", "portIndex": 0,
                                              "arguments": {"alias": "t1"}}],
                                "outTable": {"referenceType": "outputPort",
                                             "portName": "outTable", "portIndex": 0,
                                             "arguments": {}},
                            },
                            "portMappings": [],
                        },
                        out_id: {
                            "description": "", "id": out_id, "name": out_tbl,
                            "nodeType": "outputTable", "note": None, "priority": 2,
                            "properties": {}, "version": 1,
                            "tableReference": {"referenceType": "parameter",
                                               "parameterId": out_id},
                            "outputTableArguments": {"advancedOptions": [], "arguments": {}},
                        },
                    },
                    "parameters": {
                        src_id: {
                            "id": src_id, "name": in_tbl, "version": 2,
                            "parameterUsage": "INPUT",
                            "parameterType": "tableStructure",
                            "defaultValue": {
                                "table": {
                                    "name": in_tbl, "type": "dataTable",
                                    "label": tbl_meta.get("label", in_tbl),
                                    "version": 3,
                                    "creationTimeStamp": tbl_meta.get("creationTimeStamp", now),
                                    "modifiedTimeStamp": tbl_meta.get("modifiedTimeStamp", now),
                                    "providerId": "Compute",
                                    "attributes": {
                                        "id": in_tbl, "libref": in_lib,
                                        "engine": tbl_meta.get("engine", "V9"),
                                        "isCas": tbl_meta.get("isCas", False),
                                        "isReadOnly": tbl_meta.get("isReadOnly", False),
                                        "rowCount": tbl_meta.get("rowCount", 0),
                                        "columnCount": tbl_meta.get("columnCount", 0),
                                    },
                                },
                                "source": {
                                    "id": in_lib, "name": in_lib, "version": 2,
                                    "providerId": "Compute",
                                    "hasTables": True, "hasEngines": False,
                                    "createdBy": "", "modifiedBy": "",
                                    "attributes": {"libref": in_lib, "engineName": "V9"},
                                },
                            },
                            "columns": col_defs,
                        },
                        out_id: {
                            "id": out_id, "name": out_tbl, "version": 2,
                            "parameterUsage": "OUTPUT",
                            "parameterType": "table",
                            "defaultValue": {
                                "table": {"name": out_tbl, "version": 2},
                                "source": {
                                    "id": out_lib, "name": out_lib, "version": 2,
                                    "providerId": "Compute",
                                    "hasTables": True, "hasEngines": False,
                                    "createdBy": "", "modifiedBy": "",
                                    "attributes": {"libref": out_lib, "engineName": "V9"},
                                },
                            },
                        },
                    },
                    "sourceVersion": 2, "stickyNotes": [],
                    "statusHandling": [], "version": 4,
                    "properties": {
                        "UI_PROP_DF_OPTIMIZE": "false",
                        "UI_PROP_DF_EXECUTION_ORDERED": "false",
                    },
                },
                "dataFlowReference": None,
                "executionBindings": {
                    "sessionId": session_id,
                    "environmentId": "Compute",
                    "contextId": "",
                    "arguments": {
                        "__NO_OPTIMIZE": {"argumentType": "string", "value": "true"}
                    },
                    "tempTablePrefix": "000",
                    "sources": None,
                    "interactive": True,
                },
            },
        }

    # ── Code submission ──────────────────────────────────────────────────────

    def submit_code(
        self,
        session_id: str,
        code: str,
        label: str = "SAS Program.sas",
    ) -> dict:
        """Submit raw SAS code to a Studio foreground session.

        Returns the submission response dict containing ``id`` (submission ID).
        """
        resp = self._http.post(
            f"{self.base_url}/studio/sessions/{session_id}/foreground/submissions",
            headers=self._hdrs("application/json"),
            json={
                "label": label,
                "pathLabel": "",
                "uri": "",
                "code": code,
                "customCode": True,
                "memberName": label,
                "parentFolderUri": "",
                "statusNotification": False,
            },
        )
        resp.raise_for_status()
        return resp.json()

    def run_code(
        self,
        session_id: str,
        code: str,
        label: str = "SAS Program.sas",
        max_wait: int = 120,
    ) -> dict:
        """Submit SAS code, wait for completion, return HTML result + metadata."""
        # Ensure the SAS language engine is running (idempotent — safe to call every time)
        self._ensure_sassession(session_id)
        submission = self.submit_code(session_id, code, label)
        submission_id = submission.get("id", "")
        state = self.wait_for_completion(
            session_id, submission_id, is_flow=False, max_wait=max_wait
        )
        html_result = self.get_html_result(session_id, submission_id)
        out_tables  = self.get_output_tables(session_id, submission_id)
        return {
            "submission_id":     submission_id,
            "studio_session_id": session_id,
            "state":             state,
            "output_tables":     out_tables,
            "html_result":       html_result,
        }

    # ── Polling & results ────────────────────────────────────────────────────

    def wait_for_completion(
        self,
        session_id: str,
        submission_id: str,
        compute_job_link: Optional[str] = None,
        is_flow: bool = False,
        max_wait: int = 120,
    ) -> str:
        """Poll until the submission completes. Returns final state string.

        For flow submissions: uses the compute job link (fast path) when
        available, otherwise falls back to longpoll with ``flowStatus=true``.
        For code submissions (``is_flow=False``): always uses longpoll and
        detects ``JobCompleted`` / ``JobFailed`` message types.
        """
        deadline = time.time() + max_wait

        if compute_job_link and is_flow:
            # Fast path: poll compute session job status (flows only)
            while time.time() < deadline:
                try:
                    r = self._http.get(
                        f"{self.base_url}{compute_job_link}",
                        headers=self._hdrs(),
                        timeout=15.0,
                    )
                    if r.is_success:
                        state = r.json().get("state", "running")
                        if state in ("completed", "failed", "error", "canceled"):
                            return state
                except Exception:
                    pass
                time.sleep(2)
        else:
            # Longpoll — works for both code and flow submissions.
            # For flows: include flowStatus=true so the server reports flow
            # node states in addition to log chunks.
            start = 0
            params: dict = {"start": start, "logType": "html"}
            if is_flow:
                params["flowStatus"] = "true"

            while time.time() < deadline:
                try:
                    params["start"] = start
                    r = self._http.get(
                        f"{self.base_url}/studio/sessions/{session_id}"
                        f"/foreground/submissions/{submission_id}/longpoll",
                        params=params,
                        headers=self._hdrs(),
                        timeout=35.0,
                    )
                    if r.is_success:
                        msgs = r.json()
                        if not msgs:
                            # Empty array means nothing new — either still running
                            # or already finished.  Re-check with a short sleep.
                            time.sleep(1)
                            continue

                        next_start = start
                        for msg in msgs:
                            mt = msg.get("messageType", "")
                            payload = msg.get("payload", {})

                            if mt == "LogChunk":
                                # Advance offset so next poll doesn't repeat chunks
                                ns = payload.get("nextStart")
                                if ns is not None:
                                    next_start = max(next_start, ns)
                                else:
                                    next_start += 1

                            elif mt == "LogEnd":
                                next_start += 1

                            elif mt == "JobCompleted":
                                return payload.get("status", "completed")

                            elif mt in ("JobFailed", "JobError"):
                                return "error"

                        start = max(start + 1, next_start)
                except Exception:
                    pass
                time.sleep(1)

        return "timeout"

    def get_html_result(self, session_id: str, submission_id: str) -> Optional[str]:
        resp = self._http.get(
            f"{self.base_url}/studio/sessions/{session_id}"
            f"/foreground/submissions/{submission_id}/results/html",
            headers=self._hdrs(accept="text/html,application/xhtml+xml,*/*"),
        )
        return resp.text if resp.is_success else None

    def get_log(self, session_id: str, submission_id: str) -> Optional[str]:
        resp = self._http.get(
            f"{self.base_url}/studio/sessions/{session_id}"
            f"/foreground/submissions/{submission_id}/log",
            headers=self._hdrs(accept="text/html,*/*"),
        )
        return resp.text if resp.is_success else None

    def get_output_tables(self, session_id: str, submission_id: str) -> list:
        resp = self._http.get(
            f"{self.base_url}/studio/sessions/{session_id}"
            f"/foreground/submissions/{submission_id}/tables",
            headers=self._hdrs(),
        )
        return resp.json().get("items", []) if resp.is_success else []

    # ── Flow save (SAS Program node) ─────────────────────────────────────────

    def get_my_folder_uri(self, session_id: str) -> Optional[str]:
        """Return the /folders/folders/{id} URI for the current user's My Folder.

        Browses @contentroot and picks the first item with contentType in
        ('myFolder', 'userFolder').  Falls back to the first folder listed.
        """
        try:
            r = self._http.get(
                f"{self.base_url}/studio/sessions/{session_id}/files/%40contentroot/members",
                headers=self._hdrs(),
                params={"start": 0, "limit": 100, "recursive": "false",
                        "sortBy": "name:ascending,type:ascending"},
                timeout=15.0,
            )
            if not r.is_success:
                return None
            items = r.json().get("items", [])
            # Prefer myFolder / userFolder content types
            for ct in ("myFolder", "userFolder"):
                for item in items:
                    if item.get("contentType") == ct:
                        uri = item.get("uri", "")
                        # uri looks like /folders/folders/{uuid}
                        if uri.startswith("/folders/folders/"):
                            return uri
            # Fall back to first folder
            for item in items:
                uri = item.get("uri", "")
                if uri.startswith("/folders/folders/"):
                    return uri
        except Exception:
            pass
        return None

    def list_content_folders(self, session_id: str, folder_uri: Optional[str] = None) -> list:
        """List subfolders within a SAS content folder.

        If folder_uri is None, lists the @contentroot top-level folders.
        folder_uri should be a /folders/folders/{id} path.
        """
        if folder_uri:
            # Encode as sascontent:~fs~folders~fs~folders~fs~{id}
            folder_id = folder_uri.rstrip("/").split("/")[-1]
            encoded = f"sascontent%3A~fs~folders~fs~folders~fs~{folder_id}"
            url = (f"{self.base_url}/studio/sessions/{session_id}"
                   f"/files/{encoded}/members")
        else:
            url = (f"{self.base_url}/studio/sessions/{session_id}"
                   f"/files/%40contentroot/members")

        try:
            r = self._http.get(
                url,
                headers=self._hdrs(),
                params={"start": 0, "limit": 100, "recursive": "false",
                        "sortBy": "name:ascending,type:ascending"},
                timeout=15.0,
            )
            if not r.is_success:
                return []
            return r.json().get("items", [])
        except Exception:
            return []

    def save_sas_program_flow(
        self,
        name: str,
        code: str,
        folder_uri: str,
        session_id: str,
        description: str = "",
        overwrite: bool = True,
    ) -> dict:
        """Save a SAS Program node flow to the SAS content server.

        Confirmed via flow_create_and_stored.har:
          POST /dataFlows/dataFlows?parentFolderUri=/folders/folders/{id}&overwrite=true
          body: flow JSON with a single SAS Program step node containing embedded SAS code
          -> 201 {"id": "...", "name": "Nik_test_2.flw", ...}

        Args:
            name:        Flow filename — will have .flw appended if missing.
            code:        SAS code to embed inside the SAS Program node.
            folder_uri:  /folders/folders/{uuid} — where to save.
            session_id:  Active Studio session (for history registration).
            overwrite:   Replace if a flow with the same name already exists.
        Returns dict with at minimum: id, name, flow_uri (/dataFlows/dataFlows/{id}).
        """
        if not name.endswith(".flw"):
            name = name + ".flw"

        node_id = str(uuid.uuid4())
        now = _now()

        flow_body = {
            "id": "",
            "connections": [],
            "createdBy": "",
            "creationTimeStamp": now,
            "description": description,
            "eTag": "",
            "extendedProperties": {},
            "modifiedBy": "",
            "modifiedTimeStamp": now,
            "name": name,
            "nodes": {
                node_id: {
                    "description": "",
                    "id": node_id,
                    "name": "SAS Program",
                    "nodeType": "step",
                    "note": None,
                    "priority": 0,
                    "properties": {
                        "UI_PROP_LOCATION": "56 30",
                        "UI_PROP_INPUT_PORT|inTables|0":  "|Input table 1|Input tables",
                        "UI_PROP_OUTPUT_PORT|outTables|0": "|Output table 1|Output tables",
                        "UI_PROP_IS_INPUT_EXPANDED":  "false",
                        "UI_PROP_IS_OUTPUT_EXPANDED": "false",
                    },
                    "version": 1,
                    "stepReference": {
                        "type": "uri",
                        "path": f"/dataFlows/steps/{_SAS_PROGRAM_STEP_ID}",
                    },
                    "arguments": {
                        "codeOptions": {
                            "code": code,
                            "contentType": "embedded",
                            "variables": [
                                {
                                    "name": "_input1",
                                    "value": {
                                        "referenceType": "inputPort",
                                        "portName": "inTables",
                                        "portIndex": 0,
                                    },
                                },
                                {
                                    "name": "_output1",
                                    "value": {
                                        "referenceType": "outputPort",
                                        "portName": "outTables",
                                        "portIndex": 0,
                                        "arguments": {},
                                    },
                                },
                            ],
                        }
                    },
                    "portMappings": [
                        {
                            "mappingType": "tableStructure",
                            "portName": "outTables",
                            "portIndex": 0,
                            "tableStructure": {"columnDefinitions": []},
                        }
                    ],
                }
            },
            "parameters": {},
            "sourceVersion": 2,
            "stickyNotes": [],
            "statusHandling": [],
            "version": 4,
            "properties": {
                "UI_PROP_DF_OPTIMIZE": "false",
                "UI_PROP_DF_EXECUTION_ORDERED": "false",
            },
        }

        resp = self._http.post(
            f"{self.base_url}/dataFlows/dataFlows",
            headers=self._hdrs("application/json"),
            params={
                "parentFolderUri": folder_uri,
                "overwrite": "true" if overwrite else "false",
            },
            json=flow_body,
            timeout=30.0,
        )
        if resp.status_code == 403:
            raise PermissionError(
                f"Cannot save to folder '{folder_uri}' — 403 Forbidden. "
                "You do not have write permission to this folder. "
                "Save to 'My Folder' instead: omit folder_uri or pass the URI "
                "for My Folder from sas_studio_list_content_folders."
            )
        resp.raise_for_status()
        data = resp.json()
        flow_id = data.get("id", "")

        # Register in Studio session history (mirrors what the UI does)
        if flow_id and session_id:
            try:
                self._http.post(
                    f"{self.base_url}/studio/sessions/{session_id}"
                    f"/files/@myHistory/reference/{name}",
                    headers=self._hdrs("application/json"),
                    json={"uri": f"/dataFlows/dataFlows/{flow_id}",
                          "contentType": "dataFlow"},
                    timeout=10.0,
                )
            except Exception:
                pass

        return {
            "id":       flow_id,
            "name":     data.get("name", name),
            "flow_uri": f"/dataFlows/dataFlows/{flow_id}" if flow_id else "",
            "folder_uri": folder_uri,
            "saved":    resp.status_code == 201,
        }

    # ── Step catalogue ───────────────────────────────────────────────────────

    def list_step_types(self) -> list:
        resp = self._http.get(
            f"{self.base_url}/dataFlows/steps",
            headers=self._hdrs(),
            params={"limit": 100, "omitLinks": "true"},
        )
        resp.raise_for_status()
        return resp.json().get("items", [])

    def list_step_categories(self) -> list:
        resp = self._http.get(
            f"{self.base_url}/dataFlows/stepCategories",
            headers=self._hdrs(),
        )
        resp.raise_for_status()
        return resp.json().get("items", [])

    def close(self) -> None:
        self._http.close()
