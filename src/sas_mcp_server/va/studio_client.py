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
        # Compute-service backend (the zero-paste default): creating a SAS Studio
        # session needs a `zone` value that some Viya builds don't expose via any
        # API, so we fall back to the Compute service, which creates sessions
        # against a compute context with no zone and no browser dependency.
        self._compute_session_id: Optional[str] = None
        self._compute_ctx_id: Optional[str] = None

    # ── Backend discriminator ─────────────────────────────────────────────────

    @staticmethod
    def _is_compute(session_id: Optional[str]) -> bool:
        """Compute-service session IDs look like '{uuid}-ses0000'; SAS Studio
        session IDs are plain UUIDs. This lets every method route itself to the
        right set of endpoints without extra state."""
        return bool(session_id) and "-ses" in session_id

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

    # ── Compute backend (zero-paste default) ──────────────────────────────────

    def _find_studio_compute_context(self) -> Optional[str]:
        """Return the compute context ID used by SAS Studio.

        Prefers the context literally named "SAS Studio compute context";
        falls back to the first context whose name contains "studio", then to
        the first context available. Cached after first lookup.
        """
        if self._compute_ctx_id:
            return self._compute_ctx_id
        try:
            r = self._http.get(
                f"{self.base_url}/compute/contexts",
                headers=self._hdrs(),
                params={"limit": 100},
                timeout=15.0,
            )
            if not r.is_success:
                return None
            items = r.json().get("items", [])
            exact = [i for i in items if i.get("name") == "SAS Studio compute context"]
            studio = [i for i in items if "studio" in (i.get("name", "").lower())]
            chosen = (exact or studio or items or [None])[0]
            if chosen:
                self._compute_ctx_id = chosen.get("id")
        except Exception:
            self._compute_ctx_id = None
        return self._compute_ctx_id

    def _compute_session_alive(self, session_id: str) -> bool:
        try:
            r = self._http.get(
                f"{self.base_url}/compute/sessions/{session_id}/state",
                headers=self._hdrs(),
                timeout=10.0,
            )
            return r.is_success
        except Exception:
            return False

    def _create_compute_session(self) -> str:
        """Create a Compute session against the SAS Studio compute context.

        No ``zone`` is required (unlike /studio/sessions), so this works on
        builds where the Studio zone can't be discovered.
        """
        ctx = self._find_studio_compute_context()
        if not ctx:
            raise RuntimeError(
                "No compute context available to create a SAS session. "
                "Check that the Compute service is running and the user has "
                "permission to launch a session."
            )
        r = self._http.post(
            f"{self.base_url}/compute/contexts/{ctx}/sessions",
            headers=self._hdrs("application/json"),
            json={},
            timeout=60.0,
        )
        if r.status_code in (401, 403):
            raise RuntimeError(
                f"Auth failed creating compute session (HTTP {r.status_code}): "
                f"{r.text[:300]}"
            )
        if r.status_code not in (200, 201):
            raise RuntimeError(
                f"Could not create compute session (HTTP {r.status_code}): "
                f"{r.text[:300]}"
            )
        sid = r.json().get("id")
        if not sid:
            raise RuntimeError("Compute session created but no id was returned.")
        self._compute_session_id = sid
        return sid

    def _compute_run_job(self, session_id: str, code: str) -> str:
        """Submit SAS code as a compute job; return the job ID."""
        r = self._http.post(
            f"{self.base_url}/compute/sessions/{session_id}/jobs",
            headers=self._hdrs("application/json"),
            json={"code": [code]},
            timeout=60.0,
        )
        r.raise_for_status()
        return r.json().get("id", "")

    def _compute_wait(self, session_id: str, job_id: str, max_wait: int = 120) -> str:
        deadline = time.time() + max_wait
        while time.time() < deadline:
            try:
                r = self._http.get(
                    f"{self.base_url}/compute/sessions/{session_id}/jobs/{job_id}/state",
                    headers=self._hdrs(accept="text/plain"),
                    timeout=15.0,
                )
                state = (r.text or "").strip()
                if state in ("completed", "error", "failed", "canceled", "warning"):
                    return state
            except Exception:
                pass
            time.sleep(1)
        return "timeout"

    def _compute_get_log(self, session_id: str, job_id: str) -> str:
        try:
            r = self._http.get(
                f"{self.base_url}/compute/sessions/{session_id}/jobs/{job_id}/log",
                headers=self._hdrs(),
                params={"limit": 100000},
                timeout=30.0,
            )
            if not r.is_success:
                return ""
            return "\n".join(it.get("line", "") for it in r.json().get("items", []))
        except Exception:
            return ""

    def _compute_output_tables(self, session_id: str, job_id: str) -> list:
        """Best-effort list of tables produced by a compute job (results of
        type TABLE), shaped like the Studio output-tables list."""
        out: list[dict] = []
        try:
            r = self._http.get(
                f"{self.base_url}/compute/sessions/{session_id}/jobs/{job_id}/results",
                headers=self._hdrs(),
                params={"limit": 100},
                timeout=20.0,
            )
            if not r.is_success:
                return out
            for it in r.json().get("items", []):
                if (it.get("type") or "").upper() != "TABLE":
                    continue
                href = ""
                for ln in (it.get("links") or []):
                    if ln.get("rel") in ("self", "up", "data"):
                        href = ln.get("href", "")
                        break
                libref, name = "", it.get("name", "")
                # href ends with /data/{LIB}/{TABLE}
                parts = [p for p in href.split("/data/")[-1].split("/") if p]
                if len(parts) >= 2:
                    libref, name = parts[0], parts[1]
                out.append({"libref": libref, "name": name})
        except Exception:
            pass
        return out

    def _compute_get_table_rows(self, session_id: str, libref: str,
                                table: str, limit: int = 500) -> dict:
        r = self._http.get(
            f"{self.base_url}/compute/sessions/{session_id}"
            f"/data/{libref}/{table}/rows",
            headers=self._hdrs(),
            params={"start": 0, "limit": limit, "includeColumnNames": "true"},
            timeout=30.0,
        )
        if not r.is_success:
            return {}
        items = r.json().get("items", [])
        columns: list[str] = []
        rows: list[list] = []
        for it in items:
            if isinstance(it, dict) and "columns" in it:
                columns = list(it.get("columns") or [])
            elif isinstance(it, dict) and "cells" in it:
                rows.append(["" if v is None else v for v in it.get("cells", [])])
        return {"columns": columns, "rows": rows, "total": len(rows)}

    def _compute_get_table_columns(self, session_id: str, libref: str,
                                   table: str) -> list:
        r = self._http.get(
            f"{self.base_url}/compute/sessions/{session_id}"
            f"/data/{libref}/{table}/columns",
            headers=self._hdrs(),
            params={"start": 0, "limit": 1000},
            timeout=30.0,
        )
        r.raise_for_status()
        cols = []
        for c in r.json().get("items", []):
            cols.append({
                "name": c.get("name") or c.get("id", ""),
                "type": c.get("type", ""),
                "byteLength": c.get("byteLength", 8),
                "charLength": c.get("charLength", -1),
            })
        return cols

    def _compute_list_libraries(self, session_id: str) -> list:
        r = self._http.get(
            f"{self.base_url}/compute/sessions/{session_id}/data",
            headers=self._hdrs(),
            params={"limit": 200},
            timeout=20.0,
        )
        r.raise_for_status()
        return r.json().get("items", [])

    def _compute_list_tables(self, session_id: str, libref: str) -> list:
        """List tables in a library via dictionary.tables (the /data/{lib}
        collection isn't a tables list on this build)."""
        code = (
            f"proc sql; create table work._mcp_tbls as "
            f"select memname as name from dictionary.tables "
            f"where libname=upcase('{libref}') order by memname; quit;"
        )
        jid = self._compute_run_job(session_id, code)
        self._compute_wait(session_id, jid, max_wait=30)
        data = self._compute_get_table_rows(session_id, "WORK", "_MCP_TBLS", limit=1000)
        return [{"name": r[0]} for r in data.get("rows", []) if r]

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

        No-op for Compute sessions — they are ready to run code on creation.
        """
        if self._is_compute(session_id):
            return {"_status": "compute-session-ready"}
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
        """Return a usable SAS session ID, creating one automatically.

        Strategy (in order):
        1. An explicit, still-alive SAS Studio preset (SAS_STUDIO_SESSION_ID) —
           honored for backward compatibility with a pasted browser session.
        2. A previously-created Compute session that is still alive.
        3. A freshly-created Compute session — the zero-paste default. This needs
           no ``zone`` (unlike /studio/sessions) and no browser, so it works on
           builds where the Studio zone cannot be discovered.

        Compute session IDs look like '{uuid}-ses0000'; every other method uses
        _is_compute() to route to the matching endpoints.
        """
        # ── 1. Explicit Studio preset (only if genuinely alive) ────────────
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
            # Preset is dead — drop it and fall through to Compute.
            self._studio_session_id = None

        # ── 2. Reuse a live Compute session from this client ───────────────
        if self._compute_session_id and self._compute_session_alive(self._compute_session_id):
            return self._compute_session_id

        # ── 3. Create a new Compute session (no zone, no browser) ──────────
        return self._create_compute_session()

    def diagnose(self) -> dict:
        """Step-by-step connectivity check — call via sas_studio_diagnose tool."""
        report: dict = {"base_url": self.base_url, "steps": []}

        def _step(name: str, ok: bool, detail: str):
            report["steps"].append({"step": name, "ok": ok, "detail": detail})

        # 0. Optional Studio preset (only relevant if someone pasted one).
        preset = self._studio_session_id
        if preset:
            try:
                ping = self._http.get(
                    f"{self.base_url}/studio/sessions/{preset}/keepalive",
                    headers=self._hdrs(), timeout=10.0,
                )
                ok = ping.status_code == 200
                _step("studio_preset_keepalive", ok,
                      f"session_id={preset} HTTP {ping.status_code}"
                      + ("" if ok else " (expired — will auto-use a Compute session instead)"))
                if ok:
                    report["session_id"] = preset
                    report["mode"] = "studio-preset"
                    report["overall_ok"] = True
                    report["message"] = f"Studio preset session {preset!r} is alive."
                    return report
            except Exception as e:
                _step("studio_preset_keepalive", False, str(e))
        else:
            _step("studio_preset", True,
                  "No SAS_STUDIO_SESSION_ID set — using the automatic Compute "
                  "session backend (no manual session ID needed).")

        # 1. Auth check
        for path in ["/compute/contexts", "/identities/users/@currentUser"]:
            try:
                r = self._http.get(f"{self.base_url}{path}", headers=self._hdrs(),
                                   params={"limit": 1}, timeout=10.0)
                _step(f"auth_check ({path})", r.is_success,
                      f"HTTP {r.status_code}" + ("" if r.is_success else f" body={r.text[:200]}"))
                if r.is_success:
                    break
            except Exception as e:
                _step(f"auth_check ({path})", False, str(e))

        # 2. Discover the SAS Studio compute context (the zone-free way in).
        ctx = self._find_studio_compute_context()
        _step("find_compute_context", bool(ctx), f"context_id={ctx}")

        # 3. Auto-create a Compute session (no zone, no browser, no paste).
        session_id: Optional[str] = None
        try:
            session_id = self._create_compute_session()
            _step("create_compute_session", bool(session_id),
                  f"session_id={session_id}")
        except Exception as e:
            _step("create_compute_session", False, str(e))

        # 4. Run a tiny job to confirm code execution works end-to-end.
        if session_id:
            try:
                sub = self.submit_code(session_id, "%put diagnose ok;")
                sub_id = sub.get("id", "")
                state = self.wait_for_completion(session_id, sub_id, max_wait=30)
                _step("run_code", state == "completed", f"state={state}")
            except Exception as e:
                _step("run_code", False, str(e))

            # 5. Confirm the data API can read a known table.
            try:
                td = self.get_table_rows(session_id, "SASHELP", "CLASS", limit=1)
                _step("read_data_api", bool(td.get("columns")),
                      f"columns={td.get('columns')}")
            except Exception as e:
                _step("read_data_api", False, str(e))

        report["session_id"] = session_id
        report["mode"] = "compute"
        report["overall_ok"] = all(s["ok"] for s in report["steps"])
        report["message"] = (
            "SAS sessions are created automatically via the Compute service — "
            "no manual session ID is required."
            if report["overall_ok"] else
            "Automatic Compute session path failed — see the failing step above."
        )
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

        Returns {"columns": [...], "rows": [[...], ...], "total": n} or {} on error.
        Uses POST /studio/sessions/{id}/data/libraries/{lib}/tables/{tbl}/rows.

        Response shapes vary by SAS Viya build, so this parser is defensive:
          • columns: a list of strings (newer builds) OR a list of
            {"name": ...} dicts (older / Sas_studio.har builds).
          • row data: under "items" as arrays of cell values (newer builds)
            OR under "rows" as dicts keyed by column name (older builds).
          • count: -1 on builds that don't pre-count — fall back to len(rows).

        Compute sessions use the Compute data API instead (different URL/shape).
        """
        if self._is_compute(session_id):
            return self._compute_get_table_rows(session_id, libref, table, limit)
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

        # Normalize column names (strings or {"name": ...} dicts).
        columns = [
            c.get("name", "") if isinstance(c, dict) else str(c)
            for c in data.get("columns", [])
        ]

        # Normalize rows to a list of value-lists aligned to `columns`.
        rows: list[list] = []
        items = data.get("items")
        if isinstance(items, list):
            for it in items:
                if isinstance(it, list):
                    rows.append(list(it))
                elif isinstance(it, dict):
                    cells = it.get("cells")
                    if isinstance(cells, list):
                        rows.append(list(cells))
                    else:
                        rows.append([it.get(name, "") for name in columns])
        else:
            for r in data.get("rows", []):
                if isinstance(r, dict):
                    rows.append([r.get(name, "") for name in columns])
                elif isinstance(r, list):
                    rows.append(list(r))

        total = data.get("count", 0)
        if not isinstance(total, int) or total < 0:
            total = len(rows)

        return {"columns": columns, "rows": rows, "total": total}

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
        if self._is_compute(session_id):
            resp = self._http.get(
                f"{self.base_url}/compute/sessions/{session_id}/data/{libref}/{table}",
                headers=self._hdrs(),
            )
            return resp.json() if resp.is_success else {}
        resp = self._http.get(
            f"{self.base_url}/studio/sessions/{session_id}/data/libraries/{libref}/tables/{table}",
            headers=self._hdrs(),
        )
        resp.raise_for_status()
        return resp.json()

    def get_table_columns(self, session_id: str, libref: str, table: str) -> list:
        if self._is_compute(session_id):
            return self._compute_get_table_columns(session_id, libref, table)
        # Newer SAS Viya builds reject application/json here with HTTP 415 and
        # require the SAS Studio vendor media type. Send the vendor type first,
        # then fall back to application/json for older builds (per the HAR).
        # NOTE: if this returns [] the Query flow builder selects zero columns,
        # producing an empty output table — so the content type must be right.
        url = (f"{self.base_url}/studio/sessions/{session_id}"
               f"/data/libraries/{libref}/tables/{table}/columns")
        body = {"start": 0, "limit": 1000}
        resp = self._http.post(
            url,
            headers=self._hdrs("application/vnd.sas.studio.columns.request+json"),
            json=body,
        )
        if resp.status_code == 415:
            resp = self._http.post(
                url, headers=self._hdrs("application/json"), json=body
            )
        resp.raise_for_status()
        return resp.json().get("items", [])

    def list_libraries(self, session_id: str) -> list:
        if self._is_compute(session_id):
            return self._compute_list_libraries(session_id)
        resp = self._http.get(
            f"{self.base_url}/studio/sessions/{session_id}/data/libraries",
            headers=self._hdrs(),
            params={"limit": 100},
        )
        resp.raise_for_status()
        return resp.json().get("items", [])

    def list_tables(self, session_id: str, libref: str) -> list:
        if self._is_compute(session_id):
            return self._compute_list_tables(session_id, libref)
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

    # ── Query via PROC SQL (build-independent) ────────────────────────────────
    # The dataFlow "Query" step (submit_query_flow) relies on a JSON schema that
    # some SAS Viya builds silently ignore — the flow "completes" but emits no
    # SQL and writes no output table. This path expresses the same
    # select / filter / sort as PROC SQL and submits it as ordinary code, which
    # every build runs identically.

    @staticmethod
    def _quote_sas_name(name: str) -> str:
        """SAS-safe column reference: simple identifiers pass through, anything
        else (spaces, punctuation) becomes a name literal — \"col name\"n."""
        import re
        if re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", name or ""):
            return name
        return '"' + (name or "").replace('"', '""') + '"n'

    @staticmethod
    def _is_numeric_literal(val) -> bool:
        try:
            float(val)
            return True
        except (TypeError, ValueError):
            return False

    def _sql_filter_clause(self, col: str, operator: str, value: str,
                           col_types: dict) -> str:
        name = self._quote_sas_name(col)
        ctype = (col_types.get((col or "").upper(), "") or "").upper()
        # Character if the column type says so, or (type unknown) the value is
        # non-numeric. Numeric values are emitted unquoted.
        is_char = ctype.startswith(("CHAR", "VARCHAR")) or (
            not ctype and not self._is_numeric_literal(value)
        )

        def lit(v) -> str:
            return "'" + str(v).replace("'", "''") + "'" if is_char else str(v)

        op = (operator or "equals").lower()
        if op == "notequals":           return f"{name} ne {lit(value)}"
        if op == "lessthan":            return f"{name} < {lit(value)}"
        if op == "greaterthan":         return f"{name} > {lit(value)}"
        if op == "lessthanorequals":    return f"{name} <= {lit(value)}"
        if op == "greaterthanorequals": return f"{name} >= {lit(value)}"
        if op == "contains":
            return f"{name} like '%{str(value).replace(chr(39), chr(39)*2)}%'"
        if op == "startswith":
            return f"{name} like '{str(value).replace(chr(39), chr(39)*2)}%'"
        return f"{name} = {lit(value)}"  # equals / default

    def build_query_sql(
        self,
        input_libref: str, input_table: str,
        output_libref: str, output_table: str,
        selected_columns: Optional[list[str]] = None,
        sort_by: Optional[list[dict]] = None,
        filters: Optional[list[dict]] = None,
        col_types: Optional[dict] = None,
    ) -> str:
        """Render a PROC SQL step equivalent to a Studio Query flow."""
        col_types = col_types or {}
        sel = (", ".join(self._quote_sas_name(c) for c in selected_columns)
               if selected_columns else "*")
        sql = (f"proc sql;\n"
               f"  create table {output_libref}.{output_table} as\n"
               f"  select {sel}\n"
               f"  from {input_libref}.{input_table}")
        clauses = [
            self._sql_filter_clause(
                f["column"], f.get("operator", "equals"),
                str(f.get("value", "")), col_types,
            )
            for f in (filters or [])
        ]
        if clauses:
            sql += "\n  where " + " and ".join(clauses)
        if sort_by:
            order = []
            for s in sort_by:
                nm = self._quote_sas_name(s["column"])
                order.append(nm if s.get("ascending", True) else nm + " desc")
            sql += "\n  order by " + ", ".join(order)
        sql += ";\nquit;"
        return sql

    def run_query_via_sql(
        self,
        session_id: str,
        input_libref: str, input_table: str,
        output_libref: str, output_table: str,
        selected_columns: Optional[list[str]] = None,
        sort_by: Optional[list[dict]] = None,
        filters: Optional[list[dict]] = None,
        max_wait: int = 120,
    ) -> dict:
        """Build PROC SQL from the query spec, submit it, and wait for completion.

        Returns {"submission_id", "state", "sql"}. The caller reads the output
        table with get_table_rows().
        """
        self._ensure_sassession(session_id)
        # Column types let us quote character filter values correctly.
        col_types: dict = {}
        try:
            for col in self.get_table_columns(session_id, input_libref, input_table):
                col_types[(col.get("name", "") or "").upper()] = col.get("type", "")
        except Exception:
            pass
        sql = self.build_query_sql(
            input_libref, input_table, output_libref, output_table,
            selected_columns, sort_by, filters, col_types,
        )
        submission = self.submit_code(session_id, sql, label="Query.sas")
        submission_id = submission.get("id", "")
        state = self.wait_for_completion(
            session_id, submission_id, is_flow=False, max_wait=max_wait
        )
        return {"submission_id": submission_id, "state": state, "sql": sql}

    # ── Code submission ──────────────────────────────────────────────────────

    def submit_code(
        self,
        session_id: str,
        code: str,
        label: str = "SAS Program.sas",
    ) -> dict:
        """Submit raw SAS code to a session.

        Returns a dict containing ``id`` — the submission ID (Studio) or job ID
        (Compute). wait_for_completion / get_log / get_html_result accept the
        same id and route by session type.
        """
        if self._is_compute(session_id):
            return {"id": self._compute_run_job(session_id, code)}
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

        For Compute sessions, ``submission_id`` is a job ID and we poll the
        Compute job state endpoint instead.
        """
        if self._is_compute(session_id):
            return self._compute_wait(session_id, submission_id, max_wait=max_wait)

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
        if self._is_compute(session_id):
            # Compute jobs don't expose an ODS-HTML endpoint like Studio does, so
            # surface the SAS log as a readable monospace block. Tabular output
            # is read separately via the data API (get_table_rows).
            log = self._compute_get_log(session_id, submission_id)
            if not log:
                return None
            esc = (log.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))
            return (
                "<!DOCTYPE html><html><head><meta charset='utf-8'></head><body>"
                "<pre style=\"font-family:Consolas,Menlo,monospace;font-size:12px;"
                "white-space:pre-wrap;line-height:1.35\">" + esc + "</pre></body></html>"
            )
        resp = self._http.get(
            f"{self.base_url}/studio/sessions/{session_id}"
            f"/foreground/submissions/{submission_id}/results/html",
            headers=self._hdrs(accept="text/html,application/xhtml+xml,*/*"),
        )
        return resp.text if resp.is_success else None

    def get_log(self, session_id: str, submission_id: str) -> Optional[str]:
        if self._is_compute(session_id):
            return self._compute_get_log(session_id, submission_id) or None
        resp = self._http.get(
            f"{self.base_url}/studio/sessions/{session_id}"
            f"/foreground/submissions/{submission_id}/log",
            headers=self._hdrs(accept="text/html,*/*"),
        )
        return resp.text if resp.is_success else None

    def get_output_tables(self, session_id: str, submission_id: str) -> list:
        if self._is_compute(session_id):
            return self._compute_output_tables(session_id, submission_id)
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

        Compute sessions have no /files endpoint, so they resolve My Folder via
        the Folders service alias directly.
        """
        if self._is_compute(session_id):
            try:
                r = self._http.get(
                    f"{self.base_url}/folders/folders/@myFolder",
                    headers=self._hdrs(),
                    timeout=15.0,
                )
                if r.is_success:
                    fid = r.json().get("id", "")
                    return f"/folders/folders/{fid}" if fid else None
            except Exception:
                pass
            return None
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

            def _clean(uri: str) -> str:
                # SAS may return 'sascontent:/folders/folders/{id}' — strip the
                # prefix so we always hand back a clean '/folders/folders/{id}'.
                if uri.startswith("sascontent:"):
                    uri = uri.replace("sascontent:", "", 1)
                return uri

            # Prefer myFolder / userFolder content types
            for ct in ("myFolder", "userFolder"):
                for item in items:
                    if item.get("contentType") == ct:
                        uri = _clean(item.get("uri", ""))
                        if uri.startswith("/folders/folders/"):
                            return uri
            # Fall back to first folder
            for item in items:
                uri = _clean(item.get("uri", ""))
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
            # Surface SAS's ACTUAL 403 body — do not assume it's a folder write
            # ACL. A 403 here can also be an OAuth scope/capability problem or a
            # parentFolderUri that resolved to the wrong object. The real message
            # tells us which; the caller (server handler) still treats this as the
            # "try My Folder" signal via the PermissionError type.
            detail = (resp.text or "").strip()
            raise PermissionError(
                f"SAS refused the save to '{folder_uri}' with HTTP 403 Forbidden. "
                f"SAS said: {detail[:600] or '(empty body)'}"
            )
        if not resp.is_success:
            # Surface SAS's actual error body — a bare raise_for_status() hides
            # WHY a 400 happened (bad parentFolderUri format, invalid step
            # reference, etc.), leaving the assistant to guess.
            detail = (resp.text or "").strip()
            raise RuntimeError(
                f"SAS rejected the flow save (HTTP {resp.status_code}) for "
                f"parentFolderUri='{folder_uri}'. SAS said: {detail[:600]}"
            )
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
