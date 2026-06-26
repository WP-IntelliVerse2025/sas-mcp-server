# Copyright © 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""CAS table/caslib administration + data-validation + governance-read tools.

Every tool here uses the **casManagement** / **authorization** REST services
directly — the same proven pattern as the core tools — so each maps to a known
endpoint with a known response (no CAS-action scripting, which can't reach a CAS
session from the compute service in this deployment). Endpoints were confirmed
against the live environment via the resources' own HATEOAS links
(``delete``/``save``/``updateState``/``columns``/``summaryStatistics``/
``distinctCount`` on a table; ``delete``/``sources`` on a caslib).

Auth model is identical to the other tool modules: ``get_token(ctx)`` resolves
the caller's Viya token and the calls run as that user.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any

import httpx
from fastmcp import Context, FastMCP

from .config import VIYA_ENDPOINT
from .viya_client import get_json, get_paged_items, logger, make_client

_CASM = "/casManagement/servers"


def register_cas_admin_tools(
    mcp: FastMCP, get_token: Callable[[Context], Awaitable[str]]
) -> None:
    """Register the CAS-admin / data-validation / governance-read tools."""

    @asynccontextmanager
    async def session(name: str, ctx: Context) -> AsyncIterator[httpx.AsyncClient]:
        logger.info("--- TOOL USED: %s ---", name)
        token = await get_token(ctx)
        async with make_client(token) as client:
            yield client

    def _err(exc: Exception) -> dict[str, Any]:
        if isinstance(exc, httpx.HTTPStatusError):
            return {"status": "error", "httpStatusCode": exc.response.status_code,
                    "message": (exc.response.text or "")[:300]}
        return {"status": "error", "message": f"{type(exc).__name__}: {exc}"}

    def _tbl(server, caslib, table):
        return f"{_CASM}/{server}/caslibs/{caslib}/tables/{table}"

    # ── CAS table: drop / load (server-confirmed: delete:DELETE, updateState:PUT) ─

    @mcp.tool(
        name="drop_cas_table",
        description=(
            "Drop a CAS table — remove it from CAS memory (and its casManagement "
            "reference). Irreversible for the in-memory copy; a saved source file is "
            "not affected. Args: server_id (e.g. 'cas-shared-default'), caslib_name, "
            "table_name."
        ),
    )
    async def drop_cas_table(
        caslib_name: str, table_name: str, ctx: Context,
        server_id: str = "cas-shared-default",
    ) -> Any:
        try:
            async with session("drop_cas_table", ctx) as client:
                r = await client.delete(f"{VIYA_ENDPOINT}{_tbl(server_id, caslib_name, table_name)}")
                if r.status_code == 404:
                    return {"status": "not_found", "table": f"{caslib_name}.{table_name}"}
                r.raise_for_status()
                return {"status": "dropped", "table": f"{caslib_name}.{table_name}",
                        "server": server_id}
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="load_file_to_cas",
        description=(
            "Load a source table/file that already exists in a caslib into CAS memory, "
            "so it can be queried/analysed. `scope` is 'global' (visible to all "
            "sessions, default) or 'session'. Use list_caslib_files to see loadable "
            "sources, or list_source_tables. (To upload a NEW file from your machine, "
            "use upload_data instead.)"
        ),
    )
    async def load_file_to_cas(
        caslib_name: str, table_name: str, ctx: Context,
        server_id: str = "cas-shared-default", scope: str = "global",
    ) -> Any:
        try:
            async with session("load_file_to_cas", ctx) as client:
                r = await client.put(
                    f"{VIYA_ENDPOINT}{_tbl(server_id, caslib_name, table_name)}/state",
                    params={"value": "loaded", "scope": scope},
                    headers={"Accept": "*/*"})
                if r.status_code == 404:
                    return {"status": "not_found", "table": f"{caslib_name}.{table_name}",
                            "message": "No such source table in that caslib."}
                r.raise_for_status()
                return {"status": "loaded", "table": f"{caslib_name}.{table_name}",
                        "scope": scope, "server": server_id,
                        "state": (r.text or "loaded").strip()}
        except Exception as e:
            return _err(e)

    # ── Caslib: drop / list files (server-confirmed: delete, sources) ───────────

    @mcp.tool(
        name="drop_caslib",
        description=(
            "Drop (remove) a caslib from a CAS server. The caslib's data source files "
            "on disk are not deleted; only the caslib definition is removed. Args: "
            "server_id, caslib_name."
        ),
    )
    async def drop_caslib(
        caslib_name: str, ctx: Context, server_id: str = "cas-shared-default"
    ) -> Any:
        try:
            async with session("drop_caslib", ctx) as client:
                r = await client.delete(f"{VIYA_ENDPOINT}{_CASM}/{server_id}/caslibs/{caslib_name}")
                if r.status_code == 404:
                    return {"status": "not_found", "caslib": caslib_name}
                r.raise_for_status()
                return {"status": "dropped", "caslib": caslib_name, "server": server_id}
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="list_caslib_files",
        description=(
            "List the source files/tables available in a caslib's data source (the files "
            "on disk that can be loaded into CAS with load_file_to_cas). Args: server_id, "
            "caslib_name."
        ),
    )
    async def list_caslib_files(
        caslib_name: str, ctx: Context, server_id: str = "cas-shared-default", limit: int = 100
    ) -> Any:
        try:
            async with session("list_caslib_files", ctx) as client:
                # follow the caslib's own 'sources' link so the path is server-correct
                cl = await get_json(f"{_CASM}/{server_id}/caslibs/{caslib_name}", client)
                href = next((l.get("href") for l in cl.get("links", [])
                             if l.get("rel") == "sources"), None)
                if not href:
                    href = f"{_CASM}/{server_id}/caslibs/{caslib_name}/sources"
                items, count = await get_paged_items(href, client, limit=limit)
                return {"caslib": caslib_name, "count": count, "files": [
                    {k: it.get(k) for k in ("name", "type", "size", "modifiedTimeStamp")
                     if k in it} for it in items]}
        except Exception as e:
            return _err(e)

    # ── Data validation / quality (read-only: columns, info, summaryStatistics) ─

    @mcp.tool(
        name="validate_schema",
        description=(
            "Validate a CAS table's schema against expected columns. `expected_columns` "
            "is a list of {name, type?} (type optional, e.g. 'double'/'char'/'varchar'). "
            "Returns which expected columns are present/missing, any type mismatches, and "
            "extra columns. Args: server_id, caslib_name, table_name, expected_columns."
        ),
    )
    async def validate_schema(
        caslib_name: str, table_name: str, expected_columns: list[dict], ctx: Context,
        server_id: str = "cas-shared-default",
    ) -> Any:
        try:
            async with session("validate_schema", ctx) as client:
                items, _ = await get_paged_items(
                    f"{_tbl(server_id, caslib_name, table_name)}/columns", client, limit=10000)
                actual = {str(c.get("name", "")).lower(): c for c in items}
                missing, mism, present = [], [], []
                for exp in expected_columns:
                    nm = str(exp.get("name", ""))
                    a = actual.get(nm.lower())
                    if not a:
                        missing.append(nm)
                        continue
                    present.append(nm)
                    et = str(exp.get("type", "")).lower()
                    at = str(a.get("type", "")).lower()
                    if et and at and et != at:
                        mism.append({"column": nm, "expected": et, "actual": at})
                exp_names = {str(e.get("name", "")).lower() for e in expected_columns}
                extra = [c.get("name") for k, c in actual.items() if k not in exp_names]
                return {"table": f"{caslib_name}.{table_name}",
                        "valid": not missing and not mism,
                        "present": present, "missing": missing,
                        "type_mismatches": mism, "extra_columns": extra}
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="compare_tables",
        description=(
            "Compare two CAS tables — reconcile their row counts and column sets. Returns "
            "each table's row/column counts, whether row counts match, and columns unique "
            "to each side. Useful to reconcile a source vs a target after a load/transform. "
            "Args: caslib_a, table_a, caslib_b, table_b, server_id."
        ),
    )
    async def compare_tables(
        caslib_a: str, table_a: str, caslib_b: str, table_b: str, ctx: Context,
        server_id: str = "cas-shared-default",
    ) -> Any:
        try:
            async with session("compare_tables", ctx) as client:
                async def info(cl, t):
                    d = await get_json(_tbl(server_id, cl, t), client)
                    cols, _ = await get_paged_items(
                        f"{_tbl(server_id, cl, t)}/columns", client, limit=10000)
                    return d, [str(c.get("name")) for c in cols]
                try:
                    da, ca = await info(caslib_a, table_a)
                    db, cb = await info(caslib_b, table_b)
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 404:
                        return {"status": "not_found",
                                "message": "One of the tables was not found."}
                    raise
                ra, rb = da.get("rowCount"), db.get("rowCount")
                sa, sb = {c.lower() for c in ca}, {c.lower() for c in cb}
                return {
                    "table_a": {"name": f"{caslib_a}.{table_a}", "rowCount": ra, "columns": len(ca)},
                    "table_b": {"name": f"{caslib_b}.{table_b}", "rowCount": rb, "columns": len(cb)},
                    "row_counts_match": ra == rb,
                    "row_count_diff": (ra - rb) if (isinstance(ra, int) and isinstance(rb, int)) else None,
                    "columns_only_in_a": [c for c in ca if c.lower() not in sb],
                    "columns_only_in_b": [c for c in cb if c.lower() not in sa]}
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="run_data_quality_check",
        description=(
            "Run a basic data-quality profile on a CAS table: row count, and per-column "
            "completeness (non-missing %), plus min/max/mean for numeric columns (from "
            "the table's summary statistics). Highlights columns with missing values. "
            "(This is the lightweight profile; advanced DQ — match codes, standardisation "
            "— needs the Data Quality action set.) Args: server_id, caslib_name, table_name."
        ),
    )
    async def run_data_quality_check(
        caslib_name: str, table_name: str, ctx: Context,
        server_id: str = "cas-shared-default",
    ) -> Any:
        try:
            async with session("run_data_quality_check", ctx) as client:
                base = _tbl(server_id, caslib_name, table_name)
                try:
                    tbl = await get_json(base, client)
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 404:
                        return {"status": "not_found", "table": f"{caslib_name}.{table_name}"}
                    raise
                rows = tbl.get("rowCount")
                stats_items: list[dict] = []
                try:
                    stats_items, _ = await get_paged_items(f"{base}/summaryStatistics", client, limit=10000)
                except httpx.HTTPError:
                    pass
                checks = []
                for s in stats_items:
                    name = s.get("columnName")
                    nmiss = s.get("numMissingValues")
                    n = s.get("n")
                    completeness = None
                    if isinstance(n, (int, float)) and isinstance(nmiss, (int, float)) and (n + nmiss):
                        completeness = round(100 * n / (n + nmiss), 2)
                    entry = {"column": name, "missing": nmiss, "completeness_pct": completeness}
                    for k in ("min", "max", "mean"):
                        if s.get(k) is not None:
                            entry[k] = s.get(k)
                    checks.append(entry)
                flagged = [c["column"] for c in checks
                           if isinstance(c.get("missing"), (int, float)) and c["missing"]]
                return {"table": f"{caslib_name}.{table_name}", "rowCount": rows,
                        "numeric_columns_profiled": len(checks),
                        "columns_with_missing": flagged, "profile": checks,
                        "note": "summary stats cover numeric columns; character columns "
                                "are not included in this basic profile"}
        except Exception as e:
            return _err(e)

    # ── Governance (read) ───────────────────────────────────────────────────────

    @mcp.tool(
        name="get_authorization_rules",
        description=(
            "List SAS Viya authorization rules (the general authorization system). "
            "Optionally filter by `object_uri_contains` (e.g. a report/folder URI) to see "
            "the rules that govern a specific object. Returns each rule's type "
            "(grant/prohibit), principalType, permissions, objectUri and enabled flag."
        ),
    )
    async def get_authorization_rules(
        ctx: Context, object_uri_contains: str | None = None, limit: int = 50, start: int = 0
    ) -> Any:
        filters = (f"contains(objectUri,'{object_uri_contains}')"
                   if object_uri_contains else None)
        try:
            async with session("get_authorization_rules", ctx) as client:
                items, count = await get_paged_items(
                    "/authorization/rules", client, limit=limit, start=start, filters=filters)
                return {"count": count, "rules": [
                    {k: it.get(k) for k in ("id", "type", "enabled", "principalType",
                                            "principal", "permissions", "objectUri", "description")
                     if k in it} for it in items]}
        except Exception as e:
            return _err(e)
