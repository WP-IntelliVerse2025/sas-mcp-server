# Copyright © 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""SAS Visual Analytics + SAS Studio tools for the SAS Viya MCP server.

These tools were originally a separate low-level MCP server (``VA_SAS_viy_mcp`` /
``VA_SAS_viy_mcp_new``). They are ported here so a single FastMCP server exposes
the VA report/dashboard/geo builders and the SAS Studio code/flow tools
alongside the core SAS Viya execution, CAS, catalog and ML tools.

Auth model: every tool resolves the caller's Viya access token via
``get_token(ctx)`` — the same token source the core tools use (the bearer header
swapped by ``AuthMiddleware`` in HTTP mode, or the cached CLI token in stdio
mode). With that token it constructs the vendored synchronous ``SASViyaClient`` /
``SASStudioClient`` and runs their blocking work in a worker thread so the async
event loop is never blocked.

WPIntelliChat injects per-user/session context as extra ``_``-prefixed tool
arguments (``_sas_access_token``, ``_sas_base_url``, ``_file_b64``,
``_filename``). Those are stripped — and the upload bytes routed into context
state — by ``InjectedArgsMiddleware`` (see ``mcp_server.py``) before a tool runs,
so no tool needs to declare them in its schema.
"""

from __future__ import annotations

import asyncio
import base64
import os
from collections.abc import Awaitable, Callable
from typing import Any

from fastmcp import Context, FastMCP
from fastmcp.server.middleware import Middleware, MiddlewareContext
from fastmcp.utilities.types import Image

from .config import VIYA_ENDPOINT
from .va.geo_builder import build_geo_coordinate, build_geo_region
from .va.report_builder import (
    build_bar_chart_content,
    build_chart_content,
    build_dashboard_content,
)
from .va.sas_client import SASViyaClient
from .va.studio_client import SASStudioClient
from .va.va_objects import build_object_content, list_objects
from .va import spec_store

# Catalog of VA object types reproduced from the captured report (VA_report.har),
# with each type's configurable column roles and their captured default column.
_VA_OBJECTS = list_objects()

_VA_OBJECT_DEFAULT_LIB = "Public"
_VA_OBJECT_DEFAULT_TABLE = "RETAIL_SALES_SAS_VA_DATASET"


def _object_catalog_text() -> str:
    lines = []
    for otype, info in _VA_OBJECTS.items():
        params = info["params"]
        if params:
            role_str = ", ".join(f"{p} (default: {c})" for p, c in params.items())
        else:
            role_str = "no data columns (layout/container object)"
        lines.append(f"  • {otype}: {role_str}")
    return "\n".join(lines)


# ── Column-role validation (kept verbatim from the VA server) ──────────────────
# So a chart/dashboard is NEVER built with columns that don't exist (renders as
# "The required roles have not been assigned a data item" in SAS VA) or with
# required roles left empty / mistyped.
_CHART_COLUMN_KEYS = (
    "category_column", "measure_column", "x_column", "y_column",
    "size_column", "group_column",
    "color_column", "target_column", "measure2_column",
    "column_category", "row_category",
    "calc_numerator", "calc_denominator",
)
_CHART_REQUIRED_ROLES = {
    "bar_h": [("category_column",)],
    "bar_v": [("category_column",)],
    "bar":   [("category_column",)],
    "bar_stacked_v": [("category_column",), ("group_column",)],
    "bar_stacked_h": [("category_column",), ("group_column",)],
    "stacked_bar_v": [("category_column",), ("group_column",)],
    "stacked_bar_h": [("category_column",), ("group_column",)],
    "pie":   [("category_column",)],
    "donut": [("category_column",)],
    "line":  [("x_column", "category_column"), ("y_column", "measure_column")],
    "scatter": [("x_column",), ("y_column",)],
    "bubble":  [("x_column",), ("y_column",), ("size_column",)],
    "kpi":      [("measure_column",)],
    "key_value": [("measure_column",)],
    "treemap":      [("category_column",), ("size_column",)],
    "heat_map":     [("x_column",), ("y_column",), ("measure_column",)],
    "targeted_bar": [("category_column",), ("measure_column",), ("target_column",)],
    "waterfall":    [("category_column",)],
    "histogram":    [("measure_column",)],
    "box_plot":     [("category_column",), ("measure_column",)],
    "word_cloud":   [("category_column",)],
    "crosstab":     [("row_category",), ("column_category",), ("measure_column",)],
    "button_bar":   [("category_column",)],
    "list_control": [("category_column",)],
}
_NUMERIC_ROLES = {
    "bar_h":     ("measure_column",),
    "bar_v":     ("measure_column",),
    "bar":       ("measure_column",),
    "bar_stacked_v": ("measure_column",),
    "bar_stacked_h": ("measure_column",),
    "stacked_bar_v": ("measure_column",),
    "stacked_bar_h": ("measure_column",),
    "line":      ("y_column", "measure_column"),
    "pie":       ("measure_column",),
    "donut":     ("measure_column",),
    "scatter":   ("x_column", "y_column"),
    "bubble":    ("x_column", "y_column", "size_column"),
    "kpi":       ("measure_column",),
    "key_value": ("measure_column",),
    "treemap":      ("size_column", "color_column"),
    "heat_map":     ("measure_column", "color_column"),
    "targeted_bar": ("measure_column", "target_column"),
    "histogram":    ("measure_column",),
    "box_plot":     ("measure_column",),
    "crosstab":     ("measure_column", "measure2_column"),
    "list_control": ("measure_column",),
}
_CHARACTER_CAS_TYPES = {
    "varchar", "char", "character", "string", "nvarchar", "nchar", "text", "clob",
}


def _validate_object_columns(real_columns: list, spec: dict, otype: str):
    """Verify/normalise the column roles in one chart/KPI spec against the table.

    Returns ``(normalised_spec, errors)``. ``normalised_spec`` fixes the casing
    of any column that exists; ``errors`` lists invalid columns + unfilled
    required roles. Empty ``errors`` means the spec is safe to build.
    """
    by_lower = {str(col.get("name", "")).lower(): col.get("name") for col in real_columns}
    norm = dict(spec)
    errors = []
    for key in _CHART_COLUMN_KEYS:
        val = spec.get(key)
        if not val:
            continue
        real = by_lower.get(str(val).lower())
        if real:
            norm[key] = real
        else:
            errors.append(f"column '{val}' (role {key}) does not exist in the table")
    type_by_lower = {str(col.get("name", "")).lower(): str(col.get("type", "")).lower()
                     for col in real_columns}
    for key in _NUMERIC_ROLES.get(otype, ()):
        val = norm.get(key)
        if not val:
            continue
        ctype = type_by_lower.get(str(val).lower())
        if ctype in _CHARACTER_CAS_TYPES:
            errors.append(
                f"column '{val}' (role {key}) is text ({ctype}), but a '{otype}' "
                "needs a numeric column there — pick a numeric column for that role"
            )
    for role_group in _CHART_REQUIRED_ROLES.get(otype, []):
        if not any(norm.get(r) for r in role_group):
            label = " or ".join(role_group)
            errors.append(f"a '{otype}' needs a column for: {label}")
    return norm, errors


# ── Client construction ────────────────────────────────────────────────────────

def _va(token: str) -> SASViyaClient:
    """A request-scoped VA client that runs as the caller (their Viya token)."""
    return SASViyaClient(base_url=VIYA_ENDPOINT, access_token=token)


def _studio(token: str) -> SASStudioClient:
    """A request-scoped Studio client; reuses SAS_STUDIO_SESSION_ID if set."""
    preset = os.environ.get("SAS_STUDIO_SESSION_ID", "").strip() or None
    return SASStudioClient(base_url=VIYA_ENDPOINT, access_token=token,
                           preset_session_id=preset)


def _resolve_parent(c: SASViyaClient, folder_arg: str | None) -> str:
    folder_arg = folder_arg or "@myFolder"
    return c.get_my_folder_uri() if folder_arg == "@myFolder" else folder_arg


# ─────────────────────────────────────────────────────────────────────────────
# Synchronous workers. Each builds its own client(s) from the token and performs
# the original VA/Studio server logic 1:1. They run in a thread (asyncio.to_thread)
# so the FastMCP event loop never blocks on SAS REST calls.
# Chart-style workers return ``(summary_text, png_bytes | None)``; the async tool
# wrapper turns a non-None png into an inline image.
# ─────────────────────────────────────────────────────────────────────────────

def _w_list_cas_libraries(token, cas_server):
    return _va(token).list_cas_libraries(cas_server)


def _w_list_cas_tables(token, library, cas_server):
    return _va(token).list_cas_tables(library, cas_server)


def _w_get_table_columns(token, library, table, cas_server):
    return _va(token).get_table_columns(library, table, cas_server)


def _w_list_folders(token, folder_uri):
    return _va(token).list_folders(folder_uri)


def _w_create_bar_chart(token, a):
    c = _va(token)
    parent_uri = _resolve_parent(c, a.get("parent_folder_uri"))
    report = c.create_report(a["report_name"], parent_uri)
    report_id = report["id"]
    content = build_bar_chart_content(
        report_name=a["report_name"],
        cas_server=a.get("cas_server", "cas-shared-default"),
        cas_library=a["cas_library"], cas_table=a["cas_table"],
        category_column=a["category_column"], category_label=a.get("category_label"),
        group_column=a.get("group_column"), group_label=a.get("group_label"),
        table_label=a.get("table_label"),
    )
    c.update_report_content(report_id, content)
    details = c.get_report(report_id)
    text = (f"Report **{a['report_name']}** created successfully.\n"
            f"Report ID: `{report_id}`\nOpen in SAS VA: {details['url']}")
    try:
        return text, c.get_report_image(report_id)
    except Exception as img_err:
        return text + f"\n(Chart preview unavailable: {img_err})", None


def _w_create_chart(token, a):
    c = _va(token)
    parent_uri = _resolve_parent(c, a.get("parent_folder_uri"))
    try:
        real_cols = c.get_table_columns(
            a["cas_library"], a["cas_table"], a.get("cas_server", "cas-shared-default"))
    except Exception as col_err:
        return (f"Could not read columns for {a['cas_library']}.{a['cas_table']} ({col_err}). "
                "Make sure the table exists and is loaded.", None)
    norm, errors = _validate_object_columns(real_cols, a, a["chart_type"])
    if errors:
        avail = ", ".join(f"{col['name']} ({col.get('type', '?')})" for col in real_cols)
        return ("Cannot create the chart — " + "; ".join(errors) + ".\n"
                f"Available columns in {a['cas_table']}: {avail}.", None)
    a = norm
    report = c.create_report(a["report_name"], parent_uri)
    report_id = report["id"]
    content = build_chart_content(
        chart_type=a["chart_type"], report_name=a["report_name"],
        cas_server=a.get("cas_server", "cas-shared-default"),
        cas_library=a["cas_library"], cas_table=a["cas_table"],
        category_column=a.get("category_column"), category_label=a.get("category_label"),
        measure_column=a.get("measure_column"), measure_label=a.get("measure_label"),
        x_column=a.get("x_column"), x_label=a.get("x_label"),
        y_column=a.get("y_column"), y_label=a.get("y_label"),
        size_column=a.get("size_column"), size_label=a.get("size_label"),
        group_column=a.get("group_column"), group_label=a.get("group_label"),
        table_label=a.get("table_label"),
    )
    try:
        c.update_report_content(report_id, content)
    except Exception:
        try: c.delete_report(report_id)
        except Exception: pass
        raise
    details = c.get_report(report_id)
    text = (f"Report **{details.get('name', a['report_name'])}** ({a['chart_type']}) created.\n"
            f"Report ID: `{report_id}`\nOpen in SAS VA: {details['url']}")
    try:
        return text, c.get_report_image(report_id)
    except Exception as img_err:
        return text + f"\n(Chart preview unavailable: {img_err})", None


def _w_create_dashboard(token, a):
    c = _va(token)
    parent_uri = _resolve_parent(c, a.get("parent_folder_uri"))
    pages_in = a.get("pages")
    flat = ([(pi, obj) for pi, page in enumerate(pages_in) for obj in (page or [])]
            if pages_in else [(0, obj) for obj in (a.get("objects") or [])])
    if not flat:
        return "A dashboard needs at least one object (in 'objects' or 'pages').", None
    try:
        real_cols = c.get_table_columns(
            a["cas_library"], a["cas_table"], a.get("cas_server", "cas-shared-default"))
    except Exception as col_err:
        return (f"Could not read columns for {a['cas_library']}.{a['cas_table']} ({col_err}). "
                "Make sure the table exists and is loaded.", None)
    norm_flat, all_errors = [], []
    for idx, (pi, obj) in enumerate(flat):
        otype = str((obj or {}).get("type", "")).lower().strip()
        nobj, errs = _validate_object_columns(real_cols, obj or {}, otype)
        nobj["type"] = otype
        norm_flat.append((pi, nobj))
        for e in errs:
            all_errors.append(f"tile #{idx + 1} ({otype or '?'}): {e}")
    if all_errors:
        avail = ", ".join(f"{col['name']} ({col.get('type', '?')})" for col in real_cols)
        return ("Cannot create the dashboard — " + "; ".join(all_errors) + ".\n"
                f"Available columns in {a['cas_table']}: {avail}.", None)
    objects = [nobj for _p, nobj in norm_flat]
    _common = dict(report_name=a["report_name"],
                   cas_server=a.get("cas_server", "cas-shared-default"),
                   cas_library=a["cas_library"], cas_table=a["cas_table"],
                   table_label=a.get("table_label"))
    if pages_in:
        norm_pages = [[nobj for pi2, nobj in norm_flat if pi2 == p] for p in range(len(pages_in))]
        content = build_dashboard_content(pages=norm_pages, **_common)
        _spec_layout = {"pages": norm_pages}
    else:
        content = build_dashboard_content(objects=objects, **_common)
        _spec_layout = {"objects": objects}
    report = c.create_report(a["report_name"], parent_uri)
    report_id = report["id"]
    try:
        c.update_report_content(report_id, content)
    except Exception:
        try: c.delete_report(report_id)
        except Exception: pass
        raise
    details = c.get_report(report_id)
    spec_store.save_spec(report_id, {
        "report_name": details.get("name", a["report_name"]),
        "cas_server": a.get("cas_server", "cas-shared-default"),
        "cas_library": a["cas_library"], "cas_table": a["cas_table"],
        "table_label": a.get("table_label"), **_spec_layout,
    })
    tile_summary = ", ".join(
        f"{o.get('type')}"
        + (f"({o.get('measure_column') or o.get('category_column') or o.get('x_column') or ''})"
           if (o.get('measure_column') or o.get('category_column') or o.get('x_column')) else "")
        for o in objects)
    text = (f"Dashboard **{details.get('name', a['report_name'])}** created with "
            f"{len(objects)} objects: {tile_summary}.\n"
            f"Report ID: `{report_id}`\nOpen in SAS VA: {details['url']}")
    try:
        return text, c.get_report_image(report_id)
    except Exception as img_err:
        return text + f"\n(Dashboard preview unavailable: {img_err})", None


def _w_edit_dashboard(token, a):
    c = _va(token)
    rid = a["report_id"]
    op = a["operation"]
    spec = spec_store.load_spec(rid)
    if not spec:
        return (f"No saved build spec for report {rid} — sas_edit_dashboard only works on "
                "dashboards created by this server. Re-create it with "
                "sas_create_dashboard_report, or edit it in SAS VA directly.", None)
    if spec.get("pages"):
        return ("Editing a multi-page dashboard isn't supported yet — re-create it with the "
                "updated `pages`, or edit it in SAS VA directly.", None)
    objs = list(spec.get("objects") or [])
    if op == "add":
        if not a.get("object"):
            return "add needs an 'object' tile spec.", None
        objs.append(a["object"])
    elif op == "remove":
        idx = a.get("index")
        if idx is None or not (0 <= idx < len(objs)):
            return f"remove needs a valid 'index' (0..{len(objs) - 1}).", None
        if len(objs) <= 1:
            return "A dashboard needs at least one tile — won't remove the last.", None
        objs.pop(idx)
    elif op == "replace":
        idx = a.get("index")
        if idx is None or not (0 <= idx < len(objs)) or not a.get("object"):
            return f"replace needs a valid 'index' (0..{len(objs) - 1}) and an 'object'.", None
        objs[idx] = a["object"]
    else:
        return f"Unknown operation '{op}'.", None
    try:
        real_cols = c.get_table_columns(spec["cas_library"], spec["cas_table"],
                                        spec.get("cas_server", "cas-shared-default"))
    except Exception as col_err:
        return f"Could not read columns ({col_err}).", None
    norm_objects, all_errors = [], []
    for i2, obj in enumerate(objs):
        ot = str((obj or {}).get("type", "")).lower().strip()
        nobj, errs = _validate_object_columns(real_cols, obj or {}, ot)
        nobj["type"] = ot
        norm_objects.append(nobj)
        for e in errs:
            all_errors.append(f"tile #{i2 + 1} ({ot or '?'}): {e}")
    if all_errors:
        avail = ", ".join(f"{col['name']} ({col.get('type', '?')})" for col in real_cols)
        return "Cannot edit — " + "; ".join(all_errors) + f".\nAvailable: {avail}.", None
    objs = norm_objects
    content = build_dashboard_content(
        report_name=spec["report_name"], cas_server=spec.get("cas_server", "cas-shared-default"),
        cas_library=spec["cas_library"], cas_table=spec["cas_table"],
        objects=objs, table_label=spec.get("table_label"))
    c.update_report_content(rid, content)
    spec["objects"] = objs
    spec_store.save_spec(rid, spec)
    details = c.get_report(rid)
    text = (f"Dashboard **{details['name']}** updated ({op}) — now {len(objs)} tiles.\n"
            f"Open in SAS VA: {details['url']}")
    try:
        return text, c.get_report_image(rid)
    except Exception as img_err:
        return text + f"\n(preview unavailable: {img_err})", None


def _w_rename_report(token, a):
    c = _va(token)
    rid = a["report_id"]
    c.rename_report(rid, a["new_name"])
    sp = spec_store.load_spec(rid)
    if sp:
        sp["report_name"] = a["new_name"]
        spec_store.save_spec(rid, sp)
    d = c.get_report(rid)
    return {"renamed": True, "id": rid, "name": d["name"], "url": d["url"]}


def _w_move_report(token, a):
    return _va(token).move_report(a["report_id"], a["parent_folder_uri"])


def _w_create_geo_map(token, a):
    c = _va(token)
    parent_uri = _resolve_parent(c, a.get("parent_folder_uri"))
    srv = a.get("cas_server", "cas-shared-default")
    map_type = (a.get("map_type") or "region").lower()
    try:
        real_cols = c.get_table_columns(a["cas_library"], a["cas_table"], srv)
    except Exception as col_err:
        return f"Could not read columns ({col_err}).", None
    by_lower = {str(col["name"]).lower() for col in real_cols}
    errs: list[str] = []

    def _need_numeric(col, role):
        nm = (col or "").lower()
        if not nm:
            errs.append(f"{role} is required for a {map_type} map")
        elif nm not in by_lower:
            errs.append(f"{role} '{col}' does not exist in the table")
        else:
            _, e2 = _validate_object_columns(real_cols, {"measure_column": col}, "kpi")
            errs.extend(e for e in e2 if "needs a column for" not in e)

    if map_type == "coordinate":
        lat, lon, size = a.get("latitude_column"), a.get("longitude_column"), a.get("size_column")
        _need_numeric(lat, "latitude_column"); _need_numeric(lon, "longitude_column"); _need_numeric(size, "size_column")
    else:
        geo, meas = a.get("geo_column"), a.get("measure_column")
        if not geo or geo.lower() not in by_lower:
            errs.append(f"geo_column '{geo}' does not exist in the table" if geo else "geo_column is required")
        _need_numeric(meas, "measure_column")
    if errs:
        avail = ", ".join(f"{col['name']} ({col.get('type', '?')})" for col in real_cols)
        return "Cannot create the geo map — " + "; ".join(errs) + f".\nAvailable: {avail}.", None
    if map_type == "coordinate":
        content = build_geo_coordinate(a["report_name"], srv, a["cas_library"], a["cas_table"],
                                       a["latitude_column"], a["longitude_column"], a["size_column"])
        what = f"bubbles sized by {a['size_column']}"
    else:
        content = build_geo_region(a["report_name"], srv, a["cas_library"], a["cas_table"],
                                   a["geo_column"], a["measure_column"], a.get("measure_label"))
        what = f"{a['measure_column']} by {a['geo_column']}"
    report = c.create_report(a["report_name"], parent_uri)
    report_id = report["id"]
    try:
        c.update_report_content(report_id, content)
    except Exception:
        try: c.delete_report(report_id)
        except Exception: pass
        raise
    details = c.get_report(report_id)
    text = (f"Geo map **{details.get('name', a['report_name'])}** created ({map_type}: {what}).\n"
            f"Report ID: `{report_id}`\nOpen in SAS VA: {details['url']}")
    try:
        return text, c.get_report_image(report_id)
    except Exception as img_err:
        return text + f"\n(preview unavailable: {img_err})", None


def _w_create_va_object(token, a):
    c = _va(token)
    parent_uri = _resolve_parent(c, a.get("parent_folder_uri"))
    object_type = a["object_type"]
    content = build_object_content(
        object_type=object_type, report_name=a["report_name"],
        cas_server=a.get("cas_server", "cas-shared-default"),
        cas_library=a.get("cas_library", _VA_OBJECT_DEFAULT_LIB),
        cas_table=a.get("cas_table", _VA_OBJECT_DEFAULT_TABLE),
        column_overrides=a.get("columns"), table_label=a.get("table_label"),
    )
    report = c.create_report(a["report_name"], parent_uri)
    report_id = report["id"]
    c.update_report_content(report_id, content)
    details = c.get_report(report_id)
    text = (f"VA object **{object_type}** created in report **{a['report_name']}**.\n"
            f"Report ID: `{report_id}`\nOpen in SAS VA: {details['url']}")
    try:
        return text, c.get_report_image(report_id)
    except Exception as img_err:
        return text + f"\n(Chart preview unavailable: {img_err})", None


def _w_list_reports(token, limit):
    return _va(token).list_reports(limit)


def _w_get_report(token, report_id):
    return _va(token).get_report(report_id)


def _w_delete_report(token, report_id):
    _va(token).delete_report(report_id)
    spec_store.delete_spec(report_id)
    return {"success": True, "deleted_report_id": report_id}


def _w_get_report_image(token, report_id, width, height):
    c = _va(token)
    png = c.get_report_image(report_id, width=width, height=height)
    details = c.get_report(report_id)
    return f"Chart for report: {details['name']}", png


def _w_upload_data(token, a, file_b64, filename):
    if not file_b64:
        return ("No file was attached. Ask the user to attach a CSV/Excel file, "
                "then call upload_data again.")
    try:
        raw = base64.b64decode(file_b64)
    except Exception as dec_err:
        return f"Could not decode attached file: {dec_err}"
    fname = filename or "upload.csv"
    ext = fname.rsplit(".", 1)[-1].lower() if "." in fname else "csv"
    if ext in ("xlsx", "xls"):
        file_type, save_type = "ExcelFile", ext
    else:
        file_type, save_type = "DelimitedFile", "csv"
    tbl = a.get("table_name") or fname.rsplit(".", 1)[0]
    return _va(token).upload_table(
        file_bytes=raw, table_name=tbl,
        caslib=a.get("caslib_name", "Public"),
        server=a.get("server_id", "cas-shared-default"),
        filename=fname, file_type=file_type, save_type=save_type,
        delimiter=a.get("delimiter", ","),
        contains_header_row=bool(a.get("has_header", True)),
    )


# ── Studio workers ─────────────────────────────────────────────────────────────

def _w_studio_set_session(token, sid):
    sid = (sid or "").strip()
    if not sid:
        return "ERROR: session_id is required"
    sc = _studio(token)
    try:
        ping = sc._http.get(f"{sc.base_url}/studio/sessions/{sid}/keepalive",
                            headers=sc._hdrs(), timeout=10.0)
        if ping.status_code != 200:
            return (f"ERROR: Session {sid!r} keepalive returned HTTP {ping.status_code} — "
                    "the session may have expired. Reload SAS Studio in your browser "
                    "and try again with the new session ID.")
    except Exception as e:
        return f"ERROR: Could not reach session {sid!r}: {e}"
    sc._studio_session_id = sid
    os.environ["SAS_STUDIO_SESSION_ID"] = sid
    return {"ok": True, "session_id": sid,
            "message": (f"Session {sid!r} is alive and will be used for all Studio "
                        "operations. You can now run SAS code and query flows.")}


def _w_studio_diagnose(token):
    return _studio(token).diagnose()


def _w_studio_run_code(token, a):
    sc = _studio(token)
    session_id = a.get("studio_session_id") or sc.get_or_create_session()
    return sc.run_code(session_id=session_id, code=a["code"],
                       label=a.get("label", "SAS Program.sas"),
                       max_wait=a.get("max_wait_seconds", 120))


def _w_studio_create_session(token):
    return {"studio_session_id": _studio(token).get_or_create_session()}


def _w_studio_run_query_flow(token, a):
    sc = _studio(token)
    in_lib = a["input_libref"].upper()
    in_tbl = a["input_table"].upper()
    out_lib = a.get("output_libref", "WORK").upper()
    out_tbl = a.get("output_table", "QUERY_OUT").upper()
    session_id = a.get("studio_session_id") or sc.get_or_create_session()
    max_wait = a.get("max_wait_seconds", 120)
    # Run the query as PROC SQL. The dataFlow "Query" step's JSON schema is
    # silently ignored on some SAS Viya builds (the flow "completes" but writes
    # no output table), so we generate the equivalent select / where / order-by
    # as SAS code, which every build runs identically.
    run = sc.run_query_via_sql(
        session_id=session_id, input_libref=in_lib, input_table=in_tbl,
        output_libref=out_lib, output_table=out_tbl,
        selected_columns=a.get("selected_columns") or None,
        sort_by=a.get("sort_by") or None, filters=a.get("filters") or None,
        max_wait=max_wait)
    submission_id = run["submission_id"]
    state = run["state"]
    html_result = None
    tdata: dict = {}
    table_errors: list[str] = []
    if "completed" in (state or "").lower():
        try:
            tdata = sc.get_table_rows(session_id, out_lib, out_tbl, limit=500)
            if tdata.get("columns"):
                html_result = sc.rows_to_html(tdata, title=f"{out_lib}.{out_tbl}")
            else:
                table_errors.append(
                    f"{out_lib}.{out_tbl}: query ran but the table has no columns")
        except Exception as te:
            table_errors.append(f"{out_lib}.{out_tbl}: {te}")
    else:
        table_errors.append(f"submission state was {state!r}, not completed")
    result = {
        "submission_id": submission_id, "studio_session_id": session_id,
        "state": state, "output_table": f"{out_lib}.{out_tbl}",
        "columns": tdata.get("columns", []), "row_count": tdata.get("total", 0),
        "html_result": html_result, "sql": run["sql"],
    }
    # On failure / empty result, surface the SAS log tail to aid debugging.
    if table_errors:
        result["table_fetch_errors"] = table_errors
        log = sc.get_log(session_id, submission_id)
        if log:
            result["log_tail"] = log[-3000:]
    return result


def _w_studio_get_submission_result(token, a):
    sc = _studio(token)
    session_id = a["studio_session_id"]
    submission_id = a["submission_id"]
    result: dict = {
        "submission_id": submission_id,
        "output_tables": sc.get_output_tables(session_id, submission_id),
        "html_result": sc.get_html_result(session_id, submission_id),
    }
    if a.get("include_log", False):
        result["log"] = sc.get_log(session_id, submission_id)
    return result


def _w_studio_list_libraries(token, studio_session_id):
    sc = _studio(token)
    session_id = studio_session_id or sc.get_or_create_session()
    return {"studio_session_id": session_id, "libraries": sc.list_libraries(session_id)}


def _w_studio_list_tables(token, libref, studio_session_id):
    sc = _studio(token)
    session_id = studio_session_id or sc.get_or_create_session()
    libref = libref.upper()
    return {"studio_session_id": session_id, "libref": libref,
            "tables": sc.list_tables(session_id, libref)}


def _w_studio_list_content_folders(token, folder_uri):
    c = _va(token)
    folder_uri = folder_uri or None
    folders: list[dict] = []
    if not folder_uri:
        _ALIASES = [
            ("@myFolder", "My Folder"),
            ("@myFavorites", "My Favorites"),
            ("@myFolderShortcuts", "Folder Shortcuts"),
            ("@content", "SAS Content"),
        ]
        for alias, default_name in _ALIASES:
            try:
                r = c._http.get(f"{c.base_url}/folders/folders/{alias}",
                                headers=c._hdrs(), timeout=10.0)
                if r.is_success:
                    d = r.json()
                    fid = d.get("id", "")
                    folders.append({
                        "name": d.get("name", default_name),
                        "contentType": d.get("contentType") or d.get("type", "folder"),
                        "uri": f"/folders/folders/{fid}" if fid else "",
                    })
                elif alias == "@content":
                    _seen: set[str] = set()
                    for _type_filter in ("folder", "userFolder"):
                        try:
                            r2 = c._http.get(
                                f"{c.base_url}/folders/folders"
                                f"?filter=eq(type,{_type_filter})&limit=50&sortBy=name",
                                headers=c._hdrs(), timeout=10.0)
                            if r2.is_success:
                                for item in r2.json().get("items", []):
                                    if not item.get("parentFolderUri"):
                                        fid = item.get("id", "")
                                        if fid and fid in _seen:
                                            continue
                                        if fid:
                                            _seen.add(fid)
                                        folders.append({
                                            "name": f"SAS Content / {item.get('name', '')}",
                                            "contentType": item.get("type", "folder"),
                                            "uri": f"/folders/folders/{fid}" if fid else "",
                                        })
                        except Exception:
                            pass
            except Exception:
                pass
    else:
        try:
            items = c.list_folders(folder_uri)
            folders = [
                {"name": i.get("name", ""), "contentType": i.get("type", ""),
                 "uri": i.get("uri") or (f"/folders/folders/{i['id']}" if i.get("id") else "")}
                for i in items
                if i.get("type") in ("folder", "myFolder", "userFolder",
                                     "favoritesFolder", "contentFolder")
                or i.get("uri", "").startswith("/folders/folders/")
            ]
        except Exception:
            folders = []
    return {"browsed_folder": folder_uri or "@top", "folders": folders, "folder_picker": True}


def _w_studio_create_program_flow(token, a):
    sc = _studio(token)
    session_id = a.get("studio_session_id") or sc.get_or_create_session()

    # Resolve / normalise the target folder. SAS's parentFolderUri needs a clean
    # '/folders/folders/{uuid}', so a 'sascontent:' prefix or an '@myFolder'
    # alias is resolved here; a plain folder NAME (or anything else unusable) is
    # treated as "not chosen" so we ask the user rather than guessing.
    folder_uri = (a.get("folder_uri") or "").strip()
    if folder_uri.startswith("sascontent:"):
        folder_uri = folder_uri.replace("sascontent:", "", 1)
    if folder_uri.startswith("@"):
        try:
            ar = sc._http.get(f"{sc.base_url}/folders/folders/{folder_uri}",
                              headers=sc._hdrs(), timeout=8.0)
            fid = ar.json().get("id", "") if ar.is_success else ""
            folder_uri = f"/folders/folders/{fid}" if fid else ""
        except Exception:
            folder_uri = ""
    if folder_uri and not folder_uri.startswith("/folders/folders/"):
        folder_uri = ""

    if not folder_uri:
        # Don't silently save to My Folder — return the folder choices + picker
        # signal and ask the user. The flow is saved on the follow-up call once
        # folder_uri is supplied.
        folders = _w_studio_list_content_folders(token, None).get("folders", [])
        return {
            "needs_folder_selection": True,
            "folder_picker": True,
            "folders": folders,
            "flow_name": a.get("flow_name", ""),
            "studio_session_id": session_id,
            "message": (
                "Where would you like to save the flow in SAS? Choose a destination "
                "folder from the list (e.g. My Folder), then I'll save it there. To "
                "confirm, call sas_studio_create_program_flow again with the chosen "
                "folder_uri (keeping the same flow_name and code)."),
        }

    # Resolve a human-readable folder name before saving.
    folder_name = "My Folder"
    try:
        fid = folder_uri.rstrip("/").split("/")[-1]
        fr = sc._http.get(f"{sc.base_url}/folders/folders/{fid}",
                          headers=sc._hdrs(), timeout=8.0)
        if fr.is_success:
            folder_name = fr.json().get("name", folder_name)
    except Exception:
        pass

    try:
        result = sc.save_sas_program_flow(
            name=a["flow_name"], code=a["code"], folder_uri=folder_uri,
            session_id=session_id, description=a.get("description", ""),
            overwrite=a.get("overwrite", True))
    except PermissionError as perm_exc:
        # SAS refused the chosen folder (403). Don't silently re-route to My
        # Folder — report exactly what SAS said and re-offer the picker.
        folders = _w_studio_list_content_folders(token, None).get("folders", [])
        return {
            "needs_folder_selection": True,
            "folder_picker": True,
            "save_denied": True,
            "denied_folder": folder_name,
            "denied_folder_uri": folder_uri,
            "denied_reason": str(perm_exc),
            "folders": folders,
            "flow_name": a.get("flow_name", ""),
            "studio_session_id": session_id,
            "message": (
                f"SAS would not let me save to '{folder_name}'. Reason: {perm_exc} "
                "Please pick a different destination folder, then call "
                "sas_studio_create_program_flow again with the new folder_uri "
                "(keep the same flow_name and code)."),
        }

    saved = result.get("saved")
    return {**result, "folder_name": folder_name, "studio_session_id": session_id,
            "message": (f"Flow '{result['name']}' saved to '{folder_name}' in SAS Drive. "
                        f"In SAS Studio: SAS Content → {folder_name} → {result['name']}")
            if saved else "Flow save may have failed — check id field."}


# ─────────────────────────────────────────────────────────────────────────────
# Middleware: strip WPIntelliChat's injected, non-schema tool arguments.
# ─────────────────────────────────────────────────────────────────────────────

_INJECTED_DISCARD = ("_sas_access_token", "_sas_base_url")


class InjectedArgsMiddleware(Middleware):
    """Remove host-injected ``_``-prefixed tool args before a tool runs.

    WPIntelliChat injects per-user/session context into ``sas_*`` tool calls as
    extra arguments — the Viya token (``_sas_access_token``/``_sas_base_url``,
    which we ignore because the token already arrives in the bearer header) and,
    for ``upload_data``, the attached file bytes (``_file_b64``/``_filename``).
    None of these belong in a tool's input schema, so they are stripped here and
    the upload bytes are stashed in FastMCP context state for ``upload_data`` to
    read. Scrubbing in middleware (before argument validation) keeps every tool
    signature clean.
    """

    async def on_call_tool(self, ctx: MiddlewareContext, call_next: Any) -> Any:
        args = getattr(ctx.message, "arguments", None)
        if isinstance(args, dict):
            for key in _INJECTED_DISCARD:
                args.pop(key, None)
            file_b64 = args.pop("_file_b64", None)
            filename = args.pop("_filename", None)
            fctx = ctx.fastmcp_context
            if fctx is not None:
                if file_b64 is not None:
                    await fctx.set_state("upload_file_b64", file_b64)
                if filename is not None:
                    await fctx.set_state("upload_filename", filename)
        return await call_next(ctx)


# ─────────────────────────────────────────────────────────────────────────────
# Registration
# ─────────────────────────────────────────────────────────────────────────────

def register_va_tools(
    mcp: FastMCP, get_token: Callable[[Context], Awaitable[str]]
) -> None:
    """Register every VA + Studio tool on *mcp*.

    *get_token* resolves the caller's Viya access token (HTTP: bearer-header
    swap; stdio: cached CLI token) — identical to the core tools' token source.
    """

    # The VA ``upload_data`` (host-attached file bytes) supersedes the core
    # server's csv-string ``upload_data`` — that is the upload path WPIntelliChat
    # drives. Remove the core one first so there is exactly one ``upload_data``.
    try:
        mcp.local_provider.remove_tool("upload_data")
    except Exception:
        pass

    def _err(exc: Exception) -> str:
        return f"ERROR: {type(exc).__name__}: {exc}"

    def _image(text: str, png: bytes | None):
        """Build a FastMCP return from a (text, png) chart worker result."""
        if png:
            return [text, Image(data=png, format="png")]
        return text

    # ── VA: data discovery ────────────────────────────────────────────────────

    @mcp.tool(
        name="sas_list_cas_libraries",
        description="List all CAS libraries (caslibs) available on a CAS server.",
    )
    async def sas_list_cas_libraries(
        ctx: Context, cas_server: str = "cas-shared-default"
    ) -> Any:
        try:
            return await asyncio.to_thread(_w_list_cas_libraries, await get_token(ctx), cas_server)
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_list_cas_tables",
        description="List all tables in a CAS library (e.g. library='Public').",
    )
    async def sas_list_cas_tables(
        library: str, ctx: Context, cas_server: str = "cas-shared-default"
    ) -> Any:
        try:
            return await asyncio.to_thread(_w_list_cas_tables, await get_token(ctx), library, cas_server)
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_get_table_columns",
        description=("Get the column names and types for a CAS table. Use this to discover "
                     "which columns can be used as category/measure/group roles in a chart."),
    )
    async def sas_get_table_columns(
        library: str, table: str, ctx: Context, cas_server: str = "cas-shared-default"
    ) -> Any:
        try:
            return await asyncio.to_thread(_w_get_table_columns, await get_token(ctx), library, table, cas_server)
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_list_folders",
        description=("List items inside a SAS Drive folder. Use '@myFolder' for your personal "
                     "home folder, or a full URI like '/folders/folders/<uuid>'."),
    )
    async def sas_list_folders(ctx: Context, folder_uri: str = "@myFolder") -> Any:
        try:
            return await asyncio.to_thread(_w_list_folders, await get_token(ctx), folder_uri)
        except Exception as e:
            return _err(e)

    # ── VA: report / chart / dashboard builders ───────────────────────────────

    @mcp.tool(
        name="sas_create_bar_chart_report",
        description=("Create a SAS Visual Analytics report containing a horizontal bar chart. "
                     "Shows Frequency (row count) by the category column; an optional group "
                     "column adds colour grouping. Returns the report id + URL (and an inline "
                     "preview when the render service is available)."),
    )
    async def sas_create_bar_chart_report(
        report_name: str, cas_library: str, cas_table: str, category_column: str, ctx: Context,
        parent_folder_uri: str = "@myFolder", cas_server: str = "cas-shared-default",
        category_label: str | None = None, group_column: str | None = None,
        group_label: str | None = None, table_label: str | None = None,
    ) -> Any:
        try:
            text, png = await asyncio.to_thread(_w_create_bar_chart, await get_token(ctx), {
                "report_name": report_name, "cas_library": cas_library, "cas_table": cas_table,
                "category_column": category_column, "parent_folder_uri": parent_folder_uri,
                "cas_server": cas_server, "category_label": category_label,
                "group_column": group_column, "group_label": group_label, "table_label": table_label})
            return _image(text, png)
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_create_chart_report",
        description=(
            "Create a SAS Visual Analytics report with one chart of the given chart_type.\n"
            "chart_type: bar_h | bar_v | bar_stacked_v | bar_stacked_h | line | pie | donut | "
            "scatter | bubble.\n"
            "Column roles: bar/pie → category_column (char) + optional measure_column (numeric, "
            "omit for Frequency) + optional group_column. line → x_column + y_column (numeric). "
            "scatter → x_column + y_column (numeric). bubble → x_column + y_column + size_column "
            "(numeric). Column roles are validated against the real table before building. "
            "Call sas_get_table_columns first to use real column names."),
    )
    async def sas_create_chart_report(
        chart_type: str, report_name: str, cas_library: str, cas_table: str, ctx: Context,
        parent_folder_uri: str = "@myFolder", cas_server: str = "cas-shared-default",
        category_column: str | None = None, category_label: str | None = None,
        measure_column: str | None = None, measure_label: str | None = None,
        x_column: str | None = None, x_label: str | None = None,
        y_column: str | None = None, y_label: str | None = None,
        size_column: str | None = None, size_label: str | None = None,
        group_column: str | None = None, group_label: str | None = None,
        table_label: str | None = None,
    ) -> Any:
        try:
            text, png = await asyncio.to_thread(_w_create_chart, await get_token(ctx), {
                "chart_type": chart_type, "report_name": report_name, "cas_library": cas_library,
                "cas_table": cas_table, "parent_folder_uri": parent_folder_uri, "cas_server": cas_server,
                "category_column": category_column, "category_label": category_label,
                "measure_column": measure_column, "measure_label": measure_label,
                "x_column": x_column, "x_label": x_label, "y_column": y_column, "y_label": y_label,
                "size_column": size_column, "size_label": size_label, "group_column": group_column,
                "group_label": group_label, "table_label": table_label})
            return _image(text, png)
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_create_dashboard_report",
        description=(
            "Create a SAS Visual Analytics DASHBOARD — ONE report with multiple objects (KPI "
            "tiles AND charts) laid out on a single page (or several pages via `pages`). Use "
            "this whenever the user wants more than one visual, KPIs/metrics, or a 'dashboard'.\n"
            "All objects read the SAME CAS table. Pass `objects` as an ordered list; each item "
            "is one tile keyed by `type`:\n"
            "  kpi: {type:'kpi', measure_column}\n"
            "  bar: {type:'bar_h'|'bar_v', category_column, measure_column?, group_column?}\n"
            "  stacked: {type:'bar_stacked_v'|'bar_stacked_h', category_column, group_column(req), measure_column?}\n"
            "  line: {type:'line', x_column, y_column}\n"
            "  pie/donut: {type:'pie'|'donut', category_column, measure_column?}\n"
            "  scatter: {type:'scatter', x_column, y_column}; bubble: {+size_column}\n"
            "  treemap/heat_map/histogram/box_plot/waterfall/word_cloud/targeted_bar/crosstab/"
            "button_bar/list_control — see sas_list_va_object_types for their roles.\n"
            "Any measure tile also accepts aggregation (sum/average/min/max/median) and "
            "measure_format (currency/comma/percent or a raw SAS format), or a calculated ratio "
            "via calc_numerator + calc_denominator (+ calc_op). For tabs, pass `pages` (a list "
            "of pages, each a list of tiles) instead of `objects`. Columns are validated against "
            "the real table first — call sas_get_table_columns to use real names."),
    )
    async def sas_create_dashboard_report(
        report_name: str, cas_library: str, cas_table: str, ctx: Context,
        parent_folder_uri: str = "@myFolder", cas_server: str = "cas-shared-default",
        table_label: str | None = None,
        objects: list[dict] | None = None, pages: list[list[dict]] | None = None,
    ) -> Any:
        try:
            text, png = await asyncio.to_thread(_w_create_dashboard, await get_token(ctx), {
                "report_name": report_name, "cas_library": cas_library, "cas_table": cas_table,
                "parent_folder_uri": parent_folder_uri, "cas_server": cas_server,
                "table_label": table_label, "objects": objects, "pages": pages})
            return _image(text, png)
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_edit_dashboard",
        description=("EDIT a dashboard created with sas_create_dashboard_report — add / remove / "
                     "replace a tile without rebuilding it (keeps the same id/URL). operation: "
                     "'add' (append `object`), 'remove' (drop tile at `index`), 'replace' (swap "
                     "tile at `index` with `object`). Only works on dashboards this server made."),
    )
    async def sas_edit_dashboard(
        report_id: str, operation: str, ctx: Context,
        object: dict | None = None, index: int | None = None,
    ) -> Any:
        try:
            text, png = await asyncio.to_thread(_w_edit_dashboard, await get_token(ctx), {
                "report_id": report_id, "operation": operation, "object": object, "index": index})
            return _image(text, png)
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_rename_report",
        description="Rename an existing VA report or dashboard (keeps its id/URL and content).",
    )
    async def sas_rename_report(report_id: str, new_name: str, ctx: Context) -> Any:
        try:
            return await asyncio.to_thread(_w_rename_report, await get_token(ctx),
                                           {"report_id": report_id, "new_name": new_name})
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_move_report",
        description="Move a VA report into a different SAS Drive folder (by folder URI).",
    )
    async def sas_move_report(report_id: str, parent_folder_uri: str, ctx: Context) -> Any:
        try:
            return await asyncio.to_thread(_w_move_report, await get_token(ctx),
                                           {"report_id": report_id, "parent_folder_uri": parent_folder_uri})
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_create_geo_map",
        description=("Create a GEOGRAPHIC MAP report. map_type='region' (default) colours US "
                     "states by a measure — needs geo_column (US state names) + measure_column "
                     "(numeric). map_type='coordinate' plots lat/long bubbles sized by a measure "
                     "— needs latitude_column + longitude_column + size_column (all numeric). "
                     "Call sas_get_table_columns first to pick real columns."),
    )
    async def sas_create_geo_map(
        report_name: str, cas_library: str, cas_table: str, ctx: Context,
        parent_folder_uri: str = "@myFolder", cas_server: str = "cas-shared-default",
        map_type: str = "region", geo_column: str | None = None,
        measure_column: str | None = None, measure_label: str | None = None,
        latitude_column: str | None = None, longitude_column: str | None = None,
        size_column: str | None = None,
    ) -> Any:
        try:
            text, png = await asyncio.to_thread(_w_create_geo_map, await get_token(ctx), {
                "report_name": report_name, "cas_library": cas_library, "cas_table": cas_table,
                "parent_folder_uri": parent_folder_uri, "cas_server": cas_server, "map_type": map_type,
                "geo_column": geo_column, "measure_column": measure_column, "measure_label": measure_label,
                "latitude_column": latitude_column, "longitude_column": longitude_column,
                "size_column": size_column})
            return _image(text, png)
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_list_va_object_types",
        description=("List every SAS Visual Analytics object type this server can create, with "
                     "each type's configurable column roles and the sample column each role "
                     "defaults to. Call this first to discover valid object_type values and the "
                     "column role names accepted by sas_create_va_object.\n\n"
                     "Available object types and their column roles:\n" + _object_catalog_text()),
    )
    async def sas_list_va_object_types(ctx: Context) -> Any:
        return _VA_OBJECTS

    @mcp.tool(
        name="sas_create_va_object",
        description=(
            "Create a SAS Visual Analytics report containing ONE VA object of the requested type, "
            "reproduced from a real captured SAS VA report so it renders exactly as SAS VA builds "
            "it.\n\nSupported object_type values and their column roles (pass matching keys in "
            "`columns`):\n" + _object_catalog_text() +
            "\n\nEvery role defaults to a column on the sample table "
            f"{_VA_OBJECT_DEFAULT_LIB}.{_VA_OBJECT_DEFAULT_TABLE}, so object_type + report_name "
            "alone reproduces a known-good object. To use a DIFFERENT table, set cas_library/"
            "cas_table AND supply a `columns` mapping for every role (use sas_get_table_columns). "
            "Container types take no columns."),
    )
    async def sas_create_va_object(
        object_type: str, report_name: str, ctx: Context,
        parent_folder_uri: str = "@myFolder", cas_server: str = "cas-shared-default",
        cas_library: str = _VA_OBJECT_DEFAULT_LIB, cas_table: str = _VA_OBJECT_DEFAULT_TABLE,
        columns: dict[str, str] | None = None, table_label: str | None = None,
    ) -> Any:
        try:
            text, png = await asyncio.to_thread(_w_create_va_object, await get_token(ctx), {
                "object_type": object_type, "report_name": report_name,
                "parent_folder_uri": parent_folder_uri, "cas_server": cas_server,
                "cas_library": cas_library, "cas_table": cas_table,
                "columns": columns, "table_label": table_label})
            return _image(text, png)
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_list_reports",
        description="List recent VA reports (up to `limit`, most recently modified first).",
    )
    async def sas_list_reports(ctx: Context, limit: int = 50) -> Any:
        try:
            return await asyncio.to_thread(_w_list_reports, await get_token(ctx), limit)
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_get_report",
        description="Get details and the direct URL for a VA report by its ID.",
    )
    async def sas_get_report(report_id: str, ctx: Context) -> Any:
        try:
            return await asyncio.to_thread(_w_get_report, await get_token(ctx), report_id)
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_delete_report",
        description="Delete a VA report by its ID. This action is irreversible.",
    )
    async def sas_delete_report(report_id: str, ctx: Context) -> Any:
        try:
            return await asyncio.to_thread(_w_delete_report, await get_token(ctx), report_id)
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_get_report_image",
        description=("Render a VA report as an image and return it so it can be displayed in chat. "
                     "Note: this deployment's report-image render service may be unavailable."),
    )
    async def sas_get_report_image(
        report_id: str, ctx: Context, width: int = 900, height: int = 550
    ) -> Any:
        try:
            text, png = await asyncio.to_thread(_w_get_report_image, await get_token(ctx),
                                                report_id, width, height)
            return _image(text, png)
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="upload_data",
        description=(
            "Upload an ATTACHED data file (CSV / delimited text, or Excel) into a CAS library as "
            "a table so it can be charted and analysed. The file's bytes are supplied "
            "automatically by the host — do NOT paste file contents yourself; only choose the "
            "destination (caslib + table name). On a name clash the table name is auto-uniquified."),
    )
    async def upload_data(
        table_name: str, ctx: Context, caslib_name: str = "Public",
        server_id: str = "cas-shared-default", delimiter: str = ",", has_header: bool = True,
    ) -> Any:
        try:
            file_b64 = await ctx.get_state("upload_file_b64")
            filename = await ctx.get_state("upload_filename")
            return await asyncio.to_thread(
                _w_upload_data, await get_token(ctx),
                {"table_name": table_name, "caslib_name": caslib_name, "server_id": server_id,
                 "delimiter": delimiter, "has_header": has_header},
                file_b64, filename)
        except Exception as e:
            return _err(e)

    # ── SAS Studio / dataFlows ────────────────────────────────────────────────

    @mcp.tool(
        name="sas_studio_set_session",
        description=("Update the SAS Studio session ID used for all Studio operations. Call this "
                     "when session creation fails with zone errors. Find the session ID in SAS "
                     "Studio: F12 DevTools → Network → filter 'keepalive' → the UUID in the URL "
                     "between /studio/sessions/ and /keepalive."),
    )
    async def sas_studio_set_session(session_id: str, ctx: Context) -> Any:
        try:
            return await asyncio.to_thread(_w_studio_set_session, await get_token(ctx), session_id)
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_studio_diagnose",
        description=("Run a step-by-step connectivity check for SAS Studio and return a detailed "
                     "status report (token auth, session creation, code submission, result "
                     "retrieval, with raw HTTP status/body). Call this FIRST when a sas_studio_* "
                     "tool fails."),
    )
    async def sas_studio_diagnose(ctx: Context) -> Any:
        try:
            return await asyncio.to_thread(_w_studio_diagnose, await get_token(ctx))
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_studio_run_code",
        description=("Execute arbitrary SAS code in SAS Studio and return the ODS HTML output. "
                     "Supports DATA steps, PROC PRINT/MEANS/FREQ/SGPLOT/SQL, macro code, etc. The "
                     "html_result field is full ODS HTML5 and is rendered inline in the chat UI. "
                     "Reuses studio_session_id if given, else creates a session."),
    )
    async def sas_studio_run_code(
        code: str, ctx: Context, label: str = "SAS Program.sas",
        studio_session_id: str | None = None, max_wait_seconds: int = 120,
    ) -> Any:
        try:
            return await asyncio.to_thread(_w_studio_run_code, await get_token(ctx), {
                "code": code, "label": label, "studio_session_id": studio_session_id,
                "max_wait_seconds": max_wait_seconds})
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_studio_create_session",
        description=("Create a new SAS Studio session and return its session ID for reuse by the "
                     "other sas_studio_* tools."),
    )
    async def sas_studio_create_session(ctx: Context) -> Any:
        try:
            return await asyncio.to_thread(_w_studio_create_session, await get_token(ctx))
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_studio_run_query_flow",
        description=(
            "Run a SAS Studio Query flow (Data Explorer style) on an existing SAS library table "
            "and show the result as an inline table in WP Copilot chat.\n\n"
            "IMPORTANT — what this tool does and does NOT do:\n"
            "  ✅ Runs the query and shows the result table inline in chat.\n"
            "  ✅ Writes the filtered/sorted result to a WORK output table.\n"
            "  ❌ Does NOT save a .flw flow file to SAS Drive / SAS Studio file browser.\n"
            "     → To also save a persistent .flw file, call sas_studio_create_program_flow.\n\n"
            "Use this tool when the user asks to 'query', 'filter', 'select columns from', "
            "or 'show data from' a SAS table. If they also ask to 'save the flow' or 'create a "
            "reusable flow', call sas_studio_create_program_flow after this.\n\n"
            "Filter operators: equals, notequals, lessthan, greaterthan, "
            "lessthanorequals, greaterthanorequals, contains, startswith."),
    )
    async def sas_studio_run_query_flow(
        input_libref: str, input_table: str, ctx: Context,
        output_table: str = "QUERY_OUT", output_libref: str = "WORK",
        selected_columns: list[str] | None = None, sort_by: list[dict] | None = None,
        filters: list[dict] | None = None, studio_session_id: str | None = None,
        max_wait_seconds: int = 120,
    ) -> Any:
        try:
            return await asyncio.to_thread(_w_studio_run_query_flow, await get_token(ctx), {
                "input_libref": input_libref, "input_table": input_table,
                "output_table": output_table, "output_libref": output_libref,
                "selected_columns": selected_columns, "sort_by": sort_by, "filters": filters,
                "studio_session_id": studio_session_id, "max_wait_seconds": max_wait_seconds})
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_studio_get_submission_result",
        description=("Retrieve the ODS HTML output and SAS log for a previously submitted Studio "
                     "flow/code, given studio_session_id + submission_id."),
    )
    async def sas_studio_get_submission_result(
        studio_session_id: str, submission_id: str, ctx: Context, include_log: bool = False
    ) -> Any:
        try:
            return await asyncio.to_thread(_w_studio_get_submission_result, await get_token(ctx), {
                "studio_session_id": studio_session_id, "submission_id": submission_id,
                "include_log": include_log})
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_studio_list_libraries",
        description=("List all SAS libraries available in a Studio session (names, engine types, "
                     "whether they have tables). Creates a session if studio_session_id omitted."),
    )
    async def sas_studio_list_libraries(ctx: Context, studio_session_id: str | None = None) -> Any:
        try:
            return await asyncio.to_thread(_w_studio_list_libraries, await get_token(ctx), studio_session_id)
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_studio_list_tables",
        description=("List all tables in a SAS Studio library. Use sas_studio_list_libraries "
                     "first to discover library names."),
    )
    async def sas_studio_list_tables(
        libref: str, ctx: Context, studio_session_id: str | None = None
    ) -> Any:
        try:
            return await asyncio.to_thread(_w_studio_list_tables, await get_token(ctx), libref, studio_session_id)
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_studio_create_program_flow",
        description=(
            "Create and permanently save a SAS Program node flow (.flw file) to the SAS content "
            "server, making it visible in SAS Studio's file browser. The flow embeds SAS code "
            "inside a SAS Program node — the same as dragging a 'SAS Program' step onto the "
            "Studio canvas and saving the flow.\n\n"
            "WHERE TO SAVE — the tool asks first: if you call it WITHOUT folder_uri, it does "
            "NOT save; instead it returns the list of available destination folders "
            "(needs_folder_selection=true, folder_picker=true) so the user can choose. Present "
            "those folders, let the user pick one, then call this tool AGAIN with the chosen "
            "folder_uri (and the same flow_name and code) to actually save.\n\n"
            "If SAS refuses the chosen folder (no write permission), the tool returns "
            "save_denied=true with SAS's reason and the folder list again — report the error "
            "and ask the user to pick a DIFFERENT folder, then retry. It never silently saves "
            "somewhere the user didn't choose."),
    )
    async def sas_studio_create_program_flow(
        flow_name: str, code: str, ctx: Context, folder_uri: str | None = None,
        description: str = "", overwrite: bool = True, studio_session_id: str | None = None,
    ) -> Any:
        try:
            return await asyncio.to_thread(_w_studio_create_program_flow, await get_token(ctx), {
                "flow_name": flow_name, "code": code, "folder_uri": folder_uri,
                "description": description, "overwrite": overwrite,
                "studio_session_id": studio_session_id})
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_studio_list_content_folders",
        description=("List folders in the SAS content server (SAS Drive) so the user can pick "
                     "where to save a flow. Call without folder_uri to list the top-level SAS "
                     "Content panel folders (My Folder, My Favorites, Folder Shortcuts, SAS "
                     "Content). Pass a folder_uri ('/folders/folders/{uuid}') to browse into it."),
    )
    async def sas_studio_list_content_folders(ctx: Context, folder_uri: str | None = None) -> Any:
        try:
            return await asyncio.to_thread(_w_studio_list_content_folders, await get_token(ctx), folder_uri)
        except Exception as e:
            return _err(e)
