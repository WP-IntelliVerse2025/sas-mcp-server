# Copyright © 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""SAS Intelligent Decisioning tools for the SAS Viya MCP server.

These tools wrap the SAS Viya **Intelligent Decisioning** REST APIs — the
services behind *Build Decisions* / *Manage Business Rules* (captured in
``ID_har.har``, whose dominant activity was building a decision flow with a
rule set and Python/SQL code nodes):

  * **Decision flows**  ``/decisions/flows``                — the decision assets
  * **Rule sets + rules** ``/businessRules/ruleSets`` ``…/rules`` — business rules
  * **Code files**      ``/decisions/codeFiles`` (+ ``/files/files``) — SQL/Python nodes

They cover the create / read / delete lifecycle for each asset type plus rule
authoring, so the assistant can list and inspect existing decisions, rule sets
and code files, and create new ones. (Wiring nodes together into a runnable
decision flow — the large step/mapping assembly — is intentionally left as a
follow-up; these build the decision and its building blocks.)

Auth model is identical to the other tool modules: every tool resolves the
caller's Viya access token via ``get_token(ctx)`` and calls the services **as
that user**, honoring their folder permissions.
"""

from __future__ import annotations

import re
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any

import httpx
from fastmcp import Context, FastMCP

from .config import VIYA_ENDPOINT
from .viya_client import get_json, get_paged_items, logger, make_client

# Intelligent Decisioning vendor media types (these endpoints reject plain
# ``application/json`` on the typed resources — they require the exact type).
_DEC = "application/vnd.sas.decision+json"
_RS = "application/vnd.sas.business.rule.set+json"
_RS_INTEGRAL = "application/vnd.sas.business.rule.set.integral+json"
_CF = "application/vnd.sas.decision.code.file+json"
_COLLECTION = "application/vnd.sas.collection+json"
_VALIDATION_REQ = "application/vnd.sas.decision.expression.validation.request+json"
_VALIDATION_RESP = "application/vnd.sas.validation+json"
_PUBLISH_REQ = "application/vnd.sas.models.publishing.request.asynchronous+json"
_DS2 = "text/vnd.sas.source.ds2"

_VALID_DATATYPES = ("string", "integer", "decimal", "double", "date", "datetime", "boolean", "dataGrid")
_VALID_DIRECTIONS = ("input", "output", "inOut")


def _trim(item: dict[str, Any]) -> dict[str, Any]:
    """Drop the noisy HATEOAS ``links`` array from a returned object."""
    return {k: v for k, v in item.items() if k != "links"}


def _sig_vars(sig: list[dict] | None) -> list[dict[str, Any]]:
    """Compact view of a signature (the decision/rule-set variables)."""
    out = []
    for t in sig or []:
        out.append({k: t[k] for k in ("name", "dataType", "direction", "length") if k in t})
    return out


def _q(value: str) -> str:
    return value.replace("'", "''")


def _sanitize_ident(name: str) -> str:
    """A DS2/MAS-safe model name: letters/digits/underscore, not starting with a digit."""
    s = re.sub(r"[^A-Za-z0-9_]", "_", name or "") or "model"
    if s[0].isdigit():
        s = "m_" + s
    return s


def _dm_url(kind: str, asset_id: str) -> str:
    """Deep-link that opens an asset in SAS Decision Manager.

    ``kind`` is the Decision Manager route: 'decisions', 'rules', or 'codeFiles'
    (captured from the UI Referer headers in ID_har.har).
    """
    return f"{VIYA_ENDPOINT}/SASDecisionManager/{kind}/{asset_id}"


def _flow_outline(steps: list[dict] | None, indent: int = 0) -> list[str]:
    """A readable, nested text outline of a decision flow's steps (a chat preview).

    Decisioning has no image-render service (unlike VA), so the flow is shown as
    an indented outline — code nodes, rule-set nodes, and if/then/else branches.
    """
    out: list[str] = []
    pad = "  " * indent
    for s in steps or []:
        t = s.get("type", "")
        if "custom.object" in t:
            co = s.get("customObject", {}) or {}
            out.append(f"{pad}• Code node: {co.get('name')} ({co.get('type')})")
        elif "ruleset" in t:
            rs = s.get("ruleset", {}) or {}
            out.append(f"{pad}• Rule set: {rs.get('name')}")
        elif "condition" in t:
            out.append(f"{pad}• If {_cond_label(s)}:")
            on_true = (s.get("onTrue") or {}).get("steps")
            on_false = (s.get("onFalse") or {}).get("steps")
            if on_true:
                out.append(f"{pad}    then:")
                out += _flow_outline(on_true, indent + 3)
            if on_false:
                out.append(f"{pad}    else:")
                out += _flow_outline(on_false, indent + 3)
        else:
            out.append(f"{pad}• {(t.split('.')[-1] or 'step')}")
    return out


def _cond_label(step: dict) -> str:
    """Human label for a condition step, across the shapes SAS uses for it."""
    cond = step.get("condition", {}) or {}
    lhs = (cond.get("lhsTerm") or {}).get("name")
    op = cond.get("operator")
    rhs = cond.get("rhsConstant")
    if rhs is None and cond.get("rhsTerm"):
        rhs = (cond.get("rhsTerm") or {}).get("name")
    if lhs and op is not None:
        return f"{lhs} {op} {rhs}"
    term = (cond.get("term") or {}).get("name")
    expr = cond.get("expression")
    if term and expr:
        return f"{term} {expr}"
    if expr:
        return str(expr)
    return step.get("name") or "condition"


def _mm_label(text: Any) -> str:
    """Sanitise a label for a Mermaid node (drop chars that break the parser)."""
    s = re.sub(r'["\[\]{}|<>()]', "", str(text)).strip()
    return (s[:38] + "…") if len(s) > 38 else (s or "step")


def _flow_mermaid(steps: list[dict] | None) -> str | None:
    """Best-effort Mermaid flowchart of a decision flow's steps (``None`` if empty).

    Code nodes render as rectangles, rule-set nodes as stadiums, and condition
    steps as diamonds with then/else branches — so a chat UI that supports Mermaid
    can draw the decision flow as a diagram.
    """
    if not steps:
        return None
    lines = ["flowchart TD", "  start([Start])"]
    ctr = {"n": 0}

    def nid() -> str:
        ctr["n"] += 1
        return f"n{ctr['n']}"

    def walk(steps: list[dict] | None, prev: str, first_label: str | None = None) -> str:
        label = first_label
        for s in steps or []:
            t = s.get("type", "")
            i = nid()
            edge = f"  {prev} -->|{label}| {i}" if label else f"  {prev} --> {i}"
            label = None
            if "custom.object" in t:
                lines.append(f'  {i}["{_mm_label((s.get("customObject") or {}).get("name", "code"))}"]')
                lines.append(edge)
                prev = i
            elif "ruleset" in t:
                lines.append(f'  {i}(["{_mm_label((s.get("ruleset") or {}).get("name", "ruleset"))}"])')
                lines.append(edge)
                prev = i
            elif "condition" in t:
                lines.append(f'  {i}{{"{_mm_label(_cond_label(s))}"}}')
                lines.append(edge)
                walk((s.get("onTrue") or {}).get("steps"), i, "then")
                walk((s.get("onFalse") or {}).get("steps"), i, "else")
                prev = i
            else:
                lines.append(f'  {i}["{_mm_label(t.split(".")[-1] or "step")}"]')
                lines.append(edge)
                prev = i
        return prev

    last = walk(steps, "start")
    lines.append(f"  {last} --> done([End])")
    return "\n".join(lines)


def register_decision_tools(
    mcp: FastMCP, get_token: Callable[[Context], Awaitable[str]]
) -> None:
    """Register every SAS Intelligent Decisioning tool on *mcp*."""

    @asynccontextmanager
    async def session(name: str, ctx: Context) -> AsyncIterator[httpx.AsyncClient]:
        logger.info("--- TOOL USED: %s ---", name)
        token = await get_token(ctx)
        async with make_client(token) as client:
            yield client

    def _err(exc: Exception) -> dict[str, Any]:
        if isinstance(exc, httpx.HTTPStatusError):
            code = exc.response.status_code
            body = (exc.response.text or "")[:300]
            if code == 403:
                return {"status": "forbidden", "httpStatusCode": 403,
                        "message": "The Intelligent Decisioning service denied this "
                                   "request (HTTP 403). You may lack write access to "
                                   "the target folder."}
            return {"status": "error", "httpStatusCode": code, "message": body}
        return {"status": "error", "message": f"{type(exc).__name__}: {exc}"}

    # ── Decision flows ─────────────────────────────────────────────────────────

    @mcp.tool(
        name="sas_list_decisions",
        description=(
            "List SAS Intelligent Decisioning decision flows (the decisions shown in "
            "Build Decisions). Optionally filter by `name_contains`. Returns each "
            "decision's id, name, description, owner, last-modified time, and a `url` "
            "that opens it in SAS Decision Manager."
        ),
    )
    async def sas_list_decisions(
        ctx: Context, name_contains: str | None = None, limit: int = 50, start: int = 0
    ) -> Any:
        filters = f"contains(name,'{_q(name_contains)}')" if name_contains else None
        try:
            async with session("sas_list_decisions", ctx) as client:
                items, count = await get_paged_items(
                    "/decisions/flows", client, limit=limit, start=start, filters=filters,
                    extra_params={"sortBy": "modifiedTimeStamp:descending"})
                return {"count": count, "decisions": [
                    {**{k: it.get(k) for k in ("id", "name", "description", "createdBy",
                                               "modifiedTimeStamp", "majorRevision")},
                     "url": _dm_url("decisions", it.get("id", ""))}
                    for it in items]}
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_get_decision",
        description=(
            "Get one SAS decision flow by id: its name, description, signature "
            "(the decision's input/output variables), a `flow` outline of its "
            "steps/nodes (a readable text preview of the flow), a `url` that opens it "
            "in SAS Decision Manager, and `flow_diagram` (a Mermaid flowchart).\n"
            "Lead with the `flow` outline and the `url`. `flow_diagram` renders as an "
            "actual diagram only in Mermaid-capable clients; include it (in a "
            "```mermaid block) only if the user explicitly asks for a diagram."
        ),
    )
    async def sas_get_decision(decision_id: str, ctx: Context) -> Any:
        try:
            async with session("sas_get_decision", ctx) as client:
                try:
                    d = await get_json(f"/decisions/flows/{decision_id}", client, accept=_DEC)
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 404:
                        return {"status": "not_found", "decision_id": decision_id}
                    raise
                steps = (d.get("flow") or {}).get("steps") or []
                outline = _flow_outline(steps)
                result = {"id": d.get("id"), "name": d.get("name"),
                          "description": d.get("description"),
                          "signature": _sig_vars(d.get("signature")),
                          "stepCount": len(steps),
                          "flow": outline or ["(empty — no steps yet)"],
                          "createdBy": d.get("createdBy"),
                          "modifiedTimeStamp": d.get("modifiedTimeStamp"),
                          "url": _dm_url("decisions", d.get("id", decision_id))}
                diagram = _flow_mermaid(steps)
                if diagram:
                    result["flow_diagram"] = diagram
                return result
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_create_decision",
        description=(
            "Create a new (empty) SAS Intelligent Decisioning decision flow with the "
            "given name. Returns its id and a `url` that opens it in SAS Decision "
            "Manager (Build Decisions) — ALWAYS share that url with the user so they "
            "can open the flow. Use sas_create_rule_set / sas_create_code_file to "
            "build the nodes that go into it."
        ),
    )
    async def sas_create_decision(
        name: str, ctx: Context, description: str = ""
    ) -> Any:
        try:
            async with session("sas_create_decision", ctx) as client:
                r = await client.post(
                    f"{VIYA_ENDPOINT}/decisions/flows",
                    json={"assetType": "decision", "name": name, "description": description},
                    headers={"Content-Type": _DEC, "Accept": _DEC})
                r.raise_for_status()
                d = r.json()
                return {"status": "created", "id": d.get("id"), "name": d.get("name"),
                        "description": d.get("description"),
                        "url": _dm_url("decisions", d.get("id", ""))}
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_delete_decision",
        description="Delete a SAS decision flow by its id. This is irreversible.",
    )
    async def sas_delete_decision(decision_id: str, ctx: Context) -> Any:
        try:
            async with session("sas_delete_decision", ctx) as client:
                r = await client.delete(f"{VIYA_ENDPOINT}/decisions/flows/{decision_id}")
                if r.status_code == 404:
                    return {"status": "not_found", "decision_id": decision_id}
                r.raise_for_status()
                return {"status": "deleted", "decision_id": decision_id}
        except Exception as e:
            return _err(e)

    # ── Rule sets + rules ──────────────────────────────────────────────────────

    @mcp.tool(
        name="sas_list_rule_sets",
        description=(
            "List SAS business rule sets (Manage Business Rules). Optionally filter by "
            "`name_contains`. Returns each rule set's id, name, ruleSetType and owner."
        ),
    )
    async def sas_list_rule_sets(
        ctx: Context, name_contains: str | None = None, limit: int = 50, start: int = 0
    ) -> Any:
        filters = f"contains(name,'{_q(name_contains)}')" if name_contains else None
        try:
            async with session("sas_list_rule_sets", ctx) as client:
                items, count = await get_paged_items(
                    "/businessRules/ruleSets", client, limit=limit, start=start, filters=filters,
                    extra_params={"sortBy": "modifiedTimeStamp:descending"})
                return {"count": count, "rule_sets": [
                    {**{k: it.get(k) for k in ("id", "name", "ruleSetType", "description",
                                               "createdBy", "modifiedTimeStamp")},
                     "url": _dm_url("rules", it.get("id", ""))}
                    for it in items]}
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_get_rule_set",
        description=(
            "Get one SAS business rule set by id: its name, type, signature variables, "
            "and the list of rules it contains (each rule's conditions and actions)."
        ),
    )
    async def sas_get_rule_set(rule_set_id: str, ctx: Context) -> Any:
        try:
            async with session("sas_get_rule_set", ctx) as client:
                try:
                    rs = await get_json(f"/businessRules/ruleSets/{rule_set_id}", client,
                                        accept=_RS_INTEGRAL)
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 404:
                        return {"status": "not_found", "rule_set_id": rule_set_id}
                    raise
                rules_summary = []
                try:
                    items, _ = await get_paged_items(
                        f"/businessRules/ruleSets/{rule_set_id}/rules", client, limit=200)
                    for r in items:
                        rules_summary.append({
                            "name": r.get("name"),
                            "conditional": r.get("conditional"),
                            "conditions": [f"{(c.get('term') or {}).get('name')} {c.get('expression')}"
                                           for c in (r.get("conditions") or [])],
                            "actions": [f"{(a.get('term') or {}).get('name')} = {a.get('expression')}"
                                        for a in (r.get("actions") or [])]})
                except httpx.HTTPError:
                    pass
                return {"id": rs.get("id"), "name": rs.get("name"),
                        "ruleSetType": rs.get("ruleSetType"),
                        "signature": _sig_vars(rs.get("signature")),
                        "ruleCount": len(rules_summary), "rules": rules_summary,
                        "url": _dm_url("rules", rs.get("id", rule_set_id))}
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_create_rule_set",
        description=(
            "Create a new SAS business rule set. `ruleSetType` is usually 'assignment'. "
            "`variables` defines the rule set's signature — a list of "
            "{name, dataType, direction} where dataType is string/integer/decimal/"
            "boolean/date and direction is input/output/inOut (default inOut). These "
            "variables are what rules then reference. Returns the rule set id; use "
            "sas_add_rule to add rules to it."
        ),
    )
    async def sas_create_rule_set(
        name: str, ctx: Context, description: str = "",
        rule_set_type: str = "assignment", variables: list[dict] | None = None,
        folder_path: str = "/My Folder",
    ) -> Any:
        signature = []
        for v in variables or []:
            dt = str(v.get("dataType", "string"))
            direction = str(v.get("direction", "inOut"))
            if dt not in _VALID_DATATYPES:
                return {"status": "invalid", "message": f"dataType '{dt}' must be one of {_VALID_DATATYPES}."}
            if direction not in _VALID_DIRECTIONS:
                return {"status": "invalid", "message": f"direction '{direction}' must be one of {_VALID_DIRECTIONS}."}
            term = {"name": v["name"], "dataType": dt, "direction": direction}
            if v.get("length"):
                term["length"] = v["length"]
            signature.append(term)
        body = {"name": name, "description": description, "ruleSetType": rule_set_type,
                "folderPath": folder_path, "signature": signature,
                "majorRevision": 0, "minorRevision": 0}
        try:
            async with session("sas_create_rule_set", ctx) as client:
                r = await client.post(f"{VIYA_ENDPOINT}/businessRules/ruleSets",
                                      json=body, headers={"Content-Type": _RS, "Accept": _RS})
                r.raise_for_status()
                rs = r.json()
                return {"status": "created", "id": rs.get("id"), "name": rs.get("name"),
                        "ruleSetType": rs.get("ruleSetType"),
                        "variables": [v["name"] for v in signature],
                        "url": _dm_url("rules", rs.get("id", ""))}
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_add_rule",
        description=(
            "Add a rule to a SAS business rule set. The rule's conditions and actions "
            "reference the rule set's signature variables (see sas_get_rule_set / "
            "sas_create_rule_set).\n"
            "  conditions: list of {variable, expression} — e.g. {'variable':'Age', "
            "'expression':'>= 21'} (an `if`/`elseif` test). Use [] for an `else` rule.\n"
            "  actions: list of {variable, expression} — e.g. {'variable':'Eligible', "
            "'expression':\"'Yes'\"} (assigns the expression to the variable).\n"
            "  conditional: 'if' (default), 'elseif', or 'else'.\n"
            "String literals in expressions must be single-quoted (e.g. \"'Yes'\")."
        ),
    )
    async def sas_add_rule(
        rule_set_id: str, ctx: Context,
        actions: list[dict], conditions: list[dict] | None = None,
        conditional: str = "if", name: str = "rule",
    ) -> Any:
        conditions = conditions or []
        try:
            async with session("sas_add_rule", ctx) as client:
                try:
                    rs = await get_json(f"/businessRules/ruleSets/{rule_set_id}", client,
                                        accept=_RS_INTEGRAL)
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 404:
                        return {"status": "not_found", "rule_set_id": rule_set_id}
                    raise
                sig = {t.get("name"): t for t in rs.get("signature", [])}

                def term_for(var: str) -> dict | None:
                    t = sig.get(var)
                    if not t:
                        return None
                    return {k: t[k] for k in ("id", "name", "dataType", "direction", "length") if k in t}

                bad = [c.get("variable") for c in conditions if c.get("variable") not in sig] + \
                      [a.get("variable") for a in actions if a.get("variable") not in sig]
                if bad:
                    return {"status": "unknown_variable",
                            "message": f"Variable(s) {bad} are not in the rule set signature. "
                                       f"Available: {list(sig)}. Add them via sas_create_rule_set "
                                       "or use existing variable names."}
                rule = {
                    "name": name, "status": "valid", "version": 0, "conditional": conditional,
                    "conditions": [{"type": "decisionTable", "term": term_for(c["variable"]),
                                    "expression": c["expression"], "status": "valid"}
                                   for c in conditions],
                    "actions": [{"type": "assignment", "term": term_for(a["variable"]),
                                 "expression": a["expression"], "status": "valid"}
                                for a in actions],
                    "ruleFiredTrackingEnabled": True,
                }
                r = await client.post(f"{VIYA_ENDPOINT}/businessRules/ruleSets/{rule_set_id}/rules",
                                      json=rule, headers={"Content-Type": "application/json",
                                                          "Accept": "application/json"})
                r.raise_for_status()
                res = r.json()
                return {"status": "added", "rule_id": res.get("id"), "name": res.get("name"),
                        "rule_set_id": rule_set_id,
                        "url": _dm_url("rules", rule_set_id)}
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_delete_rule_set",
        description="Delete a SAS business rule set by its id. This is irreversible.",
    )
    async def sas_delete_rule_set(rule_set_id: str, ctx: Context) -> Any:
        try:
            async with session("sas_delete_rule_set", ctx) as client:
                r = await client.delete(f"{VIYA_ENDPOINT}/businessRules/ruleSets/{rule_set_id}")
                if r.status_code == 404:
                    return {"status": "not_found", "rule_set_id": rule_set_id}
                r.raise_for_status()
                return {"status": "deleted", "rule_set_id": rule_set_id}
        except Exception as e:
            return _err(e)

    # ── Code files (SQL / Python decision nodes) ───────────────────────────────

    @mcp.tool(
        name="sas_list_code_files",
        description=(
            "List SAS Intelligent Decisioning code files (the SQL / Python nodes used "
            "in decisions). Optionally filter by `name_contains`."
        ),
    )
    async def sas_list_code_files(
        ctx: Context, name_contains: str | None = None, limit: int = 50, start: int = 0
    ) -> Any:
        filters = f"contains(name,'{_q(name_contains)}')" if name_contains else None
        try:
            async with session("sas_list_code_files", ctx) as client:
                items, count = await get_paged_items(
                    "/decisions/codeFiles", client, limit=limit, start=start, filters=filters,
                    extra_params={"sortBy": "modifiedTimeStamp:descending"})
                return {"count": count, "code_files": [
                    {**{k: it.get(k) for k in ("id", "name", "type", "description",
                                               "createdBy", "modifiedTimeStamp")},
                     "url": _dm_url("codeFiles", it.get("id", ""))}
                    for it in items]}
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_get_code_file",
        description=(
            "Get one SAS decision code file by id: its name, language/type, and "
            "signature, plus the source code content."
        ),
    )
    async def sas_get_code_file(code_file_id: str, ctx: Context) -> Any:
        try:
            async with session("sas_get_code_file", ctx) as client:
                try:
                    cf = await get_json(f"/decisions/codeFiles/{code_file_id}", client, accept=_CF)
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 404:
                        return {"status": "not_found", "code_file_id": code_file_id}
                    raise
                content = None
                for link in cf.get("links", []):
                    if link.get("rel") == "content":
                        try:
                            cr = await client.get(f"{VIYA_ENDPOINT}{link['href']}")
                            if cr.is_success:
                                content = cr.text
                        except httpx.HTTPError:
                            pass
                        break
                return {"id": cf.get("id"), "name": cf.get("name"), "type": cf.get("type"),
                        "signature": _sig_vars(cf.get("signature")), "code": content,
                        "url": _dm_url("codeFiles", cf.get("id", code_file_id))}
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_create_code_file",
        description=(
            "Create a SAS Intelligent Decisioning code file (a SQL or Python node) "
            "from source code. `language` is 'sql' or 'python'. The code is uploaded "
            "to the Files service and registered as a decision code file; returns its "
            "id so it can be used as a node in a decision. (For Python, the first line "
            "may be a SAS signature-extension comment defining the node's inputs/"
            "outputs.)"
        ),
    )
    async def sas_create_code_file(
        name: str, code: str, ctx: Context, language: str = "sql", description: str = ""
    ) -> Any:
        language = language.lower().strip()
        if language not in ("sql", "python"):
            return {"status": "invalid", "message": "language must be 'sql' or 'python'."}
        ext = "sql" if language == "sql" else "py"
        cf_type = "decisionSQLCodeFile" if language == "sql" else "decisionPythonFile"
        fname = f"{name}.{ext}"
        try:
            async with session("sas_create_code_file", ctx) as client:
                up = await client.post(
                    f"{VIYA_ENDPOINT}/files/files",
                    content=code.encode("utf-8"),
                    headers={"Content-Type": "text/plain",
                             "Content-Disposition": f'attachment; filename="{fname}"',
                             "Accept": "application/json"})
                up.raise_for_status()
                file_id = up.json()["id"]
                reg = await client.post(
                    f"{VIYA_ENDPOINT}/decisions/codeFiles",
                    json={"assetType": "codeFile", "name": name, "description": description,
                          "fileUri": f"/files/files/{file_id}", "type": cf_type,
                          "outputType": "dataGrid", "testCustomContextUri": "", "checkout": False},
                    headers={"Content-Type": _CF, "Accept": _CF})
                reg.raise_for_status()
                cf = reg.json()
                return {"status": "created", "id": cf.get("id"), "name": cf.get("name"),
                        "type": cf.get("type"), "language": language,
                        "file_uri": f"/files/files/{file_id}",
                        "url": _dm_url("codeFiles", cf.get("id", ""))}
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_delete_code_file",
        description="Delete a SAS decision code file by its id. This is irreversible.",
    )
    async def sas_delete_code_file(code_file_id: str, ctx: Context) -> Any:
        try:
            async with session("sas_delete_code_file", ctx) as client:
                r = await client.delete(f"{VIYA_ENDPOINT}/decisions/codeFiles/{code_file_id}")
                if r.status_code == 404:
                    return {"status": "not_found", "code_file_id": code_file_id}
                r.raise_for_status()
                return {"status": "deleted", "code_file_id": code_file_id}
        except Exception as e:
            return _err(e)

    # ── Validate / generate code / publish (deploy) ────────────────────────────

    @mcp.tool(
        name="sas_validate_expression",
        description=(
            "Validate a SAS Intelligent Decisioning expression (a rule condition/action "
            "or a branch condition) against a set of variables, BEFORE using it in a "
            "rule or decision. `expression` is the SAS expression (e.g. \"Age >= 21\" or "
            "\"Eligible = 'Yes'\"). `variables` is the list of variables it may reference "
            "— [{name, dataType}] with dataType string/integer/decimal/boolean/date. "
            "Returns valid=true, or valid=false with the error message. Every variable "
            "the expression uses must be listed or it is invalid."
        ),
    )
    async def sas_validate_expression(
        expression: str, ctx: Context, variables: list[dict] | None = None
    ) -> Any:
        sig = [{"name": v["name"], "dataType": str(v.get("dataType", "string"))}
               for v in (variables or [])]
        try:
            async with session("sas_validate_expression", ctx) as client:
                r = await client.post(
                    f"{VIYA_ENDPOINT}/decisions/validations/expressionValidations",
                    json={"expression": expression, "signature": sig},
                    headers={"Content-Type": _VALIDATION_REQ, "Accept": _VALIDATION_RESP})
                r.raise_for_status()
                d = r.json()
                valid = bool(d.get("valid"))
                out: dict[str, Any] = {"expression": expression, "valid": valid}
                if not valid:
                    msg = (d.get("error") or {}).get("message", "")
                    # surface the meaningful WARNING/ERROR line, not the whole DS2 log
                    key = next((ln.strip() for ln in str(msg).splitlines()
                                if "ERROR" in ln or "WARNING" in ln), "")
                    out["error"] = key or (str(msg)[:300] if msg else "invalid expression")
                return out
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_get_decision_code",
        description=(
            "Get the generated SAS DS2 code for a decision flow (what actually runs when "
            "the decision is published/scored). Returns the code as text. Useful to "
            "inspect or review the decision's logic."
        ),
    )
    async def sas_get_decision_code(decision_id: str, ctx: Context) -> Any:
        try:
            async with session("sas_get_decision_code", ctx) as client:
                r = await client.get(f"{VIYA_ENDPOINT}/decisions/flows/{decision_id}/code",
                                     headers={"Accept": _DS2})
                if r.status_code == 404:
                    return {"status": "not_found", "decision_id": decision_id}
                r.raise_for_status()
                return {"decision_id": decision_id, "language": "ds2", "code": r.text,
                        "url": _dm_url("decisions", decision_id)}
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_list_publish_destinations",
        description=(
            "List the destinations a SAS decision can be published to (e.g. 'maslocal' — "
            "the Micro Analytic Score service, the default; or container/CAS destinations). "
            "Returns each destination's name, type and description. Use a name with "
            "sas_publish_decision."
        ),
    )
    async def sas_list_publish_destinations(ctx: Context) -> Any:
        try:
            async with session("sas_list_publish_destinations", ctx) as client:
                items, count = await get_paged_items(
                    "/modelPublish/destinations", client, limit=200)
                return {"count": count, "destinations": [
                    {k: it.get(k) for k in ("name", "destinationType", "description")}
                    for it in items]}
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_publish_decision",
        description=(
            "Publish (deploy) a SAS Intelligent Decisioning decision so it can be executed "
            "for scoring. Publishes the decision's current revision to a destination "
            "(`destination_name` defaults to 'maslocal', the Micro Analytic Score service "
            "— see sas_list_publish_destinations). Returns the publish id and status; check "
            "details with sas_get_publish_log. This is the 'deploy' step after building a "
            "decision."
        ),
    )
    async def sas_publish_decision(
        decision_id: str, ctx: Context, destination_name: str = "maslocal"
    ) -> Any:
        try:
            async with session("sas_publish_decision", ctx) as client:
                try:
                    dec = await get_json(f"/decisions/flows/{decision_id}", client, accept=_DEC)
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 404:
                        return {"status": "not_found", "decision_id": decision_id}
                    raise
                name = dec.get("name", "decision")
                major, minor = dec.get("majorRevision", 1), dec.get("minorRevision", 0)
                # resolve the current (latest) revision id
                revs, _ = await get_paged_items(
                    f"/decisions/flows/{decision_id}/revisions", client, limit=1,
                    extra_params={"sortBy": "modifiedTimeStamp:descending"})
                if not revs:
                    return {"status": "error", "message": "Could not resolve a revision to publish."}
                rev = revs[0].get("id")
                model_name = f"{_sanitize_ident(name)}{major}_{minor}"
                src = f"/decisions/flows/{decision_id}/revisions/{rev}"
                code_uri = (f"{src}/code?rootPackageName={model_name}&lookupMode=PACKAGE"
                            "&traversedPathFlag=false&isGeneratingRuleFiredColumn=false"
                            "&codeTarget=microAnalyticService")
                body = {
                    "name": f'Decision "{name} ({major}.{minor})" published via MCP',
                    "state": "active", "notes": "Published via SAS MCP",
                    "modelContents": [{
                        "overwrite": True, "modelName": model_name,
                        "codeMediaType": "text/vnd.sas.source.ds2.async", "codeType": "ds2",
                        "codeUri": code_uri, "publishLevel": "decision", "sourceUri": src,
                        "properties": {"open.api.version": "1.0"}}],
                    "reloadModelTable": True, "destinationName": destination_name,
                    "tags": [], "properties": {}}
                r = await client.post(
                    f"{VIYA_ENDPOINT}/modelPublish/models", json=body,
                    headers={"Content-Type": _PUBLISH_REQ, "Accept": "application/json"})
                if r.status_code == 404:
                    return {"status": "error",
                            "message": f"Destination '{destination_name}' not found. "
                                       "Use sas_list_publish_destinations."}
                r.raise_for_status()
                d = r.json()
                item = (d.get("items") or [d])[0]
                err = item.get("errorResponse") or {}
                return {"status": "error" if err else "published",
                        "publish_id": item.get("id"), "model_name": model_name,
                        "destination": destination_name, "decision": name,
                        "error": err.get("message") if err else None,
                        "message": "Published. Check sas_get_publish_log for details."}
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_get_publish_log",
        description=(
            "Get the publishing log for a publish job (from sas_publish_decision), to "
            "confirm whether the decision deployed successfully and see any messages."
        ),
    )
    async def sas_get_publish_log(publish_id: str, ctx: Context) -> Any:
        try:
            async with session("sas_get_publish_log", ctx) as client:
                r = await client.get(f"{VIYA_ENDPOINT}/modelPublish/models/{publish_id}/log",
                                     headers={"Accept": "application/json"})
                if r.status_code == 404:
                    return {"status": "not_found", "publish_id": publish_id}
                r.raise_for_status()
                d = r.json()
                log = d.get("log", "")
                ok = "successfully" in str(log).lower() or "completed" in str(log).lower()
                return {"publish_id": publish_id,
                        "status": "success" if ok else "see_log",
                        "log": str(log)[:4000]}
        except Exception as e:
            return _err(e)
