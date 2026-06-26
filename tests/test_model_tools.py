# Copyright © 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the SAS Model Manager (modelRepository) tools.

Two layers, no network:

* **Tool layer** — register the tools on a FastMCP server and drive them through
  the MCP protocol with ``SASModelManagerClient`` patched, asserting every tool
  is exposed and that each one calls the right client method with the right
  arguments (including the attached-file path of ``sas_model_import``).
* **Client layer** — exercise the real ``SASModelManagerClient`` against a fake
  synchronous httpx client, asserting the exact HTTP request each method sends
  (URL, params, headers, JSON body) and the response shaping. The CAS-column →
  model-variable mapping (``add_variables_from_cas_table``) is checked against a
  realistic column set — that mapping is the heart of the import-variables flow.
"""

import json
from unittest.mock import MagicMock, patch

import pytest
from fastmcp import Client, FastMCP

from sas_mcp_server.model_tools import register_model_tools
from sas_mcp_server.va.model_manager_client import SASModelManagerClient

BASE = "https://test.viya.com"
TOKEN = "test-token"

EXPECTED_MODEL_TOOLS = [
    "sas_model_list_repositories",
    "sas_model_list",
    "sas_model_summary",
    "sas_model_get",
    "sas_model_get_content",
    "sas_model_list_variables",
    "sas_model_add_variables",
    "sas_model_add_variables_from_cas_table",
    "sas_model_import",
    "sas_model_list_projects",
    "sas_model_get_project",
    "sas_model_projects_summary",
    "sas_model_create_project",
    "sas_model_list_project_models",
    "sas_model_copy_model_to_project",
    "sas_model_create_project_with_model",
]


# ---------------------------------------------------------------------------
# Fake synchronous httpx plumbing (the client uses httpx.Client, not AsyncClient)
# ---------------------------------------------------------------------------


class FakeResp:
    """Minimal stand-in for an httpx.Response."""

    def __init__(self, status=200, json_data=None, text="", headers=None):
        self.status_code = status
        self._json = {} if json_data is None else json_data
        self.text = text or (json.dumps(json_data) if json_data is not None else "")
        self.content = self.text.encode() if self.text else b""
        self.headers = headers or {"Content-Type": "application/json"}

    @property
    def is_success(self):
        return 200 <= self.status_code < 300

    def raise_for_status(self):
        if not self.is_success:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self._json


class FakeHTTP:
    """Routes GET/POST calls through a handler and records every call."""

    def __init__(self, handler):
        self._handler = handler
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append(("GET", url, kwargs))
        return self._handler("GET", url, **kwargs)

    def post(self, url, **kwargs):
        self.calls.append(("POST", url, kwargs))
        return self._handler("POST", url, **kwargs)

    def close(self):
        pass

    def last(self, method):
        for m, url, kwargs in reversed(self.calls):
            if m == method:
                return url, kwargs
        raise AssertionError(f"no {method} call recorded")


def make_client(handler):
    """Build a real SASModelManagerClient whose transport is the fake handler."""
    client = SASModelManagerClient(base_url=BASE, access_token=TOKEN)
    client._http = FakeHTTP(handler)
    return client


def collection(items, **extra):
    body = {"items": list(items), "count": len(items)}
    body.update(extra)
    return body


def tool_text(result):
    """A tool's textual output, whether it lands in structured data or content."""
    if result.data is not None:
        return str(result.data)
    return "".join(getattr(block, "text", "") for block in result.content)


# ===========================================================================
# Tool layer — registration, schema, and dispatch through the MCP protocol
# ===========================================================================


@pytest.fixture
def model_mcp_with_mock():
    """A FastMCP server with the model tools registered and the client patched.

    Returns ``(mcp, instance)`` where *instance* is the MagicMock every tool
    receives when it constructs ``SASModelManagerClient`` — so a test can stub a
    method's return value and assert how the tool called it.
    """
    instance = MagicMock()
    with patch(
        "sas_mcp_server.model_tools.SASModelManagerClient", return_value=instance
    ):
        mcp = FastMCP("Model Tool Test Server")

        async def get_token(_ctx):
            return TOKEN

        register_model_tools(mcp, get_token)
        yield mcp, instance


async def test_all_model_tools_registered(model_mcp_with_mock):
    mcp, _ = model_mcp_with_mock
    async with Client(mcp) as client:
        names = {t.name for t in await client.list_tools()}
    for expected in EXPECTED_MODEL_TOOLS:
        assert expected in names, f"Tool '{expected}' not registered"


async def test_model_tool_required_params(model_mcp_with_mock):
    mcp, _ = model_mcp_with_mock
    async with Client(mcp) as client:
        tool_map = {t.name: t for t in await client.list_tools()}

    add_vars = tool_map["sas_model_add_variables"]
    required = add_vars.inputSchema.get("required", [])
    assert "model_id" in required
    assert "variables" in required

    imp = tool_map["sas_model_import"]
    props = imp.inputSchema["properties"]
    assert "name" in props
    assert "folder_id" in props
    # The attached file bytes are injected by middleware, never a tool argument.
    assert "file_b64" not in props
    assert "_file_b64" not in props

    cwm = tool_map["sas_model_create_project_with_model"]
    required = cwm.inputSchema.get("required", [])
    assert "name" in required
    assert "source_model_id" in required


async def test_list_repositories_dispatch(model_mcp_with_mock):
    mcp, instance = model_mcp_with_mock
    instance.list_repositories.return_value = [{"id": "r1", "defaultRepository": True}]
    async with Client(mcp) as client:
        await client.call_tool("sas_model_list_repositories", {"limit": 50})
    instance.list_repositories.assert_called_once_with(50)


async def test_list_models_dispatch_passes_filter(model_mcp_with_mock):
    mcp, instance = model_mcp_with_mock
    instance.list_models.return_value = {"count": 0, "items": []}
    async with Client(mcp) as client:
        await client.call_tool(
            "sas_model_list", {"limit": 10, "start": 5, "name_filter": "credit"}
        )
    instance.list_models.assert_called_once_with(limit=10, start=5, name_filter="credit")


async def test_copy_model_to_project_dispatch(model_mcp_with_mock):
    mcp, instance = model_mcp_with_mock
    instance.copy_model_to_project.return_value = {"id": "m1", "projectId": "p1"}
    async with Client(mcp) as client:
        await client.call_tool(
            "sas_model_copy_model_to_project",
            {"source_model_id": "m1", "project_id": "p1", "op_code": "move"},
        )
    instance.copy_model_to_project.assert_called_once_with(
        source_model_id="m1", project_id="p1", project_version_id=None, op_code="move"
    )


# (tool name, call args, client method the tool must invoke) for every tool that
# does not consume an attached file — sas_model_import is covered separately.
_DISPATCH_CASES = [
    ("sas_model_list_repositories", {}, "list_repositories"),
    ("sas_model_list", {}, "list_models"),
    ("sas_model_summary", {}, "get_models_summary"),
    ("sas_model_get", {"model_id": "m1"}, "get_model"),
    ("sas_model_get_content", {"model_id": "m1", "content_id": "c1"}, "get_model_content"),
    ("sas_model_list_variables", {"model_id": "m1"}, "list_model_variables"),
    ("sas_model_add_variables",
     {"model_id": "m1", "variables": [{"name": "x", "type": "string"}]},
     "add_model_variables"),
    ("sas_model_add_variables_from_cas_table",
     {"model_id": "m1", "cas_library": "Public", "cas_table": "T"},
     "add_variables_from_cas_table"),
    ("sas_model_list_projects", {}, "list_projects"),
    ("sas_model_get_project", {"project_id": "p1"}, "get_project"),
    ("sas_model_projects_summary", {}, "get_projects_summary"),
    ("sas_model_create_project", {"name": "P"}, "create_project"),
    ("sas_model_list_project_models", {"project_id": "p1"}, "list_project_models"),
    ("sas_model_copy_model_to_project",
     {"source_model_id": "m1", "project_id": "p1"}, "copy_model_to_project"),
    ("sas_model_create_project_with_model",
     {"name": "P", "source_model_id": "m1"}, "create_project_with_model"),
]


@pytest.mark.parametrize("tool,args,method", _DISPATCH_CASES)
async def test_every_tool_dispatches_to_client(model_mcp_with_mock, tool, args, method):
    mcp, instance = model_mcp_with_mock
    getattr(instance, method).return_value = {"ok": True}
    async with Client(mcp) as client:
        await client.call_tool(tool, args)
    getattr(instance, method).assert_called_once()


async def test_import_model_reads_attached_file(model_mcp_with_mock):
    """sas_model_import pulls the .zip bytes from context state, not a tool arg."""
    import base64

    mcp, instance = model_mcp_with_mock
    instance.import_model.return_value = {"id": "m9", "name": "demo"}
    raw = b"PK\x03\x04 fake zip bytes"
    b64 = base64.b64encode(raw).decode()

    # Seed the same context state InjectedArgsMiddleware would populate, via a
    # middleware that runs before the tool.
    from fastmcp.server.middleware import Middleware

    class _Seed(Middleware):
        async def on_call_tool(self, ctx, call_next):
            fctx = ctx.fastmcp_context
            if fctx is not None:
                await fctx.set_state("upload_file_b64", b64)
                await fctx.set_state("upload_filename", "demo.zip")
            return await call_next(ctx)

    mcp.add_middleware(_Seed())
    async with Client(mcp) as client:
        await client.call_tool(
            "sas_model_import", {"name": "demo", "folder_id": "f1"}
        )
    instance.import_model.assert_called_once_with(
        file_bytes=raw, name="demo", folder_id="f1", model_type="GENERIC",
        filename="demo.zip",
    )


async def test_import_model_without_file_returns_guidance(model_mcp_with_mock):
    mcp, instance = model_mcp_with_mock
    async with Client(mcp) as client:
        result = await client.call_tool(
            "sas_model_import", {"name": "demo", "folder_id": "f1"}
        )
    instance.import_model.assert_not_called()
    assert "No file was attached" in tool_text(result)


async def test_tool_error_is_caught(model_mcp_with_mock):
    """A client exception is surfaced as an ERROR string, not an MCP crash."""
    mcp, instance = model_mcp_with_mock
    instance.get_model.side_effect = RuntimeError("boom")
    async with Client(mcp) as client:
        result = await client.call_tool("sas_model_get", {"model_id": "x"})
    text = tool_text(result)
    assert "ERROR" in text
    assert "boom" in text


# ===========================================================================
# Client layer — exact HTTP requests + response shaping (no network)
# ===========================================================================


def test_list_repositories_request_and_shaping():
    def handler(method, url, **kwargs):
        return FakeResp(json_data=collection([
            {"id": "r1", "name": "Repo 1", "folderId": "f1", "defaultRepository": True},
            {"id": "r2", "name": "Repo 2", "folderId": "f2"},
        ]))

    client = make_client(handler)
    repos = client.list_repositories(limit=500)

    url, kwargs = client._http.last("GET")
    assert url == f"{BASE}/modelRepository/repositories"
    assert kwargs["params"] == {"start": 0, "limit": 500}
    assert kwargs["headers"]["Accept"] == "application/vnd.sas.collection+json"
    assert kwargs["headers"]["Authorization"] == f"Bearer {TOKEN}"
    assert repos[0]["defaultRepository"] is True
    assert repos[1]["defaultRepository"] is False  # defaulted


def test_list_models_builds_name_filter():
    def handler(method, url, **kwargs):
        return FakeResp(json_data=collection([{"id": "m1", "name": "credit"}], start=0, limit=10))

    client = make_client(handler)
    out = client.list_models(limit=10, start=0, name_filter="cred")

    _url, kwargs = client._http.last("GET")
    assert kwargs["params"]["filter"] == "contains(name,'cred')"
    assert out["count"] == 1
    assert out["items"][0]["name"] == "credit"


def test_get_model_surfaces_files_and_variables():
    def handler(method, url, **kwargs):
        return FakeResp(json_data={
            "id": "m1", "name": "demo", "scoreCodeType": "Python",
            "files": [{"id": "c1", "name": "package.json", "role": "scoreResource"}],
            "inputVariables": [{"name": "x"}], "outputVariables": [{"name": "y"}],
            "scoreCodeUri": "/files/files/abc",
        })

    client = make_client(handler)
    out = client.get_model("m1")

    assert out["files"][0]["id"] == "c1"
    assert out["inputVariables"] == [{"name": "x"}]
    assert out["outputVariables"] == [{"name": "y"}]
    assert out["scoreCodeUri"] == "/files/files/abc"


def test_add_model_variables_envelope_and_normalisation():
    captured = {}

    def handler(method, url, **kwargs):
        if method == "POST":
            captured["body"] = kwargs["json"]
            captured["headers"] = kwargs["headers"]
            return FakeResp(status=201, json_data=collection(kwargs["json"]["items"]))
        return FakeResp(json_data=collection([]))

    client = make_client(handler)
    client.add_model_variables("m1", [
        {"name": "Amount", "type": "decimal", "length": 12, "role": "input"},
        {"name": "Region"},  # loose dict: type/length/role all defaulted
    ])

    body = captured["body"]
    assert set(body.keys()) == {"items"}  # the {"items":[...]} envelope
    assert captured["headers"]["Content-Type"] == "application/vnd.sas.collection+json"
    first, second = body["items"]
    assert first == {
        "name": "Amount", "type": "decimal", "description": "decimal",
        "length": 12, "role": "input",
    }
    # Defaults: type->string, length->8, role->input, description->type
    assert second == {
        "name": "Region", "type": "string", "description": "string",
        "length": 8, "role": "input",
    }


def test_add_variables_from_cas_table_maps_columns():
    """The heart of the import-variables flow: CAS columns -> model variables.

    varchar/char -> string with the column's raw length; numeric -> decimal with
    the fixed numeric_length; the target column gets role 'output'.
    """
    cas_columns = collection([
        {"name": "Demand_Class", "type": "varchar", "rawLength": 25, "index": 1},
        {"name": "Region", "type": "char", "rawLength": 10, "index": 2},
        {"name": "Amount", "type": "double", "index": 3},
        {"name": "KPI_Billing", "type": "double", "index": 4},
    ])
    posted = {}

    def handler(method, url, **kwargs):
        if method == "GET":  # casManagement columns read
            assert "/casManagement/servers/cas-shared-default/caslibs/Public/tables/S01OTH/columns" in url
            return FakeResp(json_data=cas_columns)
        posted["items"] = kwargs["json"]["items"]
        return FakeResp(status=201, json_data=collection(kwargs["json"]["items"]))

    client = make_client(handler)
    result = client.add_variables_from_cas_table(
        "m1", caslib="Public", table="S01OTH", output_columns=["KPI_Billing"]
    )

    items = {v["name"]: v for v in posted["items"]}
    assert items["Demand_Class"]["type"] == "string"
    assert items["Demand_Class"]["length"] == 25
    assert items["Region"]["type"] == "string"
    assert items["Region"]["length"] == 10
    assert items["Amount"]["type"] == "decimal"
    assert items["Amount"]["length"] == 12  # numeric_length default
    assert items["Amount"]["role"] == "input"
    assert items["KPI_Billing"]["type"] == "decimal"
    assert items["KPI_Billing"]["role"] == "output"  # marked as target
    assert result["source_table"] == "Public.S01OTH"
    assert result["mapped_from_columns"] == 4


def test_import_model_multipart_and_query():
    captured = {}

    def handler(method, url, **kwargs):
        captured["url"] = url
        captured["params"] = kwargs["params"]
        captured["files"] = kwargs["files"]
        return FakeResp(status=201, json_data=collection([{"id": "m1", "name": "demo"}]))

    client = make_client(handler)
    out = client.import_model(b"PKzip", name="demo", folder_id="f1")

    assert captured["url"] == f"{BASE}/modelRepository/models"
    assert captured["params"] == {
        "fileName": "demo", "folderId": "f1", "name": "demo", "type": "GENERIC",
    }
    files = captured["files"]
    assert files["files"][0] == "demo.zip"  # default filename from name
    assert files["files"][2] == "application/zip"
    assert out["id"] == "m1"


def test_import_model_raises_on_failure():
    def handler(method, url, **kwargs):
        return FakeResp(status=400, text="bad package")

    client = make_client(handler)
    with pytest.raises(RuntimeError, match="Model import failed HTTP 400"):
        client.import_model(b"x", name="demo", folder_id="f1")


def test_create_project_body_and_variable_shape():
    captured = {}

    def handler(method, url, **kwargs):
        if method == "GET":  # _default_repository lookup
            return FakeResp(json_data=collection([
                {"id": "r1", "folderId": "f1", "defaultRepository": True},
            ]))
        captured["body"] = kwargs["json"]
        captured["ctype"] = kwargs["headers"]["Content-Type"]
        return FakeResp(status=201, json_data={"id": "p1", "name": kwargs["json"]["name"]})

    client = make_client(handler)
    client.create_project(
        "Churn", function="classification", train_table="Public/S01OTH",
        target_variable="KPI_Billing",
        variables=[{"name": "Amount", "type": "double"}, {"name": "Region", "type": "varchar", "length": 10}],
    )

    body = captured["body"]
    assert captured["ctype"] == "application/vnd.sas.models.project+json"
    assert body["repositoryId"] == "r1"  # resolved from default repository
    assert body["folderId"] == "f1"
    assert body["function"] == "classification"
    assert body["targetVariable"] == "KPI_Billing"
    pvars = {v["name"]: v for v in body["variables"]}
    # Project variables use UPPERCASE type and STRING length.
    assert pvars["Amount"]["type"] == "DECIMAL"
    assert pvars["Amount"]["length"] == "12"
    assert pvars["Region"]["type"] == "STRING"
    assert pvars["Region"]["length"] == "10"


def test_copy_model_to_project_transfer_body():
    captured = {}

    def handler(method, url, **kwargs):
        if method == "GET":  # list_project_versions for latest-version resolution
            return FakeResp(json_data=collection([
                {"id": "v1", "versionNumber": "1.0", "projectId": "p1"},
                {"id": "v2", "versionNumber": "2.0", "projectId": "p1"},
            ]))
        captured["url"] = url
        captured["body"] = kwargs["json"]
        return FakeResp(status=201, json_data={"id": "m1", "projectId": "p1", "projectVersionId": "v2"})

    client = make_client(handler)
    out = client.copy_model_to_project(source_model_id="m1", project_id="p1")

    assert captured["url"] == f"{BASE}/modelRepository/models/transfer"
    body = captured["body"]
    assert body["opCode"] == "copy"
    assert body["sourceUri"] == "/modelRepository/models/m1"
    # Latest version (highest versionNumber) is selected automatically.
    assert body["destinationUri"] == "/modelRepository/projects/p1/projectVersions/v2"
    assert out["projectVersionId"] == "v2"


def test_create_project_with_model_chains_calls():
    """One-shot: create project, resolve version, copy model, populate variables."""
    cas_columns = collection([
        {"name": "Amount", "type": "double", "index": 1},
        {"name": "KPI_Billing", "type": "double", "index": 2},
    ])
    state = {"posted_variables": None}

    def handler(method, url, **kwargs):
        if method == "GET":
            if url.endswith("/repositories"):
                return FakeResp(json_data=collection([
                    {"id": "r1", "folderId": "f1", "defaultRepository": True},
                ]))
            if "/projectVersions" in url:
                return FakeResp(json_data=collection([
                    {"id": "v1", "versionNumber": "1.0", "projectId": "p1"},
                ]))
            if "/columns" in url:
                return FakeResp(json_data=cas_columns)
            if "/variables" in url:  # copied model has no variables yet
                return FakeResp(json_data=collection([]))
            return FakeResp(json_data=collection([]))
        # POSTs
        if url.endswith("/projects"):
            return FakeResp(status=201, json_data={"id": "p1", "name": "Churn"})
        if url.endswith("/transfer"):
            return FakeResp(status=201, json_data={"id": "m_copy", "projectId": "p1"})
        if "/variables" in url:
            state["posted_variables"] = kwargs["json"]["items"]
            return FakeResp(status=201, json_data=collection(kwargs["json"]["items"]))
        return FakeResp(status=201, json_data={})

    client = make_client(handler)
    result = client.create_project_with_model(
        name="Churn", source_model_id="m1", train_table="cas-shared-default/Public/S01OTH",
        target_variable="KPI_Billing",
    )

    assert result["status"] == "model saved to project"
    assert result["project"]["id"] == "p1"
    assert result["project_version_id"] == "v1"
    assert result["variables_added"] == 2
    names = {v["name"]: v for v in state["posted_variables"]}
    assert names["KPI_Billing"]["role"] == "output"
    assert names["Amount"]["role"] == "input"


def test_parse_train_table():
    assert SASModelManagerClient._parse_train_table("srv/lib/tbl") == ("srv", "lib", "tbl")
    assert SASModelManagerClient._parse_train_table("lib/tbl") == ("cas-shared-default", "lib", "tbl")
    assert SASModelManagerClient._parse_train_table("") == (None, None, None)


def test_get_model_content_streams_text():
    def handler(method, url, **kwargs):
        assert url.endswith("/modelRepository/models/m1/contents/c1/stream")
        assert kwargs["params"]["charNumber"] == 1000
        return FakeResp(text='{"name":"pkg"}', headers={"Content-Type": "application/json"})

    client = make_client(handler)
    out = client.get_model_content("m1", "c1", max_chars=1000)
    assert out["text"] == '{"name":"pkg"}'
    assert out["model_id"] == "m1"
