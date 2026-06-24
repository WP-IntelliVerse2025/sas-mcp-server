"""
Build single-object SAS Visual Analytics reports from BIRD templates captured
in VA_report.har.

Each template (see ``va_templates.json``) is one VA object exactly as SAS VA
emitted it — its ``gtml``/StatGraph, data definition (incl. any procedural
queries / synthetic items for histograms, box plots, heat maps, …) and the
data-source business items it needs. Because every generated report contains
exactly ONE object, the original HAR names (``bi*``/``dd*``/``ve*``) never
collide, so we keep them verbatim and only substitute:

  • the CAS resource (server / library / table), and
  • each configurable role's source-column ``xref``.

Every role defaults to the column captured in the HAR, so a zero-config call
reproduces a known-good object on the RETAIL_SALES_SAS_VA_DATASET sample; pass
``column_overrides`` to point a role at a different column.

Public API
──────────
  build_object_content(object_type, report_name, cas_server, cas_library,
                       cas_table, column_overrides=None, table_label=None) -> dict

  OBJECTS  – dict: object_type -> {element_type, graph_type, roles:{param->info}}
"""
import copy
import json
import os
from datetime import datetime, timezone
from typing import Optional

_TEMPLATES_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                               "va_templates.json")
with open(_TEMPLATES_PATH, encoding="utf-8") as _f:
    _TEMPLATES = json.load(_f)

# ── Friendly role parameters per object type ──────────────────────────────────
# Maps a human-facing parameter name -> the BIRD alias whose underlying source
# column should be replaced. The alias' captured column is the default.
_ROLE_PARAMS = {
    "key_value":    {"measure_column": "bi286"},
    "crosstab":     {"column_category": "bi118", "measure_column": "bi124",
                     "measure2_column": "bi126", "row_category": "bi128"},
    "heat_map":     {"x_column": "bi180", "y_column": "bi184",
                     "measure_column": "bi188", "color_column": "bi239"},
    "targeted_bar": {"category_column": "bi269", "target_column": "bi272",
                     "measure_column": "bi273"},
    "word_cloud":   {"category_column": "bi307"},
    "button_bar":   {"category_column": "bi314"},
    "waterfall":    {"category_column": "bi399", "measure_column": "bi19"},
    "histogram":    {"measure_column": "bi421"},
    "box_plot":     {"category_column": "bi440", "measure_column": "bi441"},
    "treemap":      {"category_column": "bi498", "size_column": "bi500",
                     "color_column": "bi502"},
    "list_control": {"category_column": "bi510", "measure_column": "bi509"},
    # containers carry no data
    "precision_container": {},
    "scrolling_container": {},
    "stacking_container":  {},
    "layout_container":    {},
}

# Container object types place their element in the layout as <Container> not <Visual>
_CONTAINER_TYPES = {"precision_container", "scrolling_container",
                    "stacking_container", "layout_container"}


def list_objects() -> dict:
    """Return {object_type: {element_type, graph_type, params:{name->default_col}}}."""
    out = {}
    for otype, tmpl in _TEMPLATES.items():
        el = tmpl["element"]
        params = {}
        role_by_alias = {r["alias"]: r for r in tmpl["roles"]}
        for pname, alias in _ROLE_PARAMS.get(otype, {}).items():
            role = role_by_alias.get(alias)
            params[pname] = role["xref"] if role else None
        out[otype] = {
            "element_type": el["@element"],
            "graph_type": el.get("graphType", ""),
            "params": params,
        }
    return out


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _apply_overrides(tmpl: dict, object_type: str,
                     column_overrides: Optional[dict]) -> None:
    """Rewrite source-item xrefs in-place for any overridden roles."""
    if not column_overrides:
        return
    param_to_alias = _ROLE_PARAMS.get(object_type, {})
    alias_to_source = {r["alias"]: r["source"] for r in tmpl["roles"]}
    src_by_name = {s["name"]: s for s in tmpl["source_items"]}

    for param, new_col in column_overrides.items():
        if not new_col:
            continue
        alias = param_to_alias.get(param)
        if alias is None:
            raise ValueError(
                f"'{param}' is not a valid column role for object '{object_type}'. "
                f"Valid roles: {sorted(param_to_alias)}"
            )
        source_name = alias_to_source.get(alias)
        src = src_by_name.get(source_name)
        if src is None:
            raise ValueError(
                f"Internal: role '{param}' has no source item in template "
                f"'{object_type}'."
            )
        src["xref"] = new_col


def _apply_measure_attrs(tmpl: dict, object_type: str,
                         aggregation: Optional[str], fmt: Optional[str]) -> None:
    """Set aggregation (sum/average/…) and/or a SAS display format on the object's
    measure source item — e.g. a KPI showing AVG Sales formatted as currency."""
    if not (aggregation or fmt):
        return
    from .report_builder import _norm_agg, _norm_format  # lazy: avoids import cycle
    alias = _ROLE_PARAMS.get(object_type, {}).get("measure_column")
    if not alias:
        return
    alias_to_source = {r["alias"]: r["source"] for r in tmpl["roles"]}
    src_by_name = {s["name"]: s for s in tmpl["source_items"]}
    src = src_by_name.get(alias_to_source.get(alias))
    if src is None:
        return
    a, f = _norm_agg(aggregation), _norm_format(fmt)
    if a:
        src["aggregation"] = a
    if f:
        src["format"] = f


def _view_section(element_name: str, is_container: bool) -> dict:
    """A single-page section placing the one object, filling the page."""
    inner = {
        "@element": "Container" if is_container else "Visual",
        "name": "viObj",
        "ref": element_name,
        "responsiveConstraint": {
            "@element": "ResponsiveConstraint",
            "widthConstraint": {
                "@element": "Responsive_WidthConstraint",
                "widths": [{"@element": "Width", "mediaTarget": "mt_small",
                            "preferredSizeBehavior": "ignore",
                            "flexibility": "flexible"}],
            },
            "heightConstraint": {
                "@element": "Responsive_HeightConstraint",
                "heights": [{"@element": "Height", "mediaTarget": "mt_small",
                             "preferredSizeBehavior": "ignore",
                             "flexibility": "flexible"}],
            },
        },
    }
    return {
        "@element": "Section",
        "name": "vi1",
        "label": "Page 1",
        "body": {
            "@element": "Body",
            "mediaContainerList": [{
                "@element": "MediaContainer",
                "target": "mt_default",
                "layout": {
                    "@element": "ResponsiveLayout",
                    "orientation": "vertical",
                    "overflow": "fit",
                    "weights": [
                        {"@element": "Weights", "mediaTarget": t,
                         "unit": "percent",
                         "values": [{"@element": "Weight", "value": "100%"}]}
                        for t in ("mt_large", "mt_medium", "mt_small")
                    ],
                },
                "containedElementList": [inner],
            }],
        },
    }


def build_object_content(
    object_type: str,
    report_name: str,
    cas_server: str,
    cas_library: str,
    cas_table: str,
    column_overrides: Optional[dict] = None,
    table_label: Optional[str] = None,
    measure_aggregation: Optional[str] = None,
    measure_format: Optional[str] = None,
) -> dict:
    """Build a complete SASReport BIRD document containing a single VA object."""
    object_type = object_type.lower().strip()
    if object_type not in _TEMPLATES:
        raise ValueError(
            f"Unknown object_type '{object_type}'. "
            f"Valid types: {sorted(_TEMPLATES)}"
        )
    tmpl = copy.deepcopy(_TEMPLATES[object_type])
    _apply_overrides(tmpl, object_type, column_overrides)
    _apply_measure_attrs(tmpl, object_type, measure_aggregation, measure_format)

    # The waterfall's displayed measure is a calculated item (a grouped sum of the
    # overridable measure source); keep its label in sync with a measure override
    # so a "Profit" waterfall isn't mislabelled "Sales".
    if object_type == "waterfall" and column_overrides and column_overrides.get("measure_column"):
        for s in tmpl["source_items"]:
            if s.get("@element") == "AggregateCalculatedItem":
                s["label"] = column_overrides["measure_column"]

    now = _now()
    table_label = table_label or cas_table
    element = tmpl["element"]
    element_name = element["name"]
    is_container = object_type in _CONTAINER_TYPES

    # Drop dangling cross-references to companion visuals we don't include.
    element.pop("supplementalVisualList", None)

    # Several captured gtml templates hardcode white text (color="16777215" =
    # 0xFFFFFF), invisible on the light theme — the value computes but the tile
    # renders blank in the VA UI. Drop the hardcoded white so text inherits the
    # theme's (dark) color.
    if element.get("gtml"):
        element["gtml"] = element["gtml"].replace(' color="16777215"', '')

    data_sources = [{
        "@element": "DataSource",
        "name": "ds7",
        "label": table_label,
        "type": "relational",
        "casResource": {
            "@element": "CasResource",
            "server": cas_server,
            "library": cas_library,
            "table": cas_table,
            "locale": "en_US",
        },
        "businessItemFolder": {
            "@element": "BusinessItemFolder",
            "items": tmpl["source_items"],
        },
    }]

    report = {
        "@element": "SASReport",
        "xmlns": "http://www.sas.com/sasreportmodel/bird-4.64.0",
        "label": report_name,
        "createdApplicationName": "SAS Visual Analytics",
        "dateModified": now,
        "lastModifiedApplicationName": "SAS Visual Analytics",
        "createdLocale": "en_US",
        "features": ["promptModelV2"],
        "implicitInteractions": ["reportPrompt", "sectionPrompt", "sectionLink"],
        "nextUniqueNameIndex": 9000,
        "dataDefinitions": tmpl["data_definitions"],
        "dataSources": data_sources,
        "visualElements": [element],
        "view": {"@element": "View", "sections": [
            _view_section(element_name, is_container)]},
        "mediaSchemes": [{
            "@element": "MediaScheme", "name": "ms1",
            "baseStylesheetResource": {
                "@element": "BaseStylesheetResource", "theme": "light2025"},
            "stylesheet": {"@element": "Stylesheet", "styles": {}},
        }],
        "mediaTargets": [
            {"@element": "MediaTarget", "name": "mt_default",
             "windowSize": "default", "scheme": "ms1"},
            {"@element": "MediaTarget", "name": "mt_small",
             "windowSize": "small", "scheme": "ms1"},
            {"@element": "MediaTarget", "name": "mt_medium",
             "windowSize": "medium", "scheme": "ms1"},
            {"@element": "MediaTarget", "name": "mt_large",
             "windowSize": "large", "scheme": "ms1"},
        ],
        "properties": [
            {"@element": "Property", "key": "displayDataSource", "value": "ds7"}],
        "history": {
            "@element": "History",
            "editors": [{
                "@element": "Editor", "applicationName": "VA",
                "revisions": [{"@element": "Revision", "editorVersion": "2020",
                               "lastDate": now}],
            }],
        },
        "sasReportState": {
            "@element": "SASReportState",
            "view": {"@element": "View_State"},
        },
    }
    return report
