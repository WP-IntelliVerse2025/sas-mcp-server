"""Geo map builders, extracted verbatim from a real SAS VA report (VA_report_new.har)
and parameterized.

A VA geo map is intricate (a geographic data item with `geoInfos`, synthetic
items, a separate `geomapDataList` for the map geometry, and a LayoutOverlayMap
gtml), so rather than hand-build it we reuse the captured subgraph and swap only
the geography column + the measure (and the CAS table).

  • region map  — colors regions (e.g. US states) by a measure. The geography
    column must match the external region source (default `us.primary.names`,
    i.e. US state names).
  • coordinate map — (added from ve69) plots lat/long points.
"""
from __future__ import annotations

import copy
import json
import os
from datetime import datetime, timezone
from typing import Optional

_HERE = os.path.dirname(os.path.abspath(__file__))
_REGION = None
_COORD = None


def _load_region() -> dict:
    global _REGION
    if _REGION is None:
        with open(os.path.join(_HERE, "geo_region_template.json"), encoding="utf-8") as f:
            _REGION = json.load(f)
    return _REGION


def _load_coord() -> dict:
    global _COORD
    if _COORD is None:
        with open(os.path.join(_HERE, "geo_coord_template.json"), encoding="utf-8") as f:
            _COORD = json.load(f)
    return _COORD


def _set_attrs(root: dict, name: str, **attrs) -> None:
    """Update the (single) node declared with ``name`` anywhere in the tree."""
    def walk(n):
        if isinstance(n, dict):
            if n.get("name") == name:
                n.update(attrs)
            for v in n.values():
                walk(v)
        elif isinstance(n, list):
            for v in n:
                walk(v)
    walk(root)


def _retable(rep: dict, server: str, library: str, table: str) -> None:
    """Point the DATA source at the target CAS table (leave the geomap geometry
    data source — the one with no table — alone)."""
    for ds in rep.get("dataSources", []):
        cr = ds.get("casResource")
        if cr and cr.get("table"):
            cr["server"], cr["library"], cr["table"] = server, library, table


def build_geo_region(report_name: str, cas_server: str, cas_library: str, cas_table: str,
                     geo_column: str, measure_column: str,
                     measure_label: Optional[str] = None,
                     geo_label: Optional[str] = None) -> dict:
    """A region geo map coloring ``geo_column`` regions by ``measure_column``.
    ``geo_column`` values must match the captured external source (US state names)."""
    rep = copy.deepcopy(_load_region())
    rep["label"] = report_name
    rep["dateModified"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    _retable(rep, cas_server, cas_library, cas_table)
    # bi22 = the color measure (xref Sales); bi54 = the geographic item (xref State).
    _set_attrs(rep, "bi22", xref=measure_column, label=measure_label or measure_column)
    _set_attrs(rep, "bi54", xref=geo_column, label=geo_label or geo_column)
    return rep


def build_geo_coordinate(report_name: str, cas_server: str, cas_library: str, cas_table: str,
                         latitude_column: str, longitude_column: str, size_column: str,
                         size_label: Optional[str] = None) -> dict:
    """A coordinate (bubble) geo map — plots lat/long points sized by a measure.
    Works on any table with real latitude + longitude numeric columns."""
    rep = copy.deepcopy(_load_coord())
    rep["label"] = report_name
    rep["dateModified"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    _retable(rep, cas_server, cas_library, cas_table)
    # bi13 = Latitude, bi14 = Longitude, bi22 = the bubble-size measure.
    _set_attrs(rep, "bi13", xref=latitude_column)
    _set_attrs(rep, "bi14", xref=longitude_column)
    _set_attrs(rep, "bi22", xref=size_column, label=size_label or size_column)
    return rep
