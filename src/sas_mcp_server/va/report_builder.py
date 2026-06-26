"""
Builds SAS Visual Analytics BIRD 4.x report content JSON.

Supported chart types
─────────────────────
  bar_h    – horizontal bar chart  (category vs frequency/measure, optional group)
  bar_v    – vertical bar chart    (same data, bars go up)
  line     – line / series chart   (x-category vs y-measure, optional group)
  pie      – pie chart             (category slices, frequency/measure)
  scatter  – scatter plot          (x-numeric vs y-numeric, optional group)
  bubble   – bubble chart          (x, y, size all numeric, optional group)

Usage
─────
  content = build_chart_content(
      chart_type      = "bar_h",
      report_name     = "My Report",
      cas_server      = "cas-shared-default",
      cas_library     = "Public",
      cas_table       = "CONTROLS",
      category_column = "controlType",
      group_column    = "riskRating",
  )

For scatter / bubble pass x_column, y_column (and size_column for bubble)
instead of category_column / measure_column.
"""

import copy
import json
import re
from datetime import datetime, timezone
from typing import Optional

# ── helpers ──────────────────────────────────────────────────────────────────

def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


# SAS aggregation keywords accepted on a measure DataItem (default = sum).
_AGGREGATIONS = {"sum", "average", "avg", "min", "max", "median", "count", "stddev"}
# Friendly format name -> SAS format string. Pass a raw SAS format (e.g. DOLLAR12.2)
# through unchanged.
_FORMAT_ALIASES = {
    "currency": "DOLLAR12.2", "dollar": "DOLLAR12.2", "usd": "DOLLAR12.2",
    "comma": "COMMA12.", "number": "COMMA12.", "comma2": "COMMA12.2",
    "percent": "PERCENT8.1", "pct": "PERCENT8.1",
    "int": "COMMA12.", "integer": "COMMA12.",
}


def _norm_format(fmt: Optional[str]) -> Optional[str]:
    if not fmt:
        return None
    return _FORMAT_ALIASES.get(str(fmt).strip().lower(), fmt)


def _norm_agg(agg: Optional[str]) -> Optional[str]:
    if not agg:
        return None
    a = str(agg).strip().lower()
    a = {"avg": "average", "mean": "average"}.get(a, a)
    return a if a in _AGGREGATIONS else None


_CALC_OPS = {"div", "mult", "sum", "minus", "ratio"}


def _bi_calc(name: str, label: str, num_alias: str, den_alias: str,
             op: str = "div", fmt: Optional[str] = None) -> dict:
    """An aggregated calculated measure — a ratio/product/etc. of two columns, e.g.
    Profit Margin = sum(Profit)/sum(Sales). ``num_alias``/``den_alias`` are the
    names of the two source DataItems it references."""
    op = {"ratio": "div", "mult": "mult", "x": "mult"}.get(str(op).lower(), str(op).lower())
    if op not in ("div", "mult", "sum", "minus"):
        op = "div"
    expr = (f"{op}(aggregate(sum,group,${{{num_alias},raw}}),"
            f"aggregate(sum,group,${{{den_alias},raw}}))")
    d = {"@element": "AggregateCalculatedItem", "name": name, "label": label,
         "usage": "quantitative", "aggregation": "sum", "dataType": "double",
         "expression": {"@element": "Expression", "value": expr}}
    f = _norm_format(fmt)
    if f:
        d["format"] = f
    return d


# Friendly aggregation-context words accepted in a calc expression's ${Col|agg|ctx}
# token.  SAS uses 'all' (across every row) and 'group' (within the visual's
# grouping) as the second argument of aggregate(...).
_CALC_CONTEXTS = {"forall": "all", "all": "all", "bygroup": "group", "group": "group"}
# Aggregation function names usable inside a calc expression.
_CALC_AGGS = {"sum", "average", "avg", "mean", "min", "max", "median",
              "count", "countdistinct", "stddev"}


def _parse_calc_expression(expression: str, start_index: int = 900):
    """Translate a *friendly* VA calculation expression into a BIRD expression.

    Columns are referenced as ``${ColumnName}`` (a measure, emitted as ``,raw``)
    or ``${ColumnName,binned}`` (a category).  Each distinct column becomes a
    DataItem named ``biNNN`` (digit names — SAS's expression parser rejects the
    underscore aliases elsewhere in the model inside ``${...}``) and the token is
    rewritten to ``${biNNN,<modifier>}``.

    Returns ``(rewritten_expression, [DataItem, ...], {column: bi_name})``.

    Example::

        minus(aggregate(average,all,${Sales}),aggregate(average,all,${Returns}))
    """
    items: list = []
    col_to_bi: dict = {}
    counter = [start_index]

    def repl(m: "re.Match") -> str:
        inner = m.group(1)
        parts = [p.strip() for p in inner.split(",")]
        col = parts[0]
        modifier = parts[1] if len(parts) > 1 and parts[1] else "raw"
        if col not in col_to_bi:
            name = f"bi{counter[0]}"
            counter[0] += 1
            usage = "categorical" if modifier == "binned" else "quantitative"
            items.append(_bi_item(name, col, col, usage))
            col_to_bi[col] = name
        return "${" + col_to_bi[col] + "," + modifier + "}"

    rewritten = re.sub(r"\$\{([^}]+)\}", repl, expression)
    return rewritten, items, col_to_bi


def _bi_calc_expr(name: str, label: str, expression: str,
                  fmt: Optional[str] = None, user_repr: Optional[str] = None) -> dict:
    """An ``AggregateCalculatedItem`` from a fully-formed BIRD ``expression``
    (already rewritten to ``${biNNN,...}`` refs).  Mirrors a SAS-VA-authored
    calculated measure: name/label/format/dataType/expression, plus an optional
    ``USER_EDIT_REPRESENTATION`` editor property carrying the human formula."""
    d = {"name": name, "label": label, "@element": "AggregateCalculatedItem",
         "dataType": "double",
         "expression": {"@element": "Expression", "value": expression}}
    f = _norm_format(fmt)
    if f:
        d["format"] = f
    if user_repr:
        d["editorProperties"] = [{
            "@element": "Editor_Property",
            "key": "USER_EDIT_REPRESENTATION", "value": user_repr}]
    return d


def _calc_measure_items(calc_expression: str, label: str,
                        fmt: Optional[str] = None, user_repr: Optional[str] = None,
                        meas_name: str = "bi_meas") -> list:
    """Build the data-source business items for a custom calculated measure: the
    per-column source DataItems referenced by the expression plus the
    ``AggregateCalculatedItem`` named *meas_name* (so it slots into the existing
    ``bi_meas`` measure alias path)."""
    expr, src_items, _ = _parse_calc_expression(calc_expression)
    calc_item = _bi_calc_expr(meas_name, label, expr, fmt,
                              user_repr or calc_expression)
    return src_items + [calc_item]


# ── filters ───────────────────────────────────────────────────────────────────
# A filter is applied to a ParentDataDefinition via an AppliedFilters block that
# references filter business items also stored on that data definition.  A
# RelationalFilterItem (detail filter) filters category VALUES; an
# AggregateFilterItem filters by a measure RANGE/comparison.  Both reference a
# RelationalDataItem alias (digit-named) over the filtered source column.

# op -> (filter element, value modifier, is_aggregate)
_FILTER_OPS = {
    "in":          ("RelationalFilterItem", "binned", False),
    "notin":       ("RelationalFilterItem", "binned", False),
    "between":     ("AggregateFilterItem",  "raw",    True),
    "greaterthan": ("AggregateFilterItem",  "raw",    True),
    "lessthan":    ("AggregateFilterItem",  "raw",    True),
    "greaterthanorequals": ("AggregateFilterItem", "raw", True),
    "lessthanorequals":    ("AggregateFilterItem", "raw", True),
    "equals":      ("AggregateFilterItem",  "raw",    True),
}


def _sas_str_list(values: list) -> str:
    """Render python values as the SAS quoted, comma-joined argument list used
    inside ``in(...)`` — strings single-quoted (and SAS-escaped), numbers bare."""
    out = []
    for v in values:
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            out.append(str(v))
        else:
            out.append("'" + str(v).replace("'", "''") + "'")
    return ",".join(out)


def _build_filters(filters: list, start_index: int = 800):
    """Translate friendly filter specs into BIRD filter machinery.

    Each spec is a dict::

        {"column": "State", "op": "in", "values": ["Assam", "Goa"]}
        {"column": "Age",   "op": "between", "min": 29, "max": 46}
        {"column": "Sales", "op": "greaterThan", "value": 1000}

    ``include_missing`` (default True) wraps the predicate in
    ``or(<pred>, ismissing(<ref>))`` exactly as SAS VA does so rows with missing
    values are not silently dropped.

    Returns ``(source_items, aliases, filter_items, applied_filters)`` where
    *source_items* are DataItems (add to the data source's businessItemFolder),
    *aliases* are RelationalDataItems (add to the ParentDataDefinition's
    businessItems), *filter_items* are the Relational/Aggregate filter items
    (also businessItems of the ParentDataDefinition), and *applied_filters* is
    the ``AppliedFilters`` dict (or ``None`` when *filters* is empty).
    """
    source_items: list = []
    aliases: list = []
    filter_items: list = []
    detail_names: list = []
    aggregate_names: list = []
    counter = [start_index]

    for spec in filters or []:
        spec = dict(spec)
        col = spec.get("column")
        op = str(spec.get("op", "in")).strip().lower()
        if not col:
            raise ValueError("Each filter needs a 'column'.")
        if op not in _FILTER_OPS:
            raise ValueError(
                f"Unknown filter op '{op}'. Valid: {sorted(_FILTER_OPS)}")
        element, modifier, is_agg = _FILTER_OPS[op]
        include_missing = spec.get("include_missing", True)

        src_name = f"bi{counter[0]}"
        ali_name = f"bi{counter[0] + 1}"
        flt_name = f"bi{counter[0] + 2}"
        counter[0] += 3

        usage = "quantitative" if is_agg else "categorical"
        source_items.append(_bi_item(src_name, col, col, usage))
        aliases.append(_alias(ali_name, src_name))

        ref = "${" + ali_name + "," + modifier + "}"
        if op in ("in", "notin"):
            values = spec.get("values") or []
            if not values:
                raise ValueError(f"Filter on '{col}' with op '{op}' needs 'values'.")
            pred = f"in({ref},{_sas_str_list(values)})"
            if op == "notin":
                pred = f"not({pred})"
        elif op == "between":
            lo, hi = spec.get("min"), spec.get("max")
            if lo is None or hi is None:
                raise ValueError(f"Filter on '{col}' with op 'between' needs 'min' and 'max'.")
            pred = f"between({ref},{lo},{hi})"
        else:
            val = spec.get("value")
            if val is None:
                raise ValueError(f"Filter on '{col}' with op '{op}' needs 'value'.")
            sas_op = {"greaterthan": "greaterThan", "lessthan": "lessThan",
                      "greaterthanorequals": "greaterThanOrEquals",
                      "lessthanorequals": "lessThanOrEquals",
                      "equals": "eq"}[op]
            val_s = _sas_str_list([val])
            pred = f"{sas_op}({ref},{val_s})"

        expr = f"or({pred},ismissing({ref}))" if include_missing else pred
        filter_items.append({
            "name": flt_name,
            "expression": {"@element": "Expression", "value": expr},
            "editorProperties": [
                {"@element": "Editor_Property", "key": "complexity",
                 "value": "SINGLE_DATA_ITEM"},
                {"@element": "Editor_Property", "key": "interactiveEditingAllowed",
                 "value": "TRUE"},
            ],
            "@element": element,
        })
        (aggregate_names if is_agg else detail_names).append(flt_name)

    applied = None
    if filter_items:
        applied = {"@element": "AppliedFilters"}
        if aggregate_names:
            applied["aggregateFilters"] = aggregate_names
        if detail_names:
            applied["detailFilters"] = detail_names
    return source_items, aliases, filter_items, applied


def apply_filters(content: dict, filters: Optional[list],
                  start_index: int = 800) -> dict:
    """Post-process an assembled single-object report, applying *filters* to it.

    Adds the filter source DataItems to the data source's businessItemFolder and
    the RelationalDataItem aliases + Relational/Aggregate FilterItems and the
    ``AppliedFilters`` block to the (first) ParentDataDefinition. Mutates and
    returns *content*. A no-op when *filters* is falsy."""
    if not filters:
        return content
    src, ali, fitems, applied = _build_filters(filters, start_index)
    if not applied:
        return content
    content["dataSources"][0]["businessItemFolder"]["items"].extend(src)
    # The ParentDataDefinition is the first data definition (dd1 for charts).
    pdd = next((d for d in content["dataDefinitions"]
                if d.get("@element") == "ParentDataDefinition"),
               content["dataDefinitions"][0])
    pdd["businessItems"] = pdd.get("businessItems", []) + ali + fitems
    pdd["appliedFilters"] = applied
    return content


def _bi_item(name: str, label: str, column: str, usage: str = "categorical",
             aggregation: Optional[str] = None, fmt: Optional[str] = None) -> dict:
    d = {"@element": "DataItem", "name": name, "label": label,
         "usage": usage, "xref": column}
    agg = _norm_agg(aggregation)
    if agg:
        d["aggregation"] = agg
    f = _norm_format(fmt)
    if f:
        d["format"] = f
    return d


def _bi_freq(name: str = "bi_freq") -> dict:
    return {"@element": "DataSource_PredefinedDataItem", "name": name,
            "label": "Frequency", "usage": "quantitative",
            "format": "COMMA12.", "calculation": "totalCount"}


def _bi_freq_pct(name: str = "bi_fpct") -> dict:
    return {"@element": "DataSource_PredefinedDataItem", "name": name,
            "label": "Frequency Percent", "usage": "quantitative",
            "format": "PERCENT20.2", "calculation": "totalCountPercent"}


def _alias(name: str, base: str) -> dict:
    return {"@element": "RelationalDataItem", "name": name, "base": base}


def _axis(axis_type: str, items: list) -> dict:
    return {"@element": "Query_Axis", "type": axis_type, "itemList": items}


def _sort_desc(ref: str) -> dict:
    return {"@element": "MeasureSortItem", "sortDirection": "descending", "reference": ref}


def _sort_asc(ref: str) -> dict:
    return {"@element": "SortItem", "sortDirection": "ascending", "reference": ref}


# ── GTML builders ─────────────────────────────────────────────────────────────

def _ln(*parts: str) -> str:
    """Join GTML lines with \r\n."""
    return "\r\n".join(parts) + "\r\n"


def _gtml_bar(result: str, freq_r: str, cat_r: str, grp_r: str,
              orient: str = "horizontal", group_display: str = "cluster") -> str:
    tip1 = f"                    <Value>{result}.{freq_r}</Value>"
    tip2 = f"                    <Value>{result}.{cat_r}</Value>"
    grp_default = f"                    <Value>{result}.{grp_r}</Value>"
    dmap = f'            <Entry model="LayoutDataMatrix" data="{result}"/>'
    return _ln(
        '<StatGraph border="false" opaque="false" includeMissingDiscrete="true"'
        ' selectionMode="multiple" missingValueDisplay="autolabel"'
        ' overplottingPolicy="REDUCEMARKERSIZE" displayOptionPolicy="union">',
        "    <Dimension/>",
        '    <PadAttrs top="0px" bottom="0px" left="0px" right="0px"/>',
        "    <Dimension/>",
        "    <Meta>",
        "        <DynVars>",
        '            <DynVar name="CATEGORY" description="CATEGORY_VAR"'
        ' required="true" assignedType="character" type="character">',
        "                <DefaultValues>",
        f"                    <Value>{result}.{cat_r}</Value>",
        "                </DefaultValues>",
        "            </DynVar>",
        '            <DynVar name="RESPONSE" description="MEASURE_VAR"'
        ' required="true" assignedType="numeric" type="numeric" multiplesAllowed="true">',
        "                <DefaultValues>",
        f"                    <Value>{result}.{freq_r}</Value>",
        "                </DefaultValues>",
        "            </DynVar>",
        '            <DynVar name="GROUP" description="SUBGROUP_VAR"'
        ' required="false" assignedType="character" type="character">',
        "                <DefaultValues>",
        grp_default,
        "                </DefaultValues>",
        "            </DynVar>",
        '            <DynVar name="COLUMN" description="HORIZONTAL_SERIES_VAR"'
        ' required="false" type="character" multiplesAllowed="true"/>',
        '            <DynVar name="ROW" description="VERTICAL_SERIES_VAR"'
        ' required="false" type="character" multiplesAllowed="true"/>',
        '            <DynVar name="TIP" description="TIP_VAR" required="false"'
        ' assignedType="character" type="any" multiplesAllowed="true">',
        "                <DefaultValues>",
        tip1,
        tip2,
        "                </DefaultValues>",
        "            </DynVar>",
        '            <DynVar name="KEY_FRAME" description="ANIMATION"'
        ' required="false" type="time" multiplesAllowed="false"/>',
        '            <DynVar name="HIDDEN" description="HIDDEN_VAR"'
        ' required="false" type="character" multiplesAllowed="true"/>',
        "        </DynVars>",
        "        <DataNameMap>",
        dmap,
        "        </DataNameMap>",
        "    </Meta>",
        '    <LayoutDataMatrix cellHeightMin="1px" cellWidthMin="1px"'
        ' includeMissingClass="true" rowVars="ROW" columnVars="COLUMN"'
        ' name="LayoutDataMatrix">',
        '        <LayoutPrototypeOverlay2D wallDisplay="NONE">',
        f'            <BarChartParm name="BarChart" tipListPolicy="replace"'
        f' _stmt="barchart" compactLabelFormats="true" groupDisplay="{group_display}"'
        f' orient="{orient}" stat="none" tip="TIP" category="CATEGORY"'
        f' responseVars="RESPONSE" group="GROUP">',
        '                <CompactLabelFormatOpts scaleType="value" precisionType="number"/>',
        "            </BarChartParm>",
        ('            <XAxisOpts name="categoryAxis">'
         if orient == "horizontal" else
         '            <YAxisOpts name="categoryAxis">'),
        "                <DiscreteOpts sortOrder=\"data\"/>",
        "                <LinearOpts>",
        '                    <TickValueFormatOpts extractScale="true"/>',
        "                </LinearOpts>",
        '            </XAxisOpts>' if orient == "horizontal" else '            </YAxisOpts>',
        ('            <YAxisOpts reverse="true">' if orient == "horizontal"
         else '            <XAxisOpts>'),
        '                <DiscreteOpts tickValueFitPolicy="thin" sortOrder="data"'
        ' tickValueAppearance="auto"/>',
        "                <LinearOpts>",
        '                    <TickValueFormatOpts extractScale="true"/>',
        "                </LinearOpts>",
        '            </YAxisOpts>' if orient == "horizontal" else '            </XAxisOpts>',
        "        </LayoutPrototypeOverlay2D>",
        "    </LayoutDataMatrix>",
        '    <LayoutGlobalLegend legendTitlePosition="top" allowCollapsed="true">',
        "        <AutoLegend>",
        "            <GraphNames>",
        "                <Value>BarChart</Value>",
        "            </GraphNames>",
        "        </AutoLegend>",
        "    </LayoutGlobalLegend>",
        '    <Animation keyFrameSortOrder="ascending_unformatted" keyFrame="KEY_FRAME"/>',
        '    <OverviewAxis maxPlotSize="60px" minPlotSize="35px"'
        ' reverseOrthogonalAxis="true" visible="off" axis="categoryAxis"/>',
        "</StatGraph>",
    )


def _gtml_line(result: str, x_r: str, y_r: str, grp_r: Optional[str]) -> str:
    grp_block = ""
    if grp_r:
        grp_block = _ln(
            '            <DynVar name="GROUP" description="SUBGROUP_VAR"'
            ' required="false" assignedType="character" type="character">',
            "                <DefaultValues>",
            f"                    <Value>{result}.{grp_r}</Value>",
            "                </DefaultValues>",
            "            </DynVar>",
        )
    else:
        grp_block = ('            <DynVar name="GROUP" description="SUBGROUP_VAR"'
                     ' required="false" assignedType="character" type="character"/>\r\n')
    return _ln(
        '<StatGraph border="false" opaque="false" includeMissingDiscrete="true"'
        ' selectionMode="multiple" displayOptionPolicy="union">',
        "    <Dimension/>",
        '    <PadAttrs top="0px" bottom="0px" left="0px" right="0px"/>',
        "    <Dimension/>",
        "    <Meta>",
        "        <DynVars>",
        '            <DynVar name="X" description="CATEGORY_VAR"'
        ' required="true" assignedType="any" type="any">',
        "                <DefaultValues>",
        f"                    <Value>{result}.{x_r}</Value>",
        "                </DefaultValues>",
        "            </DynVar>",
        '            <DynVar name="Y" description="MEASURE_VAR"'
        ' required="true" assignedType="numeric" type="numeric" multiplesAllowed="true">',
        "                <DefaultValues>",
        f"                    <Value>{result}.{y_r}</Value>",
        "                </DefaultValues>",
        "            </DynVar>",
    ) + grp_block + _ln(
        '            <DynVar name="TIP" description="TIP_VAR" required="false"'
        ' type="any" multiplesAllowed="true">',
        "                <DefaultValues>",
        f"                    <Value>{result}.{x_r}</Value>",
        f"                    <Value>{result}.{y_r}</Value>",
        "                </DefaultValues>",
        "            </DynVar>",
        "        </DynVars>",
        "        <DataNameMap>",
        f'            <Entry model="LayoutDataMatrix" data="{result}"/>',
        "        </DataNameMap>",
        "    </Meta>",
        '    <LayoutDataMatrix cellHeightMin="1px" cellWidthMin="1px" name="LayoutDataMatrix">',
        '        <LayoutPrototypeOverlay2D wallDisplay="NONE">',
        '            <SeriesPlot name="Series" compactLabelFormats="true"'
        ' x="X" y="Y" group="GROUP" tip="TIP">',
        '                <CompactLabelFormatOpts scaleType="value" precisionType="number"/>',
        "            </SeriesPlot>",
        '            <XAxisOpts name="xAxis">',
        '                <DiscreteOpts sortOrder="data"/>',
        "                <LinearOpts>",
        '                    <TickValueFormatOpts extractScale="true"/>',
        "                </LinearOpts>",
        "            </XAxisOpts>",
        "            <YAxisOpts>",
        "                <LinearOpts>",
        '                    <TickValueFormatOpts extractScale="true"/>',
        "                </LinearOpts>",
        "            </YAxisOpts>",
        "        </LayoutPrototypeOverlay2D>",
        "    </LayoutDataMatrix>",
        '    <LayoutGlobalLegend legendTitlePosition="top" allowCollapsed="true">',
        "        <AutoLegend>",
        "            <GraphNames>",
        "                <Value>Series</Value>",
        "            </GraphNames>",
        "        </AutoLegend>",
        "    </LayoutGlobalLegend>",
        "</StatGraph>",
    )


def _gtml_pie(result: str, cat_r: str, freq_r: str, fpct_r: Optional[str] = None,
              display: str = "PIEHEADER") -> str:
    """Pie chart GTML — matches the SAS VA HAR capture exactly.

    ``display='DONUT'`` renders a donut (pie with a hole). ``fpct_r=None`` draws a
    single ring (the measure pie); passing a Frequency-Percent alias adds the
    second concentric ring of the classic count pie."""
    def _dvals(*aliases: str) -> str:
        inner = "\r\n".join(f"                    <Value>{result}.{a}</Value>" for a in aliases)
        return "                <DefaultValues>\r\n" + inner + "\r\n                </DefaultValues>"
    _resp_block = _dvals(*([freq_r] + ([fpct_r] if fpct_r else [])))
    _tip_block = _dvals(*([cat_r, freq_r] + ([fpct_r] if fpct_r else [])))
    return _ln(
        '<StatGraph border="false" opaque="false" includeMissingDiscrete="true"'
        ' selectionMode="multiple" missingValueDisplay="autolabel"'
        ' overplottingPolicy="REDUCEMARKERSIZE" displayOptionPolicy="union">',
        "    <Dimension/>",
        '    <PadAttrs top="0px" bottom="0px" left="0px" right="0px"/>',
        "    <Dimension/>",
        "    <Meta>",
        "        <DynVars>",
        '            <DynVar name="CATEGORY" description="CATEGORY_VAR"'
        ' required="true" assignedType="character" type="character">',
        "                <DefaultValues>",
        f"                    <Value>{result}.{cat_r}</Value>",
        "                </DefaultValues>",
        "            </DynVar>",
        '            <DynVar name="RESPONSE" description="MEASURE_VAR"'
        ' required="true" assignedType="numeric" type="numeric" multiplesAllowed="true">',
        _resp_block,
        "            </DynVar>",
        '            <DynVar name="GROUP" description="SUBGROUP_VAR"'
        ' required="false" type="character"/>',
        '            <DynVar name="COLUMN" description="HORIZONTAL_SERIES_VAR"'
        ' required="false" type="character" multiplesAllowed="true"/>',
        '            <DynVar name="ROW" description="VERTICAL_SERIES_VAR"'
        ' required="false" type="character" multiplesAllowed="true"/>',
        '            <DynVar name="TIP" description="TIP_VAR" required="false"'
        ' assignedType="numeric" type="any" multiplesAllowed="true">',
        _tip_block,
        "            </DynVar>",
        '            <DynVar name="KEY_FRAME" description="ANIMATION"'
        ' required="false" type="time" multiplesAllowed="false"/>',
        '            <DynVar name="HIDDEN" description="HIDDEN_VAR"'
        ' required="false" type="character" multiplesAllowed="true"/>',
        "        </DynVars>",
        "    </Meta>",
        '    <LayoutDataMatrix cellHeightMin="1px" cellWidthMin="1px"'
        ' includeMissingClass="true" rowVars="ROW" columnVars="COLUMN">',
        "        <LayoutPrototypeRegion>",
        f'            <PieChart name="PieChart" tipListPolicy="replace" _stmt="piechart"'
        f' compactLabelFormats="true" categoryOrder="data" display="{display}" start="90"'
        ' stat="none" dataLabelContent="NONE" includeMissingGroup="true"'
        ' tip="TIP" category="CATEGORY" responseVars="RESPONSE" group="GROUP">',
        '                <CompactLabelFormatOpts scaleType="value" precisionType="number"/>',
        "            </PieChart>",
        "        </LayoutPrototypeRegion>",
        "    </LayoutDataMatrix>",
        '    <LayoutGlobalLegend legendTitlePosition="top" allowCollapsed="true">',
        "        <AutoLegend>",
        "            <GraphNames>",
        "                <Value>PieChart</Value>",
        "            </GraphNames>",
        "        </AutoLegend>",
        "    </LayoutGlobalLegend>",
        '    <Animation keyFrameSortOrder="ascending_unformatted" keyFrame="KEY_FRAME"/>',
        "</StatGraph>",
    )


def _gtml_scatter(result: str, x_r: str, y_r: str, grp_r: Optional[str]) -> str:
    grp_block = ""
    if grp_r:
        grp_block = _ln(
            '            <DynVar name="GROUP" description="SUBGROUP_VAR"'
            ' required="false" assignedType="character" type="character">',
            "                <DefaultValues>",
            f"                    <Value>{result}.{grp_r}</Value>",
            "                </DefaultValues>",
            "            </DynVar>",
        )
    else:
        grp_block = ('            <DynVar name="GROUP" description="SUBGROUP_VAR"'
                     ' required="false" assignedType="character" type="character"/>\r\n')
    return _ln(
        '<StatGraph border="false" opaque="false" selectionMode="multiple"'
        ' displayOptionPolicy="union">',
        "    <Dimension/>",
        '    <PadAttrs top="0px" bottom="0px" left="0px" right="0px"/>',
        "    <Dimension/>",
        "    <Meta>",
        "        <DynVars>",
        '            <DynVar name="X" description="X_VAR"'
        ' required="true" assignedType="numeric" type="numeric">',
        "                <DefaultValues>",
        f"                    <Value>{result}.{x_r}</Value>",
        "                </DefaultValues>",
        "            </DynVar>",
        '            <DynVar name="Y" description="Y_VAR"'
        ' required="true" assignedType="numeric" type="numeric">',
        "                <DefaultValues>",
        f"                    <Value>{result}.{y_r}</Value>",
        "                </DefaultValues>",
        "            </DynVar>",
    ) + grp_block + _ln(
        '            <DynVar name="TIP" description="TIP_VAR" required="false"'
        ' type="any" multiplesAllowed="true">',
        "                <DefaultValues>",
        f"                    <Value>{result}.{x_r}</Value>",
        f"                    <Value>{result}.{y_r}</Value>",
        "                </DefaultValues>",
        "            </DynVar>",
        "        </DynVars>",
        "        <DataNameMap>",
        f'            <Entry model="LayoutDataMatrix" data="{result}"/>',
        "        </DataNameMap>",
        "    </Meta>",
        '    <LayoutDataMatrix cellHeightMin="1px" cellWidthMin="1px" name="LayoutDataMatrix">',
        '        <LayoutPrototypeOverlay2D wallDisplay="NONE">',
        '            <ScatterPlot name="Scatter" x="X" y="Y" group="GROUP" tip="TIP"/>',
        "            <XAxisOpts>",
        "                <LinearOpts>",
        '                    <TickValueFormatOpts extractScale="true"/>',
        "                </LinearOpts>",
        "            </XAxisOpts>",
        "            <YAxisOpts>",
        "                <LinearOpts>",
        '                    <TickValueFormatOpts extractScale="true"/>',
        "                </LinearOpts>",
        "            </YAxisOpts>",
        "        </LayoutPrototypeOverlay2D>",
        "    </LayoutDataMatrix>",
        '    <LayoutGlobalLegend legendTitlePosition="top" allowCollapsed="true">',
        "        <AutoLegend>",
        "            <GraphNames>",
        "                <Value>Scatter</Value>",
        "            </GraphNames>",
        "        </AutoLegend>",
        "    </LayoutGlobalLegend>",
        "</StatGraph>",
    )


def _gtml_bubble(result: str, x_r: str, y_r: str, size_r: str,
                 grp_r: Optional[str]) -> str:
    grp_block = ""
    if grp_r:
        grp_block = _ln(
            '            <DynVar name="GROUP" description="SUBGROUP_VAR"'
            ' required="false" assignedType="character" type="character">',
            "                <DefaultValues>",
            f"                    <Value>{result}.{grp_r}</Value>",
            "                </DefaultValues>",
            "            </DynVar>",
        )
    else:
        grp_block = ('            <DynVar name="GROUP" description="SUBGROUP_VAR"'
                     ' required="false" assignedType="character" type="character"/>\r\n')
    return _ln(
        '<StatGraph border="false" opaque="false" selectionMode="multiple"'
        ' displayOptionPolicy="union">',
        "    <Dimension/>",
        '    <PadAttrs top="0px" bottom="0px" left="0px" right="0px"/>',
        "    <Dimension/>",
        "    <Meta>",
        "        <DynVars>",
        '            <DynVar name="X" description="X_VAR"'
        ' required="true" assignedType="numeric" type="numeric">',
        "                <DefaultValues>",
        f"                    <Value>{result}.{x_r}</Value>",
        "                </DefaultValues>",
        "            </DynVar>",
        '            <DynVar name="Y" description="Y_VAR"'
        ' required="true" assignedType="numeric" type="numeric">',
        "                <DefaultValues>",
        f"                    <Value>{result}.{y_r}</Value>",
        "                </DefaultValues>",
        "            </DynVar>",
        '            <DynVar name="SIZE" description="SIZE_VAR"'
        ' required="false" assignedType="numeric" type="numeric">',
        "                <DefaultValues>",
        f"                    <Value>{result}.{size_r}</Value>",
        "                </DefaultValues>",
        "            </DynVar>",
    ) + grp_block + _ln(
        '            <DynVar name="TIP" description="TIP_VAR" required="false"'
        ' type="any" multiplesAllowed="true">',
        "                <DefaultValues>",
        f"                    <Value>{result}.{x_r}</Value>",
        f"                    <Value>{result}.{y_r}</Value>",
        "                </DefaultValues>",
        "            </DynVar>",
        "        </DynVars>",
        "        <DataNameMap>",
        f'            <Entry model="LayoutDataMatrix" data="{result}"/>',
        "        </DataNameMap>",
        "    </Meta>",
        '    <LayoutDataMatrix cellHeightMin="1px" cellWidthMin="1px" name="LayoutDataMatrix">',
        '        <LayoutPrototypeOverlay2D wallDisplay="NONE">',
        '            <BubblePlot name="Bubble" x="X" y="Y" size="SIZE"'
        ' group="GROUP" tip="TIP"/>',
        "            <XAxisOpts>",
        "                <LinearOpts>",
        '                    <TickValueFormatOpts extractScale="true"/>',
        "                </LinearOpts>",
        "            </XAxisOpts>",
        "            <YAxisOpts>",
        "                <LinearOpts>",
        '                    <TickValueFormatOpts extractScale="true"/>',
        "                </LinearOpts>",
        "            </YAxisOpts>",
        "        </LayoutPrototypeOverlay2D>",
        "    </LayoutDataMatrix>",
        '    <LayoutGlobalLegend legendTitlePosition="top" allowCollapsed="true">',
        "        <AutoLegend>",
        "            <GraphNames>",
        "                <Value>Bubble</Value>",
        "            </GraphNames>",
        "        </AutoLegend>",
        "    </LayoutGlobalLegend>",
        "</StatGraph>",
    )


# ── title helpers ────────────────────────────────────────────────────────────

def _dynamic_span(key: str, measure_alias: str, category_alias: str) -> dict:
    """Return a DynamicSpan dict for use in a Graph title paragraphList.

    Two label substitutions only — [measure, category]. The connector word
    ("of") is supplied by the template (e.g. oneOfTwo.fmt.txt). An earlier
    middle ``bird.autotitle.category.txt`` substitution hijacked the category
    slot, so the title rendered the literal placeholder "Category" instead of
    the real column label (e.g. "Age of Category" rather than "Age of AccountType").
    """
    return {
        "@element": "DynamicSpan",
        "dynamicSpanKey": key,
        "substitutions": [
            {"@element": "Substitution", "valueType": "label",
             "itemsList": [measure_alias]},
            {"@element": "Substitution", "valueType": "label",
             "itemsList": [category_alias]},
        ],
    }


# ── shared BIRD scaffold ───────────────────────────────────────────────────────

def _assemble(
    report_name: str,
    cas_server: str,
    cas_library: str,
    cas_table: str,
    table_label: str,
    bi_items: list,
    dd1_bis: list,
    axes: list,
    sort_items: list,
    detail: bool,
    graph_type: str,
    graph_label: str,
    gtml: str,
    source_interactions: list,
    title_span: dict,
    now: str,
    dd_status: str = "executable",
    max_rows_behavior: str = "truncate",
    extra_editor_props: Optional[list] = None,
    table_columns: Optional[list] = None,    # Table_Column dicts for pie
    table_interactions: Optional[list] = None,
    filter_items: Optional[list] = None,      # Relational/Aggregate FilterItem dicts
    applied_filters: Optional[dict] = None,   # AppliedFilters block (or None)
) -> dict:
    return {
        "@element": "SASReport",
        "xmlns": "http://www.sas.com/sasreportmodel/bird-4.64.0",
        "label": report_name,
        "createdApplicationName": "SAS Visual Analytics",
        "dateModified": now,
        "lastModifiedApplicationName": "SAS Visual Analytics",
        "createdLocale": "en_US",
        "features": ["promptModelV2"],
        "implicitInteractions": ["reportPrompt", "sectionPrompt", "sectionLink"],
        "nextUniqueNameIndex": 20,
        "dataDefinitions": [{
            "@element": "ParentDataDefinition",
            "name": "dd1",
            "businessItems": dd1_bis + (filter_items or []),
            "source": "ds1",
            "childQueryRelationshipType": "independent",
            **({"appliedFilters": applied_filters} if applied_filters else {}),
            "dataDefinitionList": [{
                "@element": "DataDefinition",
                "name": "dd2",
                "type": "multidimensional",
                "multidimensionalQueryList": [{
                    "@element": "MultidimensionalQuery",
                    "detail": detail,
                    "axes": axes,
                    **({"columnSortItems": sort_items} if sort_items else {}),
                }],
                "source": "ds1",
                "resultDefinitions": [{
                    "@element": "ResultDefinition",
                    "name": "dd_res",
                    "purpose": "primary",
                    "maxRowsBehavior": max_rows_behavior,
                    "maxRowsLookup": "graphDefault",
                }],
            }],
            "status": dd_status,
        }],
        "dataSources": [{
            "@element": "DataSource",
            "name": "ds1",
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
                "items": bi_items,
            },
        }],
        "visualElements": [
            {
                "@element": "Graph",
                "name": "ve1",
                "labelAttribute": graph_label,
                "graphType": graph_type,
                "dataList": ["dd1"],
                "applyDynamicBrushes": "yes",
                "title": {
                    "@element": "Title",
                    "auto": True,
                    "paragraphList": [
                        {"@element": "P", "elements": [title_span]}
                    ],
                },
                "sourceInteractionVariableList": source_interactions,
                "editorProperties": [
                    {"@element": "Editor_Property", "key": "isAutoLabel", "value": "true"},
                    *(extra_editor_props or []),
                ],
                "resultDefinitionList": ["dd_res"],
                "supplementalVisualList": ["ve2"],
                "gtml": gtml,
            },
            {
                "@element": "Table",
                "name": "ve2",
                "applyDynamicBrushes": "yes",
                **({"sourceInteractionVariableList": table_interactions}
                   if table_interactions else {}),
                "resultDefinitionList": ["dd_res"],
                "dataList": ["dd1"],
                **({"columns": {
                    "@element": "Table_Columns",
                    "columns": table_columns,
                }} if table_columns else {}),
                "columnSizing": "autoFill",
            },
        ],
        "view": {
            "@element": "View",
            "sections": [{
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
                                {"@element": "Weights", "mediaTarget": "mt_large",
                                 "unit": "percent",
                                 "values": [{"@element": "Weight", "value": "100%"}]},
                                {"@element": "Weights", "mediaTarget": "mt_medium",
                                 "unit": "percent",
                                 "values": [{"@element": "Weight", "value": "100%"}]},
                                {"@element": "Weights", "mediaTarget": "mt_small",
                                 "unit": "percent",
                                 "values": [{"@element": "Weight", "value": "100%"}]},
                            ],
                        },
                        "containedElementList": [{
                            "@element": "Visual",
                            "name": "vi2",
                            "ref": "ve1",
                            "responsiveConstraint": {
                                "@element": "ResponsiveConstraint",
                                "widthConstraint": {
                                    "@element": "Responsive_WidthConstraint",
                                    "widths": [{"@element": "Width",
                                                "mediaTarget": "mt_small",
                                                "preferredSizeBehavior": "ignore",
                                                "flexibility": "flexible"}],
                                },
                                "heightConstraint": {
                                    "@element": "Responsive_HeightConstraint",
                                    "heights": [{"@element": "Height",
                                                 "mediaTarget": "mt_small",
                                                 "preferredSizeBehavior": "ignore",
                                                 "flexibility": "flexible"}],
                                },
                            },
                        }],
                    }],
                },
            }],
        },
        "mediaSchemes": [{
            "@element": "MediaScheme",
            "name": "ms1",
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
            {"@element": "Property", "key": "displayDataSource", "value": "ds1"}],
        "exportProperties": [{
            "@element": "Export",
            "destination": "pdf",
            "exportPropertyList": [
                {"@element": "Export_Property", "key": "showCoverPage",
                 "value": "true", "content": ""},
                {"@element": "Export_Property", "key": "showPageNumbers",
                 "value": "true", "content": ""},
            ],
        }],
        "history": {
            "@element": "History",
            "editors": [{
                "@element": "Editor",
                "applicationName": "VA",
                "revisions": [{
                    "@element": "Revision",
                    "editorVersion": "2020",
                    "lastDate": now,
                }],
            }],
        },
        "sasReportState": {
            "@element": "SASReportState",
            "view": {"@element": "View_State"},
        },
    }


# ── public API ─────────────────────────────────────────────────────────────────

def build_chart_content(
    chart_type: str,
    report_name: str,
    cas_server: str,
    cas_library: str,
    cas_table: str,
    # bar / pie / line
    category_column: Optional[str] = None,
    category_label: Optional[str] = None,
    measure_column: Optional[str] = None,   # None → use Frequency count
    measure_label: Optional[str] = None,
    # scatter / bubble
    x_column: Optional[str] = None,
    x_label: Optional[str] = None,
    y_column: Optional[str] = None,
    y_label: Optional[str] = None,
    size_column: Optional[str] = None,      # bubble only
    size_label: Optional[str] = None,
    # common
    group_column: Optional[str] = None,
    group_label: Optional[str] = None,
    table_label: Optional[str] = None,
    # measure aggregation (sum/average/min/max/median) + display format
    aggregation: Optional[str] = None,
    measure_format: Optional[str] = None,
    # calculated ratio measure (e.g. Profit Margin = Profit/Sales)
    calc_numerator: Optional[str] = None,
    calc_denominator: Optional[str] = None,
    calc_op: str = "div",
    # custom calculated measure — a friendly VA expression that references
    # columns as ${Col} / ${Col,binned} (e.g.
    #   "minus(aggregate(average,all,${Sales}),aggregate(average,all,${Returns}))")
    calc_expression: Optional[str] = None,
    calc_label: Optional[str] = None,
    calc_format: Optional[str] = None,
    calc_user_repr: Optional[str] = None,
    # data filters applied to this object (see _build_filters for the spec shape)
    filters: Optional[list] = None,
) -> dict:
    """Build a chart report, then apply any *filters* (post-process).

    Thin public wrapper over :func:`_build_chart_content` so data filters compose
    with every chart type without threading them through each branch."""
    content = _build_chart_content(
        chart_type=chart_type, report_name=report_name, cas_server=cas_server,
        cas_library=cas_library, cas_table=cas_table,
        category_column=category_column, category_label=category_label,
        measure_column=measure_column, measure_label=measure_label,
        x_column=x_column, x_label=x_label, y_column=y_column, y_label=y_label,
        size_column=size_column, size_label=size_label,
        group_column=group_column, group_label=group_label, table_label=table_label,
        aggregation=aggregation, measure_format=measure_format,
        calc_numerator=calc_numerator, calc_denominator=calc_denominator, calc_op=calc_op,
        calc_expression=calc_expression, calc_label=calc_label,
        calc_format=calc_format, calc_user_repr=calc_user_repr,
    )
    return apply_filters(content, filters)


def _build_chart_content(
    chart_type: str,
    report_name: str,
    cas_server: str,
    cas_library: str,
    cas_table: str,
    category_column: Optional[str] = None,
    category_label: Optional[str] = None,
    measure_column: Optional[str] = None,
    measure_label: Optional[str] = None,
    x_column: Optional[str] = None,
    x_label: Optional[str] = None,
    y_column: Optional[str] = None,
    y_label: Optional[str] = None,
    size_column: Optional[str] = None,
    size_label: Optional[str] = None,
    group_column: Optional[str] = None,
    group_label: Optional[str] = None,
    table_label: Optional[str] = None,
    aggregation: Optional[str] = None,
    measure_format: Optional[str] = None,
    calc_numerator: Optional[str] = None,
    calc_denominator: Optional[str] = None,
    calc_op: str = "div",
    calc_expression: Optional[str] = None,
    calc_label: Optional[str] = None,
    calc_format: Optional[str] = None,
    calc_user_repr: Optional[str] = None,
) -> dict:
    """
    Build a SASReport BIRD 4.64.0 JSON document for the requested chart type.

    chart_type values
    -----------------
    bar_h   – horizontal bar (default)
    bar_v   – vertical bar
    line    – line / series chart
    pie     – pie chart
    scatter – scatter plot  (requires x_column, y_column)
    bubble  – bubble chart  (requires x_column, y_column, size_column)
    """
    chart_type = chart_type.lower().strip()
    now = _now()
    table_label = table_label or cas_table

    # ── BAR (horizontal / vertical, clustered or stacked) ───────────────────
    if chart_type in ("bar_h", "bar_v", "bar",
                      "bar_stacked_v", "bar_stacked_h",
                      "stacked_bar_v", "stacked_bar_h"):
        cat_col = category_column or ""
        if not cat_col:
            raise ValueError(f"{chart_type} chart requires a category_column")
        cat_lbl = category_label or cat_col
        has_grp = bool(group_column and group_column != cat_col)
        grp_lbl = group_label or (group_column if has_grp else cat_lbl)
        has_calc_expr = bool(calc_expression)
        has_calc = bool(calc_numerator and calc_denominator)
        use_freq = not (bool(measure_column) or has_calc or has_calc_expr)
        if has_calc_expr:
            meas_lbl = calc_label or measure_label or "Calculation"
        elif has_calc:
            meas_lbl = measure_label or f"{calc_numerator}/{calc_denominator}"
        else:
            meas_lbl = measure_label or (measure_column if measure_column else "Frequency")

        bi_items = [
            _bi_item("bi_cat", cat_lbl, cat_col, "categorical"),
            _bi_freq(),
            _bi_freq_pct(),
        ]
        if has_calc_expr:
            bi_items.extend(_calc_measure_items(
                calc_expression, meas_lbl, calc_format or measure_format, calc_user_repr))
        elif has_calc:
            # A calculated ratio measure references two source columns. The two
            # source items MUST use bi+digit names (bi991/bi992) — SAS's
            # expression parser rejects underscore names inside ${...,raw}.
            bi_items.append(_bi_item("bi991", calc_numerator, calc_numerator, "quantitative"))
            bi_items.append(_bi_item("bi992", calc_denominator, calc_denominator, "quantitative"))
            bi_items.append(_bi_calc("bi_meas", meas_lbl, "bi991", "bi992", calc_op, measure_format))
        elif measure_column:
            bi_items.append(_bi_item("bi_meas", meas_lbl, measure_column, "quantitative",
                                     aggregation, measure_format))
        if has_grp:
            bi_items.append(_bi_item("bi_grp", grp_lbl, group_column, "categorical"))

        freq_src = "bi_freq" if use_freq else "bi_meas"
        dd1_bis = [
            _alias("bi_freq_r", freq_src),
            _alias("bi_cat_r", "bi_cat"),
        ]
        if has_grp:
            dd1_bis.append(_alias("bi_grp_r", "bi_grp"))

        # The group/subgroup goes on the COLUMN axis next to the category so the
        # query returns category×group cells and the bars cluster/stack by group.
        # (On the "page" axis it becomes small-multiples and the single rendered
        # view shows ungrouped bars titled "grouped by undefined".)
        axes = [
            _axis("column", ["bi_cat_r"] + (["bi_grp_r"] if has_grp else [])),
            _axis("row", ["bi_freq_r"]),
        ]

        sort_items = [_sort_desc("bi_freq_r"), _sort_asc("bi_cat_r")]
        orient = "horizontal" if chart_type in ("bar_h", "bar", "bar_stacked_h",
                                                 "stacked_bar_h") else "vertical"
        group_display = "stack" if "stacked" in chart_type else "cluster"
        grp_alias = "bi_grp_r" if has_grp else "bi_cat_r"
        gtml = _gtml_bar("dd_res", "bi_freq_r", "bi_cat_r", grp_alias, orient, group_display)

        graph_label = f"Bar - {meas_lbl} 1"
        si = ["bi_cat_r"] + (["bi_grp_r"] if has_grp else [])
        if has_grp:
            # "<measure> of <category> grouped by <group>" — 3 label substitutions.
            ts = {
                "@element": "DynamicSpan",
                "dynamicSpanKey": "bird.autotitle.template.oneOfTwoGroupedByThree.fmt.txt",
                "substitutions": [
                    {"@element": "Substitution", "valueType": "label", "itemsList": ["bi_freq_r"]},
                    {"@element": "Substitution", "valueType": "label", "itemsList": ["bi_cat_r"]},
                    {"@element": "Substitution", "valueType": "label", "itemsList": ["bi_grp_r"]},
                ],
            }
        else:
            ts = _dynamic_span(
                "bird.autotitle.template.oneOfTwo.fmt.txt",
                "bi_freq_r", "bi_cat_r")

        # A count-based bar (no explicit measure) aggregates via Frequency — VA
        # only binds that auto-measure when the graph declares it, otherwise the
        # whole query stays unbound and renders the "Category" placeholder.
        bar_ep = ([{"@element": "Editor_Property",
                    "key": "autoFrequencyQueryDataItemName", "value": "bi_freq_r"}]
                  if use_freq else None)

        return _assemble(report_name, cas_server, cas_library, cas_table, table_label,
                         bi_items, dd1_bis, axes, sort_items, False,
                         "bar", graph_label, gtml, si, ts, now,
                         extra_editor_props=bar_ep)

    # ── LINE ────────────────────────────────────────────────────────────────
    if chart_type == "line":
        x_col = x_column or category_column or ""
        x_lbl = x_label or category_label or x_col
        y_col = y_column or measure_column or ""
        y_lbl = y_label or measure_label or y_col
        if not x_col or not y_col:
            raise ValueError("line chart requires x_column and y_column")
        has_grp = bool(group_column)
        grp_lbl = group_label or group_column or ""

        bi_items = [
            _bi_item("bi_x", x_lbl, x_col, "categorical"),
            _bi_item("bi_y", y_lbl, y_col, "quantitative", aggregation, measure_format),
        ]
        if has_grp:
            bi_items.append(_bi_item("bi_grp", grp_lbl, group_column, "categorical"))

        dd1_bis = [_alias("bi_x_r", "bi_x"), _alias("bi_y_r", "bi_y")]
        if has_grp:
            dd1_bis.append(_alias("bi_grp_r", "bi_grp"))

        axes = [_axis("column", ["bi_x_r"]), _axis("row", ["bi_y_r"])]
        if has_grp:
            axes.append(_axis("page", ["bi_grp_r"]))

        sort_items = [_sort_asc("bi_x_r")]
        grp_r = "bi_grp_r" if has_grp else None
        gtml = _gtml_line("dd_res", "bi_x_r", "bi_y_r", grp_r)
        si = ["bi_x_r"] + (["bi_grp_r"] if has_grp else [])
        ts = _dynamic_span("bird.autotitle.template.oneOfTwo.fmt.txt",
                           "bi_y_r", "bi_x_r")

        return _assemble(report_name, cas_server, cas_library, cas_table, table_label,
                         bi_items, dd1_bis, axes, sort_items, False,
                         "line", f"Line - {y_lbl} 1", gtml, si, ts, now)

    # ── PIE / DONUT ─────────────────────────────────────────────────────────
    if chart_type in ("pie", "donut"):
        cat_col = category_column or ""
        if not cat_col:
            raise ValueError(f"{chart_type} chart requires a category_column")
        cat_lbl = category_label or cat_col
        # A pie/donut defaults to record COUNT (Frequency), but for an executive
        # view a measure (e.g. Sales) is far more useful — slices then size by the
        # measure's sum per category rather than transaction count.
        use_freq = not bool(measure_column)
        meas_lbl = measure_label or (measure_column if measure_column else "Frequency")

        bi_items = [
            _bi_item("bi_cat", cat_lbl, cat_col, "categorical"),
            _bi_freq(),
            _bi_freq_pct(),
        ]
        if measure_column:
            bi_items.append(_bi_item("bi_meas", meas_lbl, measure_column, "quantitative",
                                     aggregation, measure_format))

        # The primary response (slice size) is the measure when given, else Frequency.
        dd1_bis = [
            _alias("bi_freq_r", "bi_freq" if use_freq else "bi_meas"),
            _alias("bi_fpct_r", "bi_fpct"),
            _alias("bi_cat_r", "bi_cat"),
        ]

        # The count pie has a 2nd Frequency-Percent ring; a measure pie is a
        # single clean ring sized by the measure.
        row_items = ["bi_freq_r", "bi_fpct_r"] if use_freq else ["bi_freq_r"]
        axes = [
            _axis("column", ["bi_cat_r"]),
            _axis("row", row_items),
        ]
        sort_items = [_sort_desc("bi_freq_r"), _sort_asc("bi_cat_r")]
        pie_display = "DONUT" if chart_type == "donut" else "PIEHEADER"
        gtml = _gtml_pie("dd_res", "bi_cat_r", "bi_freq_r",
                         "bi_fpct_r" if use_freq else None, pie_display)

        # Title: "Frequency, Frequency Percent by <cat>" for a count pie, or the
        # cleaner "<measure> by <cat>" when a measure drives the slices.
        ts = {
            "@element": "DynamicSpan",
            "dynamicSpanKey": "bird.autotitle.template.oneByTwo.fmt.txt",
            "substitutions": [
                {"@element": "Substitution", "valueType": "label",
                 "itemsList": ["bi_freq_r", "bi_fpct_r"] if use_freq else ["bi_freq_r"]},
                {"@element": "Substitution", "valueType": "label",
                 "itemsList": ["bi_cat_r"]},
            ],
        }
        # autoFrequency only applies to the count-based pie.
        extra_ep = ([{"@element": "Editor_Property",
                      "key": "autoFrequencyQueryDataItemName", "value": "bi_freq_r"}]
                    if use_freq else None)
        tbl_cols = [
            {"@element": "Table_Column", "variable": "bi_cat_r"},
            {"@element": "Table_Column", "variable": "bi_freq_r"},
            {"@element": "Table_Column", "variable": "bi_fpct_r"},
        ]

        return _assemble(report_name, cas_server, cas_library, cas_table, table_label,
                         bi_items, dd1_bis, axes, sort_items, False,
                         "pie", f"Pie - {cat_lbl} 1", gtml, ["bi_cat_r"], ts, now,
                         dd_status="executable", max_rows_behavior="noData",
                         extra_editor_props=extra_ep,
                         table_columns=tbl_cols, table_interactions=["bi_cat_r"])

    # ── SCATTER ─────────────────────────────────────────────────────────────
    if chart_type == "scatter":
        x_col = x_column or ""
        x_lbl = x_label or x_col
        y_col = y_column or ""
        y_lbl = y_label or y_col
        if not x_col or not y_col:
            raise ValueError("scatter chart requires x_column and y_column")
        has_grp = bool(group_column)
        grp_lbl = group_label or group_column or ""

        bi_items = [
            _bi_item("bi_x", x_lbl, x_col, "quantitative"),
            _bi_item("bi_y", y_lbl, y_col, "quantitative"),
        ]
        if has_grp:
            bi_items.append(_bi_item("bi_grp", grp_lbl, group_column, "categorical"))

        dd1_bis = [_alias("bi_x_r", "bi_x"), _alias("bi_y_r", "bi_y")]
        if has_grp:
            dd1_bis.append(_alias("bi_grp_r", "bi_grp"))

        axes = [_axis("column", ["bi_x_r"]), _axis("row", ["bi_y_r"])]
        grp_r = "bi_grp_r" if has_grp else None
        gtml = _gtml_scatter("dd_res", "bi_x_r", "bi_y_r", grp_r)
        si = ["bi_x_r", "bi_y_r"] + (["bi_grp_r"] if has_grp else [])
        ts = _dynamic_span("bird.autotitle.template.oneOfTwo.fmt.txt",
                           "bi_y_r", "bi_x_r")

        return _assemble(report_name, cas_server, cas_library, cas_table, table_label,
                         bi_items, dd1_bis, axes, [], True,
                         "scatter", f"Scatter - {x_lbl} vs {y_lbl}", gtml, si, ts, now)

    # ── BUBBLE ──────────────────────────────────────────────────────────────
    if chart_type == "bubble":
        x_col = x_column or ""
        x_lbl = x_label or x_col
        y_col = y_column or ""
        y_lbl = y_label or y_col
        if not x_col or not y_col:
            raise ValueError("bubble chart requires x_column and y_column")
        sz_col = size_column or y_col
        sz_lbl = size_label or sz_col
        has_grp = bool(group_column)
        grp_lbl = group_label or group_column or ""

        bi_items = [
            _bi_item("bi_x", x_lbl, x_col, "quantitative"),
            _bi_item("bi_y", y_lbl, y_col, "quantitative"),
            _bi_item("bi_sz", sz_lbl, sz_col, "quantitative"),
        ]
        if has_grp:
            bi_items.append(_bi_item("bi_grp", grp_lbl, group_column, "categorical"))

        dd1_bis = [
            _alias("bi_x_r", "bi_x"),
            _alias("bi_y_r", "bi_y"),
            _alias("bi_sz_r", "bi_sz"),
        ]
        if has_grp:
            dd1_bis.append(_alias("bi_grp_r", "bi_grp"))

        axes = [
            _axis("column", ["bi_x_r"]),
            _axis("row", ["bi_y_r"]),
            _axis("page", ["bi_sz_r"]),
        ]
        grp_r = "bi_grp_r" if has_grp else None
        gtml = _gtml_bubble("dd_res", "bi_x_r", "bi_y_r", "bi_sz_r", grp_r)
        si = ["bi_x_r", "bi_y_r"] + (["bi_grp_r"] if has_grp else [])
        ts = _dynamic_span("bird.autotitle.template.oneOfTwo.fmt.txt",
                           "bi_y_r", "bi_x_r")

        return _assemble(report_name, cas_server, cas_library, cas_table, table_label,
                         bi_items, dd1_bis, axes, [], True,
                         "bubble", f"Bubble - {x_lbl} vs {y_lbl}", gtml, si, ts, now)

    raise ValueError(
        f"Unknown chart_type '{chart_type}'. "
        "Valid values: bar_h, bar_v, bar_stacked_v, bar_stacked_h, line, pie, donut, scatter, bubble"
    )


# ── interactive control prompts (drop-down / checkbox list / button bar) ─────
# A control is a section Prompt that filters every peer object sharing its data
# source (the report-wide implicitInteractions enable sectionPrompt brushing).
# control_type -> (promptVisual element, maxRowsLookup, label prefix,
#                  selection_disabled, needs_measure)
_CONTROL_TYPES = {
    "dropdown":      ("ComboBox",     "dropdown",  "Drop-down list", True,  False),
    "combobox":      ("ComboBox",     "dropdown",  "Drop-down list", True,  False),
    "button_bar":    ("LinkBar",      "buttonBar", "Button bar",     True,  False),
    "buttonbar":     ("LinkBar",      "buttonBar", "Button bar",     True,  False),
    "checkbox_list": ("CheckBoxList", "list",      "List",           False, True),
    "checkbox":      ("CheckBoxList", "list",      "List",           False, True),
    "list":          ("CheckBoxList", "list",      "List",           False, True),
}


def _media_scaffolding() -> dict:
    """The mediaSchemes / mediaTargets / history / sasReportState blocks shared by
    every report variant (identical to the ones _assemble emits)."""
    now = _now()
    return {
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
        "history": {
            "@element": "History",
            "editors": [{
                "@element": "Editor", "applicationName": "VA",
                "revisions": [{"@element": "Revision", "editorVersion": "2020",
                               "lastDate": now}],
            }],
        },
        "sasReportState": {
            "@element": "SASReportState", "view": {"@element": "View_State"}},
    }


def build_control_data(control_type: str, cas_server: str, cas_library: str,
                       cas_table: str, category_column: str,
                       measure_column: Optional[str] = None, table_label: Optional[str] = None,
                       suffix: str = "c", category_label: Optional[str] = None):
    """Build the data source + ParentDataDefinition + Prompt visual for one
    control, returning ``(data_source, data_definition, prompt_visual)``.

    Names are suffixed with *suffix* so multiple controls (and other objects)
    can coexist in one report without colliding. The Prompt's
    ``sourceInteractionVariableList`` carries the value variable so VA brushes
    every peer object that shares this data source when a value is selected.
    """
    ct = str(control_type).lower().strip()
    if ct not in _CONTROL_TYPES:
        raise ValueError(
            f"Unknown control_type '{control_type}'. Valid: {sorted(_CONTROL_TYPES)}")
    element, max_rows, label_prefix, selection_disabled, needs_measure = _CONTROL_TYPES[ct]
    if not category_column:
        raise ValueError(f"{control_type} control requires a category_column")

    ds = f"ds{suffix}"
    val_src, val_ali = f"bival{suffix}", f"bivalr{suffix}"
    dd1, dd2, dd_res = f"dd{suffix}", f"ddq{suffix}", f"ddres{suffix}"
    ve = f"vectl{suffix}"
    cat_lbl = category_label or category_column

    source_items = [_bi_item(val_src, cat_lbl, category_column, "categorical")]
    bi_items = [_alias(val_ali, val_src)]
    axis_items = [val_ali]
    prompt_visual = {"@element": element, "valueVariable": val_ali}

    if needs_measure and measure_column:
        meas_src, meas_ali = f"bimea{suffix}", f"bimear{suffix}"
        source_items.append(_bi_item(meas_src, measure_column, measure_column, "quantitative"))
        bi_items.insert(0, _alias(meas_ali, meas_src))
        axis_items = [meas_ali, val_ali]   # measure first, then value (HAR order)
        prompt_visual["measureVariable"] = meas_ali

    data_source = {
        "@element": "DataSource", "name": ds, "label": table_label or cas_table,
        "type": "relational",
        "casResource": {"@element": "CasResource", "server": cas_server,
                        "library": cas_library, "table": cas_table, "locale": "en_US"},
        "businessItemFolder": {"@element": "BusinessItemFolder", "items": source_items},
    }
    data_definition = {
        "@element": "ParentDataDefinition", "name": dd1, "businessItems": bi_items,
        "source": ds, "childQueryRelationshipType": "independent",
        "dataDefinitionList": [{
            "@element": "DataDefinition", "name": dd2, "type": "relational",
            "relationalQueryList": [{
                "@element": "RelationalQuery",
                "sortItems": [{"@element": "SortItem", "sortDirection": "ascending",
                               "reference": val_ali}],
                "axes": [{"@element": "Query_Axis", "type": "column", "itemList": axis_items}],
            }],
            "source": ds,
            "resultDefinitions": [{
                "@element": "ResultDefinition", "name": dd_res, "purpose": "primary",
                "maxRowsBehavior": "truncate", "maxRowsLookup": max_rows}],
        }],
        "status": "executable",
    }
    editor_props = [
        {"@element": "Editor_Property", "key": "autoChartCategory", "value": "CONTROL"},
        {"@element": "Editor_Property", "key": "isAutoLabel", "value": "true"},
    ]
    prompt = {
        "@element": "Prompt", "name": ve, "data": dd1,
        "applyDynamicBrushes": "promptsOnly",
        "labelAttribute": f"{label_prefix} - {cat_lbl} 1",
        **({"selectionDisabled": "true"} if selection_disabled else {}),
        "sourceInteractionVariableList": [val_ali],
        "editorProperties": editor_props,
        "resultDefinitionList": [dd_res],
        "promptVisual": prompt_visual,
    }
    return data_source, data_definition, prompt


def build_control_content(control_type: str, report_name: str, cas_server: str,
                          cas_library: str, cas_table: str, category_column: str,
                          measure_column: Optional[str] = None,
                          category_label: Optional[str] = None,
                          table_label: Optional[str] = None) -> dict:
    """Build a one-control SAS VA report (a drop-down / checkbox list / button
    bar prompt over *category_column*). On its own it filters nothing; placed in
    a dashboard alongside charts on the same table it becomes a page filter."""
    table_label = table_label or cas_table
    ds, dd, prompt = build_control_data(
        control_type, cas_server, cas_library, cas_table, category_column,
        measure_column, table_label, suffix="1", category_label=category_label)
    report = {
        "@element": "SASReport",
        "xmlns": "http://www.sas.com/sasreportmodel/bird-4.64.0",
        "label": report_name,
        "createdApplicationName": "SAS Visual Analytics",
        "dateModified": _now(),
        "lastModifiedApplicationName": "SAS Visual Analytics",
        "createdLocale": "en_US",
        "features": ["promptModelV2"],
        "implicitInteractions": ["reportPrompt", "sectionPrompt", "sectionLink"],
        "nextUniqueNameIndex": 20,
        "dataDefinitions": [dd],
        "dataSources": [ds],
        "visualElements": [prompt],
        "view": {"@element": "View", "sections": [{
            "@element": "Section", "name": "vi1", "label": "Page 1",
            "body": {"@element": "Body", "mediaContainerList": [{
                "@element": "MediaContainer", "target": "mt_default",
                "layout": {"@element": "ResponsiveLayout", "orientation": "vertical",
                           "overflow": "fit", "weights": _weights([100])},
                "containedElementList": [_dash_visual("vi2", prompt["name"])],
            }]},
        }]},
        "properties": [{"@element": "Property", "key": "displayDataSource",
                        "value": ds["name"]}],
        **_media_scaffolding(),
    }
    return report


# ── crosstab with row/column hierarchies ─────────────────────────────────────

def build_crosstab_data(cas_server: str, cas_library: str, cas_table: str,
                        row_categories: list, column_categories: Optional[list] = None,
                        measures: Optional[list] = None, table_label: Optional[str] = None,
                        measure_format: Optional[str] = None, suffix: str = "x"):
    """Build the data source + ParentDataDefinition + Crosstab visual for a
    crosstab whose row and/or column axis may stack SEVERAL category columns
    (a drill hierarchy), with one or more measures. Returns
    ``(data_source, data_definition, crosstab_visual)`` with *suffix*-tagged names."""
    row_categories = [c for c in (row_categories or []) if c]
    column_categories = [c for c in (column_categories or []) if c]
    measures = [m for m in (measures or []) if m]
    if not (row_categories or column_categories):
        raise ValueError("crosstab needs at least one row or column category")
    if not measures:
        raise ValueError("crosstab needs at least one measure_column")

    ds = f"ds{suffix}"
    dd1, dd2, dd_res = f"dd{suffix}", f"ddq{suffix}", f"ddres{suffix}"
    source_items: list = []
    bi_items: list = []
    n = [0]

    def make(col: str, usage: str) -> str:
        n[0] += 1
        src, ali = f"bisrc{suffix}_{n[0]}", f"bi{suffix}{n[0]}"
        fmt = measure_format if usage == "quantitative" else None
        source_items.append(_bi_item(src, col, col, usage, fmt=fmt))
        bi_items.append(_alias(ali, src))
        return ali

    col_cat_aliases = [make(c, "categorical") for c in column_categories]
    row_cat_aliases = [make(c, "categorical") for c in row_categories]
    meas_aliases = [make(m, "quantitative") for m in measures]

    # Crosstab visual axes: each category becomes a Crosstab_Hierarchy level; the
    # measures sit in a single Measures block on the column axis.
    v = [0]

    def vename() -> str:
        v[0] += 1
        return f"ve{suffix}{v[0]}"

    col_dims = [{"@element": "Crosstab_Hierarchy", "name": vename(), "variable": a}
                for a in col_cat_aliases]
    col_dims.append({"@element": "Measures", "measureList": [
        {"@element": "Crosstab_Measure", "name": vename(), "variable": a,
         "compactFormat": False} for a in meas_aliases]})
    row_dims = [{"@element": "Crosstab_Hierarchy", "name": vename(), "variable": a}
                for a in row_cat_aliases]

    col_items = col_cat_aliases + meas_aliases
    query_axes = [_axis("column", col_items)]
    if row_cat_aliases:
        query_axes.append(_axis("row", row_cat_aliases))

    mdq = {"@element": "MultidimensionalQuery", "axes": query_axes,
           "rowSubtotals": False, "columnSubtotals": False}
    if row_cat_aliases:
        mdq["rowSortItems"] = [{"@element": "SortItem", "sortDirection": "descending",
                                "reference": row_cat_aliases[0]}]
    if col_cat_aliases:
        mdq["columnSortItems"] = [_sort_asc(col_cat_aliases[0])]

    data_source = {
        "@element": "DataSource", "name": ds, "label": table_label or cas_table,
        "type": "relational",
        "casResource": {"@element": "CasResource", "server": cas_server,
                        "library": cas_library, "table": cas_table, "locale": "en_US"},
        "businessItemFolder": {"@element": "BusinessItemFolder", "items": source_items},
    }
    data_definition = {
        "@element": "ParentDataDefinition", "name": dd1, "businessItems": bi_items,
        "source": ds, "childQueryRelationshipType": "independent",
        "dataDefinitionList": [{
            "@element": "DataDefinition", "name": dd2, "type": "multidimensional",
            "multidimensionalQueryList": [mdq], "source": ds,
            "resultDefinitions": [{
                "@element": "ResultDefinition", "name": dd_res, "purpose": "primary",
                "maxRowsBehavior": "noData", "maxRowsLookup": "crosstab"}],
        }],
        "status": "executable",
    }
    crosstab = {
        "@element": "Crosstab", "name": f"vect{suffix}", "data": dd1,
        "applyDynamicBrushes": "yes", "labelAttribute": "Crosstab 1",
        "sourceInteractionVariableList": col_cat_aliases + row_cat_aliases,
        "editorProperties": [{"@element": "Editor_Property", "key": "isAutoLabel",
                              "value": "true"}],
        "resultDefinitionList": [dd_res], "measureSizing": "autoFill",
        "axes": [
            {"@element": "Crosstab_Axis", "type": "column", "dimensionList": col_dims},
            {"@element": "Crosstab_Axis", "type": "row", "dimensionList": row_dims},
        ],
    }
    return data_source, data_definition, crosstab


def build_crosstab_content(report_name: str, cas_server: str, cas_library: str,
                           cas_table: str, row_categories: list,
                           column_categories: Optional[list] = None,
                           measures: Optional[list] = None,
                           table_label: Optional[str] = None,
                           measure_format: Optional[str] = None) -> dict:
    """Build a one-crosstab SAS VA report. *row_categories* / *column_categories*
    are ordered lists — pass more than one to stack drill levels (a hierarchy) on
    that axis; *measures* is one or more measure columns shown in the cells."""
    table_label = table_label or cas_table
    ds, dd, crosstab = build_crosstab_data(
        cas_server, cas_library, cas_table, row_categories, column_categories,
        measures, table_label, measure_format, suffix="1")
    return {
        "@element": "SASReport",
        "xmlns": "http://www.sas.com/sasreportmodel/bird-4.64.0",
        "label": report_name,
        "createdApplicationName": "SAS Visual Analytics",
        "dateModified": _now(),
        "lastModifiedApplicationName": "SAS Visual Analytics",
        "createdLocale": "en_US",
        "features": ["promptModelV2"],
        "implicitInteractions": ["reportPrompt", "sectionPrompt", "sectionLink"],
        "nextUniqueNameIndex": 20,
        "dataDefinitions": [dd],
        "dataSources": [ds],
        "visualElements": [crosstab],
        "view": {"@element": "View", "sections": [{
            "@element": "Section", "name": "vi1", "label": "Page 1",
            "body": {"@element": "Body", "mediaContainerList": [{
                "@element": "MediaContainer", "target": "mt_default",
                "layout": {"@element": "ResponsiveLayout", "orientation": "vertical",
                           "overflow": "fit", "weights": _weights([100])},
                "containedElementList": [_dash_visual("vi2", crosstab["name"])],
            }]},
        }]},
        "properties": [{"@element": "Property", "key": "displayDataSource",
                        "value": ds["name"]}],
        **_media_scaffolding(),
    }


# ── dashboard (multiple KPIs + charts in one report) ─────────────────────────

# Chart object types handled by build_chart_content; anything else is treated as
# a VA catalog object (e.g. "kpi"/"key_value") built via va_objects.
_CHART_TYPES = {"bar_h", "bar_v", "bar", "bar_stacked_v", "bar_stacked_h",
                "stacked_bar_v", "stacked_bar_h", "line", "pie", "donut",
                "scatter", "bubble"}


def _catalog_types() -> set:
    """VA catalog object types usable as dashboard tiles — every va_objects role
    object except key_value (handled as the KPI strip) and the data-less layout
    containers. Lazy import to avoid a module-load cycle."""
    from .va_objects import _ROLE_PARAMS, _CONTAINER_TYPES
    return (set(_ROLE_PARAMS) - {"key_value"}) - set(_CONTAINER_TYPES)


def _merge_data_sources(data_sources: list, data_definitions: list):
    """Collapse DataSources that point at the SAME CAS table (server/library/table)
    into one, concatenating their business items and rewriting every ``source``
    reference in *data_definitions* to the surviving name.

    A shared data source is what lets a section prompt (control) brush its peer
    objects and lets charts cross-filter each other (linked selection). Returns
    ``(merged_sources, rename_map)``; *data_definitions* is mutated in place."""
    by_key: dict = {}
    rename: dict = {}
    merged: list = []
    for ds in data_sources:
        cr = ds.get("casResource", {})
        key = (cr.get("server"), cr.get("library"), cr.get("table"))
        if key in by_key:
            canon = by_key[key]
            rename[ds["name"]] = canon["name"]
            canon["businessItemFolder"]["items"].extend(
                ds.get("businessItemFolder", {}).get("items", []))
        else:
            by_key[key] = ds
            merged.append(ds)

    if rename:
        def rewrite(node):
            if isinstance(node, dict):
                src = node.get("source")
                if isinstance(src, str) and src in rename:
                    node["source"] = rename[src]
                for v in node.values():
                    rewrite(v)
            elif isinstance(node, list):
                for v in node:
                    rewrite(v)
        rewrite(data_definitions)
    return merged, rename


def _collect_names(node, acc: set) -> None:
    """Collect every declared identifier (the value of any "name" key)."""
    if isinstance(node, dict):
        n = node.get("name")
        if isinstance(n, str) and n:
            acc.add(n)
        for v in node.values():
            _collect_names(v, acc)
    elif isinstance(node, list):
        for v in node:
            _collect_names(v, acc)


def _namespace(sections: dict, suffix: str) -> dict:
    """Append ``suffix`` (digits only) to every internal identifier in
    ``sections`` so multiple objects can coexist in one report without name
    collisions.

    The single-object builders reuse fixed symbolic names (ds1, dd1, dd2,
    dd_res, ve1, ve2, bi_cat, …). Composing N objects would collide, so we
    rename every declared ``name`` — and every reference to it, including inside
    the gtml StatGraph strings.

    The rename MUST be a trailing DIGIT run, never a prefix. SAS VA's reportData
    validator parses each BIRD element name as ``<typecode>(<digits> | _<word>…)``
    and rejects names that don't START with their type code. So a prefix like
    ``o0_dd1`` makes every tile's data query fail with HTTP 400 "dataDefinitions
    is not valid" (each dashboard tile then renders an Error in the VA UI), while
    a trailing digit keeps the type code first and the name valid:
    ``dd1`` → ``dd10``, ``dd_res`` → ``dd_res0``, ``bi_cat_r`` → ``bi_cat_r0``.
    Whole-token (``\\b…\\b``) matching + longest-first keeps e.g. ``bi_freq``
    from corrupting ``bi_freq_r``; a single pass avoids re-scanning replacements.
    """
    names: set = set()
    _collect_names(sections, names)
    if not names:
        return sections
    # Longest first is belt-and-suspenders on top of the word boundaries.
    ordered = sorted(names, key=len, reverse=True)
    pattern = re.compile(r"\b(" + "|".join(re.escape(n) for n in ordered) + r")\b")
    serialized = json.dumps(sections)
    serialized = pattern.sub(lambda m: m.group(1) + suffix, serialized)
    return json.loads(serialized)


def _dash_visual(name: str, ref: str) -> dict:
    """A layout Visual entry referencing one object's primary visual element."""
    return {
        "@element": "Visual",
        "name": name,
        "ref": ref,
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


def _weights(pcts: list) -> list:
    """One Weights block per media target, each carrying N per-child weights."""
    def block(target: str) -> dict:
        return {
            "@element": "Weights", "mediaTarget": target, "unit": "percent",
            "values": [{"@element": "Weight", "value": f"{p}%"} for p in pcts],
        }
    return [block("mt_large"), block("mt_medium"), block("mt_small")]


def _distribute(units: list) -> list:
    """Turn per-child relative units into integer percentages summing to 100."""
    total = float(sum(units)) or 1.0
    pcts = [int(round(100 * u / total)) for u in units]
    # Fix rounding drift so the weights sum to exactly 100.
    drift = 100 - sum(pcts)
    if pcts:
        pcts[-1] += drift
    return pcts


def _visual_container_element(name: str) -> dict:
    """A layout VisualContainer element that a grid row references. A row
    Container in the view MUST `ref` a real VisualContainer element — an inline
    layout-only Container crashes the VA UI ("Cannot read properties of null")."""
    return {
        "@element": "VisualContainer", "name": name, "containerType": "layout",
        "selectionDisabled": "true", "labelAttribute": name,
        "editorProperties": [
            {"@element": "Editor_Property", "key": "visualType", "value": "LAYOUT_CONTAINER"},
            {"@element": "Editor_Property", "key": "isAutoLabel", "value": "true"},
        ],
    }


def _grid_row(name: str, ref: str, child_visuals: list) -> dict:
    """One grid row: a Container (referencing a VisualContainer element) that lays
    its tiles out side-by-side (orientation 'vertical' = side-by-side splits)."""
    pcts = _distribute([1] * len(child_visuals))
    return {
        "@element": "Container", "name": name, "ref": ref,
        "layout": {"@element": "ResponsiveLayout", "orientation": "vertical",
                   "overflow": "fit", "weights": _weights(pcts)},
        "containedElementList": child_visuals,
        "responsiveConstraint": _dash_visual("_", "_")["responsiveConstraint"],
    }


def build_dashboard_content(
    report_name: str,
    cas_server: str,
    cas_library: str,
    cas_table: str,
    objects: list = None,
    table_label: Optional[str] = None,
    pages: Optional[list] = None,
    link_interactions: Optional[bool] = None,
) -> dict:
    """Build ONE SAS VA report containing multiple objects (KPIs + charts).

    ``objects`` is an ordered list of dicts, each describing one tile:

      KPI   – {"type": "kpi", "measure_column": "Profit", "label": "Total Profit"}
      chart – {"type": "bar_h"|"bar_v"|"line"|"pie"|"scatter"|"bubble",
               + the same column roles build_chart_content accepts
               (category_column / measure_column / x_column / y_column /
                size_column / group_column and their *_label variants)}

    All objects share the dashboard's single CAS table. Tiles are laid out top
    to bottom in a vertical responsive layout; KPIs get a smaller share of the
    height than charts so they read as compact tiles.
    """
    # Multi-page: ``pages`` is a list of object-lists (one per page/tab). Single
    # page = the flat ``objects`` list. Flatten with a parallel page index so every
    # tile still gets a unique global digit suffix.
    if pages is None:
        pages = [objects or []]
    flat = [(pi, obj) for pi, page_objs in enumerate(pages) for obj in (page_objs or [])]
    if not flat:
        raise ValueError("A dashboard needs at least one object.")
    now = _now()
    table_label = table_label or cas_table

    all_data_sources: list = []
    all_data_definitions: list = []
    all_visual_elements: list = []
    visuals: list = []
    units: list = []
    tile_pages: list = []
    has_control = False

    for i, (page_idx, spec) in enumerate(flat):
        tile_pages.append(page_idx)
        spec = dict(spec or {})
        otype = str(spec.get("type", "")).lower().strip()
        # Per-object DIGIT suffix (not a prefix) — see _namespace: BIRD element
        # names must start with their type code or the reportData query 400s.
        suffix = str(i)

        # ── interactive control prompt (filters the page) ────────────────────
        # type:'control' (with control_type) or directly 'dropdown' / 'button_bar'
        # / 'checkbox_list'. build_control_data already emits suffixed names, so a
        # control bypasses the _namespace step the data objects need.
        ctl_kind = spec.get("control_type") if otype == "control" else otype
        if ctl_kind and str(ctl_kind).lower().strip() in _CONTROL_TYPES:
            ds_c, dd_c, prompt_c = build_control_data(
                str(ctl_kind), cas_server, cas_library, cas_table,
                category_column=spec.get("category_column"),
                measure_column=spec.get("measure_column"),
                category_label=spec.get("category_label"),
                table_label=table_label, suffix=suffix)
            all_data_sources.append(ds_c)
            all_data_definitions.append(dd_c)
            all_visual_elements.append(prompt_c)
            visuals.append(_dash_visual(f"vi_obj{i}", prompt_c["name"]))
            units.append(1)
            has_control = True
            continue

        # ── crosstab with a row/column hierarchy (list-form roles) ───────────
        # A crosstab tile that supplies row_categories / column_categories /
        # measures (lists) drills via build_crosstab_data; the single-level
        # crosstab still flows through the catalog branch below.
        if otype == "crosstab" and (spec.get("row_categories")
                                    or spec.get("column_categories")
                                    or spec.get("measures")):
            ds_x, dd_x, xt = build_crosstab_data(
                cas_server, cas_library, cas_table,
                row_categories=spec.get("row_categories"),
                column_categories=spec.get("column_categories"),
                measures=spec.get("measures"),
                table_label=table_label, measure_format=spec.get("measure_format"),
                suffix=suffix)
            all_data_sources.append(ds_x)
            all_data_definitions.append(dd_x)
            all_visual_elements.append(xt)
            visuals.append(_dash_visual(f"vi_obj{i}", xt["name"]))
            units.append(3)
            continue

        if otype in _CHART_TYPES:
            single = build_chart_content(
                chart_type=otype,
                report_name=report_name,
                cas_server=cas_server,
                cas_library=cas_library,
                cas_table=cas_table,
                category_column=spec.get("category_column"),
                category_label=spec.get("category_label"),
                measure_column=spec.get("measure_column"),
                measure_label=spec.get("measure_label"),
                x_column=spec.get("x_column"),
                x_label=spec.get("x_label"),
                y_column=spec.get("y_column"),
                y_label=spec.get("y_label"),
                size_column=spec.get("size_column"),
                size_label=spec.get("size_label"),
                group_column=spec.get("group_column"),
                group_label=spec.get("group_label"),
                table_label=table_label,
                aggregation=spec.get("aggregation"),
                measure_format=spec.get("measure_format"),
                calc_numerator=spec.get("calc_numerator"),
                calc_denominator=spec.get("calc_denominator"),
                calc_op=spec.get("calc_op", "div"),
                calc_expression=spec.get("calc_expression"),
                calc_label=spec.get("calc_label"),
                calc_format=spec.get("calc_format"),
                calc_user_repr=spec.get("calc_user_repr"),
            )
            units.append(3)
        elif otype in ("kpi", "key_value", "keyvalue"):
            # KPIs are the VA "key_value" catalog object built by va_objects.
            from .va_objects import build_object_content
            measure = spec.get("measure_column") or spec.get("measure")
            if not measure:
                raise ValueError(f"KPI object #{i + 1} needs a 'measure_column'.")
            single = build_object_content(
                object_type="key_value",
                report_name=report_name,
                cas_server=cas_server,
                cas_library=cas_library,
                cas_table=cas_table,
                column_overrides={"measure_column": measure},
                table_label=table_label,
                measure_aggregation=spec.get("aggregation"),
                measure_format=spec.get("measure_format"),
            )
            units.append(1)
        elif otype in _catalog_types():
            # Other VA catalog objects: treemap, heat_map, targeted_bar, waterfall,
            # histogram, box_plot, word_cloud, crosstab, button_bar, list_control.
            # Their role keys come straight from va_objects._ROLE_PARAMS, so the
            # spec just supplies matching column fields (category_column,
            # measure_column, x_column, y_column, size_column, color_column,
            # target_column, measure2_column, column_category, row_category).
            from .va_objects import build_object_content, _ROLE_PARAMS
            overrides = {role: spec.get(role)
                         for role in _ROLE_PARAMS.get(otype, {}) if spec.get(role)}
            single = build_object_content(
                object_type=otype,
                report_name=report_name,
                cas_server=cas_server,
                cas_library=cas_library,
                cas_table=cas_table,
                column_overrides=overrides,
                table_label=table_label,
            )
            units.append(3)
        else:
            raise ValueError(
                f"Object #{i + 1} has unknown type '{otype}'. Valid: "
                "kpi, bar_h, bar_v, bar_stacked_v, bar_stacked_h, line, pie, donut, "
                "scatter, bubble, treemap, heat_map, targeted_bar, waterfall, "
                "histogram, box_plot, word_cloud, crosstab, button_bar, list_control."
            )

        # Optional per-tile data filters (apply before namespacing so the filter
        # business items get suffixed along with the rest of the object).
        apply_filters(single, spec.get("filters"))

        # The primary visual element is the first one the builder emitted (the
        # Graph for charts, the object for catalog objects). Record it BEFORE
        # namespacing, then reference its suffixed name in the layout.
        primary = single["visualElements"][0]["name"]
        sections = {
            "dataSources": single["dataSources"],
            "dataDefinitions": single["dataDefinitions"],
            "visualElements": single["visualElements"],
        }
        ns = _namespace(copy.deepcopy(sections), suffix)
        all_data_sources.extend(ns["dataSources"])
        all_data_definitions.extend(ns["dataDefinitions"])
        all_visual_elements.extend(ns["visualElements"])
        visuals.append(_dash_visual(f"vi_obj{i}", primary + suffix))

    # Share one data source across same-table tiles so a control prompt filters
    # its peers and (when requested) charts cross-filter each other. Controls are
    # useless without it, so it is forced on whenever a control is present.
    if link_interactions is None:
        link_interactions = has_control
    if link_interactions:
        all_data_sources, _rename = _merge_data_sources(
            all_data_sources, all_data_definitions)

    display_ds = all_data_sources[0]["name"] if all_data_sources else "ds1"

    # ── Grid layout per page: KPI strip(s) on top, charts 2-up below ──────────
    # `units` carries 1 per KPI tile and 3 per chart tile (set in the loop), so
    # it doubles as the KPI/chart classifier. Each row is a Container that refs a
    # VisualContainer element; row-container names use a global counter so they
    # stay unique across pages.
    def _chunk(lst, n):
        return [lst[i:i + n] for i in range(0, len(lst), n)]

    def _page_section(secname, label, page_vu, row_offset):
        kpi_vis = [v for v, u in page_vu if u == 1]
        chart_vis = [v for v, u in page_vu if u != 1]
        rows_spec = []
        for grp in _chunk(kpi_vis, 4):
            rows_spec.append((grp, 2))
        for grp in _chunk(chart_vis, 2):
            rows_spec.append((grp, 3))
        if not rows_spec:
            rows_spec = [([v for v, _u in page_vu], 3)]
        row_containers = []
        for ridx, (child_vis, _h) in enumerate(rows_spec):
            elname = f"ve_row{row_offset + ridx}"
            all_visual_elements.append(_visual_container_element(elname))
            row_containers.append(_grid_row(f"vi_row{row_offset + ridx}", elname, child_vis))
        heights = _distribute([h for _v, h in rows_spec])
        section = {
            "@element": "Section", "name": secname, "label": label,
            "body": {"@element": "Body", "mediaContainerList": [{
                "@element": "MediaContainer", "target": "mt_default",
                "layout": {"@element": "ResponsiveLayout", "orientation": "horizontal",
                           "overflow": "scroll", "weights": _weights(heights)},
                "containedElementList": row_containers,
            }]},
        }
        return section, row_offset + len(rows_spec)

    view_sections = []
    row_off = 0
    for pi in range(len(pages)):
        page_vu = [(v, u) for v, u, p in zip(visuals, units, tile_pages) if p == pi]
        if not page_vu:
            continue
        sec, row_off = _page_section(f"vi{pi + 1}", f"Page {pi + 1}", page_vu, row_off)
        view_sections.append(sec)

    return {
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
        "dataDefinitions": all_data_definitions,
        "dataSources": all_data_sources,
        "visualElements": all_visual_elements,
        "view": {"@element": "View", "sections": view_sections},
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
            {"@element": "Property", "key": "displayDataSource", "value": display_ds}],
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


# ── backward-compat wrapper ───────────────────────────────────────────────────

def build_bar_chart_content(
    report_name: str,
    cas_server: str,
    cas_library: str,
    cas_table: str,
    category_column: str,
    category_label: Optional[str] = None,
    group_column: Optional[str] = None,
    group_label: Optional[str] = None,
    table_label: Optional[str] = None,
) -> dict:
    return build_chart_content(
        chart_type="bar_h",
        report_name=report_name,
        cas_server=cas_server,
        cas_library=cas_library,
        cas_table=cas_table,
        category_column=category_column,
        category_label=category_label,
        group_column=group_column,
        group_label=group_label,
        table_label=table_label,
    )
