from __future__ import annotations
from typing import List
import re

from .parse_cv import CVModel
from .parse_sql_view import SQLViewModel
from .parse_procedure import ProcedureModel


def _compact_list(items: List[str], max_items: int = 6) -> str:
    if not items:
        return ""
    if len(items) <= max_items:
        return ", ".join(items)
    return ", ".join(items[:max_items]) + f" … (+{len(items) - max_items} more)"


# -------------------------------
# Stored Procedure summarization
# -------------------------------
def summarize_procedure(p: ProcedureModel) -> List[str]:
    bullets: List[str] = []
    sql = p.sql.upper()

    # High-level purpose guess
    if any(x in sql for x in ["DATEDIFF(", "@DAYS1", "@DAYS2", "@DAYS10"]):
        bullets.append("Computes time-based **aging buckets** using date differences and threshold parameters (e.g., @Days1..@Days10).")
    if "OVER(" in sql:
        bullets.append("Uses **window functions** (OVER) to compute running/cumulative values (e.g., cumulative stock).")
    if "SELECT" in sql and ("INTO #" in sql or "CREATE TABLE #".upper() in sql):
        bullets.append("Stages intermediate results in **temporary tables** (#…) using SELECT INTO / CTAS.")

    # Data sources
    real_reads = [r for r in (p.reads_from or []) if not r.startswith("#") and r.upper() != "STRING_SPLIT"]
    if real_reads:
        bullets.append(f"Reads primary data from: {_compact_list(real_reads)}.")

    # Temp tables
    if hasattr(p, "temp_tables") and p.temp_tables:
        bullets.append(f"Builds temp staging tables: {_compact_list(p.temp_tables)}.")

    # Joins / filters hints
    join_count = len(re.findall(r'\bJOIN\b', sql, re.IGNORECASE))
    if join_count:
        bullets.append(f"Contains ~{join_count} JOINs across fact/dimension tables.")

    if "DEBITCREDIT" in sql:
        # simple directional hint
        if re.search(r"DEBITCREDIT\s*=\s*'H'", sql, re.IGNORECASE):
            bullets.append("Applies filter **DebitCredit='H'** (outbound/credit movements) in parts of the logic.")
        if re.search(r"DEBITCREDIT\s*=\s*'S'", sql, re.IGNORECASE):
            bullets.append("Applies filter **DebitCredit='S'** (inbound/debit movements) in parts of the logic.")

    # Final selection / bucketing presence
    if "CASE" in sql:
        bullets.append("Derives **bucket measures** via CASE/filtered SUM expressions.")

    # Special hints for common names
    tls = [t.lower() for t in getattr(p, "temp_tables", [])]
    if any("outboundquantity" in t for t in tls):
        bullets.append("**#OutboundQuantity** aggregates outbound quantities by Company/Plant/Product/SpecialStock up to the report date.")
    if any("inventoryaging" in t for t in tls):
        bullets.append("**#InventoryAging** computes on-hand per posting date (cumulative arrivals minus outbound) and enriches with valuation/GL info.")

    return bullets


# -------------------------------
# SQL View summarization
# -------------------------------
def summarize_sql_view(v: SQLViewModel) -> List[str]:
    bullets: List[str] = []
    sql = v.sql.upper()

    # SELECT list / columns
    if v.columns:
        bullets.append(f"Outputs **{len(v.columns)}** columns (preview: {_compact_list(v.columns)}).")

    # Inputs / joins
    if v.inputs:
        bullets.append(f"Sources from: {_compact_list(v.inputs)}.")
    join_count = len(re.findall(r'\bJOIN\b', sql, re.IGNORECASE))
    if join_count:
        bullets.append(f"Contains ~{join_count} JOINs.")

    # DISTINCT / GROUP BY / aggregation cues
    if "DISTINCT" in sql:
        bullets.append("Uses **SELECT DISTINCT** to remove duplicates.")
    if re.search(r'\bGROUP\s+BY\b', sql, re.IGNORECASE):
        agg_funcs = re.findall(r'\b(SUM|COUNT|AVG|MIN|MAX)\s*\(', sql, re.IGNORECASE)
        if agg_funcs:
            bullets.append(f"Aggregates data (**{', '.join(sorted(set(a.upper() for a in agg_funcs)))}**).")

    # WHERE clause (first occurrence) preview
    where_m = re.search(r'\bWHERE\b(.*?)(\bGROUP\b|\bORDER\b|;|$)', v.sql, re.IGNORECASE | re.DOTALL)
    if where_m:
        where_txt = " ".join(where_m.group(1).split())
        preview = where_txt[:180] + ("…" if len(where_txt) > 180 else "")
        bullets.append(f"Filters rows in WHERE clause (preview): {preview}")

    return bullets


# -------------------------------
# HANA Calculation View summarization
# -------------------------------
def summarize_cv(model: CVModel) -> List[str]:
    bullets: List[str] = []
    node_types = [n.node_type for n in model.nodes.values()]
    if node_types:
        pv = node_types.count("ProjectionView")
        jv = node_types.count("JoinView")
        av = node_types.count("AggregationView")
        uv = node_types.count("UnionView")
        bullets.append(f"Graph contains **{len(node_types)}** nodes "
                       f"(Projection: {pv}, Join: {jv}, Aggregation: {av}, Union: {uv}).")

    # Measures, attributes, calculated items
    total_attrs = sum(len(n.attributes) for n in model.nodes.values())
    total_meas = sum(len(n.measures) for n in model.nodes.values())
    total_calc_meas = sum(len(n.calculated_measures) for n in model.nodes.values())
    if total_attrs or total_meas:
        bullets.append(f"Defines ~{total_attrs} attributes and ~{total_meas} measures across nodes.")
    if total_calc_meas:
        bullets.append(f"Includes **{total_calc_meas}** calculated measures.")

    # Filters / joins summary
    total_filters = sum(len(n.filters) for n in model.nodes.values())
    if total_filters:
        bullets.append(f"Applies ~{total_filters} node-level filters.")
    join_types = sorted({n.join_type for n in model.nodes.values() if n.join_type})
    if join_types:
        bullets.append(f"Join types used: {', '.join(join_types)}.")

    # Topological order preview
    if model.nodes:
        ordered = list(model.nodes.keys())
        bullets.append(f"Build order preview: {_compact_list(ordered)}.")

    return bullets