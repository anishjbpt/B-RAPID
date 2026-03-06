from __future__ import annotations
from typing import List
import re
from .parse_cv import CVModel
from .parse_sql_view import SQLViewModel
from .parse_procedure import ProcedureModel
from .parse_abap_cds import ABAPCDSModel  # NEW

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
    """
    Produce plain-English bullets for a SQL View:
      - outputs/inputs/join count
      - DISTINCT / aggregate functions
      - WHERE (preview)
      - GROUP BY (column preview)
      - HAVING (preview)
      - ORDER BY (column + ASC/DESC preview)
      - LIMIT / TOP (row limit)
    """
    bullets: List[str] = []
    # Keep both cases to parse; use original for substring captures, uppercase for quick checks
    sql_raw = v.sql or ""
    sql_up = sql_raw.upper()

    # 1) Columns / outputs
    if getattr(v, "columns", None):
        bullets.append(f"Outputs **{len(v.columns)}** columns (preview: {_compact_list(v.columns)}).")

    # 2) Inputs / joins
    if getattr(v, "inputs", None):
        bullets.append(f"Sources from: {_compact_list(v.inputs)}.")
    join_count = len(re.findall(r'\bJOIN\b', sql_raw, re.IGNORECASE))
    if join_count:
        bullets.append(f"Contains ~{join_count} JOINs.")

    # 3) DISTINCT / aggregation cues
    if "DISTINCT" in sql_up:
        bullets.append("Uses **SELECT DISTINCT** to remove duplicates.")
    agg_funcs = re.findall(r'\b(SUM|COUNT|AVG|MIN|MAX)\s*\(', sql_raw, re.IGNORECASE)
    if agg_funcs:
        bullets.append(f"Aggregates data (**{', '.join(sorted(set(a.upper() for a in agg_funcs)))}**).")

    # Helper: compact preview of a captured clause
    def _preview(text: str, n: int = 180) -> str:
        t = " ".join((text or "").split())
        return t[:n] + ("…" if len(t) > n else "")

    # 4) WHERE preview  (stop at GROUP/HAVING/ORDER/;/$)
    where_m = re.search(r'\bWHERE\b(.*?)(\bGROUP\b|\bHAVING\b|\bORDER\b|;|$)', sql_raw, re.IGNORECASE | re.DOTALL)
    if where_m:
        bullets.append(f"Filters rows in WHERE clause (preview): {_preview(where_m.group(1))}")

    # 5) GROUP BY — capture list between GROUP BY and (HAVING|ORDER|;|$)
    grp_m = re.search(r'\bGROUP\s+BY\b(.*?)(\bHAVING\b|\bORDER\b|;|$)', sql_raw, re.IGNORECASE | re.DOTALL)
    if grp_m:
        grp_txt = " ".join(grp_m.group(1).split())
        # split on commas at top level to preview grouping columns
        cols = []
        depth = 0; buf = []
        for ch in grp_txt:
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth = max(0, depth - 1)
            if ch == ',' and depth == 0:
                cols.append("".join(buf).strip()); buf = []
            else:
                buf.append(ch)
        if buf:
            cols.append("".join(buf).strip())
        cols_preview = _compact_list([c for c in cols if c], max_items=6) if cols else _preview(grp_txt, 120)
        bullets.append(f"Groups results by: {cols_preview}")

    # 6) HAVING — capture text until ORDER/;/$
    having_m = re.search(r'\bHAVING\b(.*?)(\bORDER\b|;|$)', sql_raw, re.IGNORECASE | re.DOTALL)
    if having_m:
        bullets.append(f"Filters groups in HAVING clause (preview): {_preview(having_m.group(1))}")

    # 7) ORDER BY — capture items until ; or end
    #    We also extract ASC/DESC per item when present.
    ord_m = re.search(r'\bORDER\s+BY\b(.*?)(;|$)', sql_raw, re.IGNORECASE | re.DOTALL)
    if ord_m:
        ord_txt = " ".join(ord_m.group(1).split())
        # split respecting parentheses
        items, depth, buf = [], 0, []
        for ch in ord_txt:
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth = max(0, depth - 1)
            if ch == ',' and depth == 0:
                items.append("".join(buf).strip()); buf = []
            else:
                buf.append(ch)
        if buf:
            items.append("".join(buf).strip())
        # Extract direction hints
        ord_preview = []
        for it in items[:6]:
            m_dir = re.search(r'\b(ASC|DESC)\b', it, re.IGNORECASE)
            dir_txt = f" {m_dir.group(1).upper()}" if m_dir else ""
            # Try to pull the leading expression/column name
            # Strip trailing ASC/DESC and NULLS clauses for display
            clean = re.sub(r'\b(ASC|DESC)\b.*$', '', it, flags=re.IGNORECASE).strip()
            ord_preview.append(f"{clean}{dir_txt}")
        bullets.append("Orders results by: " + ", ".join(ord_preview) + (" …" if len(items) > 6 else ""))

    # 8) LIMIT / TOP — support LIMIT n, FETCH FIRST n ROWS ONLY, and SELECT TOP n
    # LIMIT n
    lim_m = re.search(r'\bLIMIT\s+(\d+)\b', sql_raw, re.IGNORECASE)
    # FETCH FIRST n ROWS ONLY
    fetch_m = re.search(r'\bFETCH\s+FIRST\s+(\d+)\s+ROWS?\s+ONLY\b', sql_raw, re.IGNORECASE)
    # TOP n (at start of SELECT list)
    top_m = re.search(r'\bSELECT\b\s+TOP\s+(\d+)\b', sql_raw, re.IGNORECASE)

    lim_val = None
    if lim_m:
        lim_val = lim_m.group(1)
    elif fetch_m:
        lim_val = fetch_m.group(1)
    elif top_m:
        lim_val = top_m.group(1)

    if lim_val:
        bullets.append(f"Limits result set to **{lim_val}** row(s).")

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
        bullets.append(f"Graph contains **{len(node_types)}** nodes (Projection: {pv}, Join: {jv}, Aggregation: {av}, Union: {uv}).")
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

# -------------------------------
# ABAP CDS summarization (NEW)
# -------------------------------
def summarize_abap_cds(cds: ABAPCDSModel) -> List[str]:
    bullets: List[str] = []
    bullets.append(f"Defines ABAP CDS **{cds.name}**{' (SQL View: ' + cds.sql_view_name + ')' if cds.sql_view_name else ''}.")
    if cds.extraction_enabled:
        bullets.append("**Extraction enabled** for replication/CDC (ABAP CDS pipeline).")
    else:
        bullets.append("Extraction **not enabled** — replication flows will not run until @Analytics.dataExtraction.enabled: true.")
    if cds.cdc_annotation:
        bullets.append(f"CDC annotation present: `{cds.cdc_annotation}`.")
    if cds.parameters:
        bullets.append(f"Has **{len(cds.parameters)}** parameter(s) → Replication Flow **not supported for parameterized CDS**.")
    if cds.keys:
        bullets.append("Keys in SELECT: " + _compact_list(cds.keys))
    if cds.sources:
        bullets.append("Reads from: " + _compact_list(cds.sources))
    if cds.associations:
        bullets.append("Associations to: " + _compact_list(cds.associations))
    return bullets