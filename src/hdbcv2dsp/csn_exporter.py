# hdbcv2dsp/csn_exporter.py
# ======================================================================
# Neutral + Native CSN exporter for SAP Datasphere
#  - Neutral CSN for tables and (neutral) SQL views
#  - Native SQL View CSN using a tenant-exported template (inject SELECT)
#  - Replication Flow (ABAP CDS) CSN using a tenant-exported template
#
# This module is intentionally schema-agnostic for native JSON:
# - We "clone" a tenant-exported JSON and patch typical fields by name.
# - This makes it resilient to tenant version changes.
# ======================================================================

from __future__ import annotations

import io
import re
import json
import copy
import uuid
import zipfile
from datetime import datetime
from typing import Dict, List, Optional, Tuple

# Project types
from hdbcv2dsp.parse_sql_view import SQLViewModel
from hdbcv2dsp.parse_cv import CVModel
from hdbcv2dsp.parse_procedure import ProcedureModel
from hdbcv2dsp.parse_abap_cds import ABAPCDSModel
from hdbcv2dsp.artifacts import ArtifactNode


# ======================================================================
# Small utilities
# ======================================================================

def _sanitize(name: str) -> str:
    """Safe identifier for CSN definition names."""
    safe = re.sub(r"[^A-Za-z0-9_]", "_", (name or "").strip())
    return safe or ("OBJ_" + uuid.uuid4().hex[:8])


def _sanitize_name(name: str) -> str:
    """Alias for historical compatibility."""
    return _sanitize(name)


def _sql_select_body(sql: str) -> str:
    """
    Extract a clean SELECT statement:
    - If an input SQL contains CREATE VIEW ... AS SELECT ...,
      we return the 'SELECT ...' part.
    - If input is already a plain SELECT, return as-is.
    """
    if not sql:
        return ""
    text = sql.strip().strip(";")
    # Try to find "... AS SELECT"
    m = re.search(r"\bAS\s+SELECT\b", text, re.IGNORECASE)
    if m:
        return text[m.end()-6:].strip()  # include "SELECT"
    # If already starts with SELECT, return
    if re.match(r"^SELECT\b", text, re.IGNORECASE):
        return text
    # Otherwise return as-is
    return text


def _collect_sources_from_graph(graph: Optional[Dict[str, ArtifactNode]]) -> List[str]:
    """
    Best-effort collector of base sources (tables/remote tables) from the dependency graph.
    Returns a de-duplicated, sorted list of source identifiers (strings).
    Safe when graph is None.
    """
    if not graph:
        return []
    base_kinds = {"Table", "RemoteTable", "Dataset", "CSV", "ABAP_TABLE", "ABAP_VIEW"}
    src: set[str] = set()
    for node_id, node in graph.items():
        kind = (getattr(node, "kind", "") or "")
        if kind.upper() in {k.upper() for k in base_kinds}:
            src.add(node_id)
        # Also collect inputs of non-table nodes (often table names)
        for inp in getattr(node, "inputs", []) or []:
            if inp and not inp.startswith("#"):
                src.add(inp)
    # prune obvious non-table markers
    out = [s for s in src if s and not s.startswith("#")]
    return sorted(set(out))


# ======================================================================
# NEUTRAL CSN builder (tables + neutral SQL views)
# ======================================================================

def _make_neutral_csn(
    package_name: str,
    sql_views: List[SQLViewModel],
    base_sources: List[str],
    view_mode: str,                       # 'sql' | 'graphical' (we only emit neutral SQL)
    table_mode: str,                      # 'view_only' | 'tables_only' | 'local_stub'
    include_analytic: bool,               # reserved for future
    cv_model: Optional[CVModel],
    procedures: List[ProcedureModel],
    table_schemas: Optional[Dict[str, dict]] = None,
):
    """
    Build a minimal, neutral CSN that imports fine in Datasphere:
    - Optional local tables (from table_schemas or base_sources as stubs)
    - Optional SQL Views (neutral representation that we stash SQL text into
      under '_neutralView.sql' for our own round-trip)
    We keep this intentionally simple to avoid coupling to CSN schema changes.
    """
    csn_pkg = {
        "$version": "1.0",
        "version": {"csn": "1.0"},
        "definitions": {}
    }
    created_tables: List[str] = []

    # 1) Tables (explicit schemas)
    table_schemas = table_schemas or {}
    for t_name, spec in table_schemas.items():
        def_name = t_name
        elements = {}
        for col in spec.get("columns", []):
            el = dict(col)  # expected keys: name, and cds.* type fields already provided
            col_name = el.pop("name")
            elements[col_name] = el
        csn_pkg["definitions"][def_name] = {
            "kind": "entity",
            "elements": elements,
            "@EndUserText.label": t_name,
            "@ObjectModel.modelingPattern": {"#": "DATA_STRUCTURE"},
            "@ObjectModel.supportedCapabilities": [{"#": "DATA_STRUCTURE"}],
        }
        created_tables.append(t_name)

    # 2) Tables (stubs from base_sources) only if requested and not already defined
    if table_mode in ("tables_only", "local_stub"):
        for src in base_sources:
            if src in csn_pkg["definitions"]:
                continue  # skip if a view/table with that name already exists
            csn_pkg["definitions"][src] = {
                "kind": "entity",
                "elements": {
                    "__PLACEHOLDER__": {"type": "cds.String", "length": 1}
                },
                "@EndUserText.label": src,
                "@ObjectModel.supportedCapabilities": [{"#": "DATA_STRUCTURE"}],
            }
            created_tables.append(src)    
    # 3) Views (neutral) – only when not tables-only
    if table_mode != "tables_only":
        for v in sql_views or []:
            elems = _elements_from_view(v)
            if not elems:
                elems = {"COL1": {"type": "cds.String", "length": 500}}

            def_name = v.name
            sql_body = _sql_select_body(v.sql)
            
            csn_pkg["definitions"][def_name] = {
                "kind": "view",
                "@EndUserText.label": v.name,
                "elements": elems,
                # --- FIX: This block populates the Field List and the SQL Editor ---
                "query": {
                    "sql": sql_body
                },
                # For compatibility with some tenant versions:
                "@DataWarehouse.sqlEditor.query": sql_body 
            } 

    # (We do not emit Calculation Views or Procedures in this neutral package;
    #  the app generates a DOCX instead for those artifacts.)
    return {"csn": csn_pkg, "created_tables": created_tables}


def _simple_manifest(
    package_name: str,
    sql_views: List[SQLViewModel],
    cv_model: Optional[CVModel],
    procedures: List[ProcedureModel],
    graph: Optional[Dict[str, ArtifactNode]],
    table_mode: str,
    view_mode: str,
    include_analytic: bool,
    created_tables: List[str],
):
    return {
        "package": package_name,
        "generatedAt": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "tablesCreated": created_tables,
        "views": [getattr(v, "name", "UNKNOWN") for v in (sql_views or [])],
        "hasCalcView": bool(cv_model),
        "procedures": [getattr(p, "name", "UNKNOWN") for p in (procedures or [])],
        "mode": {
            "tables": table_mode,
            "views": view_mode,
            "analytic": include_analytic,
        },
    }


# ======================================================================
# NATIVE SQL VIEW helper: load & apply template
# ======================================================================

def _load_template(template_bytes: bytes) -> dict:
    if not template_bytes:
        raise ValueError("Empty template bytes.")
    return json.loads(template_bytes.decode("utf-8"))


def _apply_native_template(template: dict, view_model: SQLViewModel) -> dict:
    """
    Clone a native SQL View template and inject:
      - definition name (sanitized)
      - EndUser label (if present)
      - SQL body into @DataWarehouse.sqlEditor.query
      - elements inferred from the uploaded SELECT
    Also removes the template's "query" node so the editor uses the injected SQL.
    """
    tpl = copy.deepcopy(template)
    if "definitions" not in tpl or not tpl["definitions"]:
        raise ValueError("Native template JSON missing 'definitions'.")

    # choose first definition as base
    base_key = sorted(tpl["definitions"].keys())[0]
    base_obj = tpl["definitions"][base_key]

    new_name = _sanitize(view_model.name)
    sql_body = _sql_select_body(view_model.sql)

    obj = copy.deepcopy(base_obj)

    # Set a nice label if present
    if "@EndUserText.label" in obj:
        obj["@EndUserText.label"] = view_model.name

    # 1) Put user's SQL in the SQL editor view
    obj["@DataWarehouse.sqlEditor.query"] = sql_body

    # 2) Remove the template's "query" (forces editor to use the injected SQL)
    obj.pop("query", None)

    # 3) Clean folder assignment noise from some templates
    if "_meta" in obj and "dependencies" in obj["_meta"]:
        obj["_meta"]["dependencies"].pop("folderAssignment", None)
        if not obj["_meta"]["dependencies"]:
            obj["_meta"].pop("dependencies")
        if not obj["_meta"]:
            obj.pop("_meta")

    # 4) Build elements from the uploaded SQL (aliases/heuristics)
    #    This ensures the view validates after import.
    
    obj["@DataWarehouse.sqlEditor.query"] = sql_body
    obj.pop("query", None)
    elems = _elements_from_view(view_model)
    if elems:
        obj["elements"] = elems

    return {
        "$version": tpl.get("$version", "1.0"),
        "version": tpl.get("version", {"csn": "1.0"}),
        "definitions": { new_name: obj }
    }

# --- Helpers used by _apply_native_template ---

def _extract_select_segment(sql: str):
    m = re.search(r"\bselect\b(.*?)\bfrom\b", sql, flags=re.I | re.S)
    return m.group(1).strip() if m else ""

def _split_comma(segment: str) -> List[str]:
    out, buf, d = [], [], 0
    for ch in segment:
        if ch == '(':
            d += 1
        elif ch == ')':
            d -= 1
        if ch == ',' and d == 0:
            out.append("".join(buf).strip()); buf = []
        else:
            buf.append(ch)
    if buf:
        out.append("".join(buf).strip())
    return out

def _extract_alias(expr: str) -> Optional[str]:
    expr = expr.strip()
    m = re.search(r"\bAS\s+([A-Za-z_][\w$]*)\b", expr, flags=re.I)
    if m:
        return m.group(1)
    # trailing identifier after a space: "... expr alias"
    m = re.search(r"\s+([A-Za-z_][\w$]*)\s*$", expr)
    if m and " " in expr.strip():
        return m.group(1)
    return None

def _infer_type(expr: str) -> str:
    u = expr.upper()
    # numeric aggregations
    if any(fn+"(" in u for fn in ("SUM", "AVG", "MIN", "MAX", "STDDEV", "VARIANCE")):
        return "cds.Decimal"
    # count -> integer
    if "COUNT(" in u:
        return "cds.Integer"
    # casts
    if re.search(r"\bCAST\s*\(.*?AS\s+(INT|INTEGER|DECIMAL|NUMERIC|BIGINT|DOUBLE)\b", u):
        return "cds.Decimal"
    if re.search(r"\bDATE\b|\bTO_DATE\(", u):
        return "cds.Date"
    if re.search(r"\bTIMESTAMP\b", u):
        return "cds.Timestamp"
    return "cds.String"

def _apply_defaults(elem_type: str) -> Dict[str, object]:
    if elem_type == "cds.String":
        return {"type": "cds.String", "length": 500}
    if elem_type == "cds.Decimal":
        return {"type": "cds.Decimal", "precision": 38, "scale": 10}
    if elem_type == "cds.Integer":
        return {"type": "cds.Integer"}
    if elem_type in ("cds.Date", "cds.Time", "cds.Timestamp"):
        return {"type": elem_type}
    return {"type": elem_type}

def _elements_from_view(v) -> dict[str, dict]:
    """
    Infer Datasphere columns from the uploaded SQL:
      1) alias (quoted/unquoted) if present
      2) else simple ref name (EMP_GROUP) if expr is a plain reference
      3) else COL#
    """
    elems: dict[str, dict] = {}

    # Prefer pre-parsed columns if your parse_hdbview_or_sql provided them
    raw_cols = (getattr(v, "columns", None) or [])[:500]

    # If unavailable, parse SELECT list
    if not raw_cols:
        sql = getattr(v, "sql", "") or ""
        m = re.search(r'\bselect\b(.*?)\bfrom\b', sql, flags=re.I | re.S)
        segment = m.group(1).strip() if m else ""
        raw_cols = _split_comma(segment) if segment else []

    if not raw_cols:
        # Fallback to a single placeholder column to keep the view valid
        return {"COL1": {"type": "cds.String", "length": 500}}

    def _infer_type(expr: str) -> str:
        u = (expr or "").upper()
        if "COUNT(" in u:
            return "cds.Integer"
        if any(fn + "(" in u for fn in ("SUM", "AVG", "MIN", "MAX", "STDDEV", "VARIANCE")):
            return "cds.Decimal"
        if re.search(r'\bTIMESTAMP\b|\bCURRENT_TIMESTAMP\b', u):
            return "cds.Timestamp"
        if re.search(r'\bDATE\b|\bTO_DATE\s*\(', u):
            return "cds.Date"
        return "cds.String"

    def _apply_defaults(t: str) -> dict:
        if t == "cds.String":
            return {"type": "cds.String", "length": 500}
        if t == "cds.Decimal":
            return {"type": "cds.Decimal", "precision": 38, "scale": 10}
        if t == "cds.Integer":
            return {"type": "cds.Integer"}
        if t in ("cds.Date", "cds.Time", "cds.Timestamp"):
            return {"type": t}
        return {"type": t}

    seen: set[str] = set()
    for i, raw in enumerate(raw_cols, 1):
        expr = raw.strip()
        # 1) explicit alias
        name = _extract_alias(expr)
        # 2) simple reference (EMP_GROUP or "EMP_GROUP")
        if not name:
            name = _extract_simple_ref_name(expr)
        # 3) fallback
        if not name:
            name = f"COL{i}"
        # sanitize: strip quotes, but keep original case
        name = name.strip().strip('"').strip('`').strip('[]')
        # ensure unique
        base = name
        k = 2
        while name.upper() in seen:
            name = f"{base}_{k}"
            k += 1
        seen.add(name.upper())

        t = _infer_type(expr)
        elems[name] = _apply_defaults(t)

    return elems    

# ======================================================================
# REPLICATION FLOW (ABAP CDS) — template patcher
# ======================================================================

def _first_def_key(template: Dict) -> Optional[str]:
    defs = template.get("definitions") or {}
    keys = sorted(defs.keys())
    return keys[0] if keys else None


def _deep_find_paths(obj, key_pred):
    """
    Yields tuples (parent, key) for any dict 'parent' where parent[key] satisfies key_pred.
    """
    if isinstance(obj, dict):
        for k, v in obj.items():
            if key_pred(k, v, obj):
                yield (obj, k)
            # Recurse
            for t in _deep_find_paths(v, key_pred):
                yield t
    elif isinstance(obj, list):
        for it in obj:
            for t in _deep_find_paths(it, key_pred):
                yield t


def _patch_replication_objects(def_obj: Dict, cds_name: str, target_table: str,
                               load_type: str, content_type: Optional[str]) -> Dict:
    """
    Heuristically patch common JSON slots in a Replication Flow object:
      - '@EndUserText.label' → 'RF_<cds>'
      - Typical 'loadType' fields (InitialOnly, InitialAndDelta, DeltaOnly)
      - Source object slots (sourceObject/objectName/source/name under 'source' contexts)
      - Target table slots (targetObject/targetTable/tableName/objectName under 'target' contexts)
      - contentType (if present)
    """
    obj = copy.deepcopy(def_obj)

    # 1) EndUser label
    if "@EndUserText.label" in obj:
        obj["@EndUserText.label"] = _sanitize(f"RF_{cds_name}")

    # 2) Load Type normalization
    for parent, key in _deep_find_paths(obj, lambda k, v, p: isinstance(v, str) and k.lower() == "loadtype"):
        parent[key] = load_type.upper().replace(" ", "_")

    # 3) Source object names (ABAP CDS entity)
    name_fields = ("sourceObject", "objectName", "source", "name")
    def is_name_slot(k, v, p):
        if isinstance(v, str) and any(k.lower() == f for f in (f.lower() for f in name_fields)):
            pk = {kk.lower() for kk in p.keys()} if isinstance(p, dict) else set()
            # crude context hint
            return ("source" in pk) or ("sourceconnection" in pk) or ("replication" in pk)
        return False

    for parent, key in _deep_find_paths(obj, is_name_slot):
        parent[key] = cds_name

    # 4) Target mapping (table)
    tgt_fields = ("targetObject", "targetTable", "tableName", "objectName")
    def is_target_slot(k, v, p):
        if isinstance(v, str) and any(k.lower() == f for f in (f.lower() for f in tgt_fields)):
            pk = {kk.lower() for kk in p.keys()} if isinstance(p, dict) else set()
            return ("target" in pk) or ("targetconnection" in pk) or ("replication" in pk)
        return False

    for parent, key in _deep_find_paths(obj, is_target_slot):
        parent[key] = target_table

    # 5) Content Type for ABAP-based sources (if present)
    if content_type:
        for parent, key in _deep_find_paths(obj, lambda k, v, p: isinstance(v, str) and k.lower() == "contenttype"):
            parent[key] = content_type

    return obj


def _apply_rf_template(template_bytes: bytes,
                       cds: ABAPCDSModel,
                       load_type: str = "INITIAL_AND_DELTA",
                       content_type: Optional[str] = None,
                       target_table: Optional[str] = None) -> Dict:
    template = json.loads(template_bytes.decode("utf-8"))
    base_key = _first_def_key(template)
    if not base_key:
        raise ValueError("Invalid Replication Flow template JSON (no definitions found).")
    base_obj = copy.deepcopy(template["definitions"][base_key])

    new_name = _sanitize(f"RF_{cds.name}")
    new_obj = _patch_replication_objects(
        base_obj,
        cds_name=cds.name,
        target_table=target_table or _sanitize(cds.name),
        load_type=load_type,
        content_type=content_type,
    )

    return {
        "$version": template.get("$version", "1.0"),
        "version": template.get("version", {"csn": "1.0"}),
        "definitions": {new_name: new_obj},
    }

# --- new: extract a simple column reference name (with optional schema/alias and quoting) ---
_SIMPLE_REF_RE = re.compile(
    r'''^\s*
        (?:
            (?:"[^"]+"|`[^`]+`|\[[:\w\$]+\])\s*\.\s*   # optional qualifier "SCHEMA"."TABLE" or alias
        )?
        (?:"([^"]+)"|`([^`]+)`|\[([A-Za-z_][\w\$]*)\]|([A-Za-z_][\w\$]*))
        \s*$
    ''',
    re.X
)

def _extract_simple_ref_name(expr: str) -> str | None:
    """Return the column name if expr is a simple reference (with or without quoting)."""
    m = _SIMPLE_REF_RE.match(expr or "")
    if not m:
        return None
    # capture groups represent the same name in different quoting styles
    for g in m.groups():
        if g:
            return g
    return None

# --- improved: alias extraction with support for quoted identifiers and trailing alias ---
def _extract_alias(expr: str) -> str | None:
    s = (expr or "").strip()
    # ... AS "Alias" | AS [Alias] | AS `Alias`
    m = re.search(r'\bAS\s+("([^"]+)"|`([^`]+)`|\[([A-Za-z_][\w\$]*)\]|([A-Za-z_][\w\$]*))\b',
                  s, flags=re.I)
    if m:
        for i in range(2, 6):
            if m.group(i):
                return m.group(i)

    # trailing alias without AS (… expr "Alias"), quoted or unquoted
    m = re.search(r'\s+("([^"]+)"|`([^`]+)`|\[([A-Za-z_][\w\$]*)\]|([A-Za-z_][\w\$]*))\s*$',
                  s)
    if m:
        for i in range(2, 6):
            if m.group(i):
                return m.group(i)

    # no alias found
    return None

# --- minor: keep your segment splitter (comma-safe with parentheses), re-using yours or this ---
def _split_comma(segment: str) -> list[str]:
    out, buf, d, in_str, esc = [], [], 0, None, False
    for ch in segment:
        if in_str:
            buf.append(ch)
            if esc:
                esc = False
            elif ch == '\\':
                esc = True
            elif (in_str == '"' and ch == '"') or (in_str == "'" and ch == "'"):
                in_str = None
            continue
        if ch in ("'", '"'):
            in_str = ch
            buf.append(ch)
            continue
        if ch == '(':
            d += 1
        elif ch == ')':
            d = max(0, d - 1)
        if ch == ',' and d == 0:
            out.append("".join(buf).strip()); buf = []
        else:
            buf.append(ch)
    if buf:
        out.append("".join(buf).strip())
    return out

# ----------------------------------------------------------------------
# Replication Flow helpers (place with other helpers, before main build)
# ----------------------------------------------------------------------
def _detect_rf_shape(template: dict) -> str:
    """
    Returns 'definitions' if RF objects live under template['definitions'],
    'replicationflows' if they live under template['replicationflows'],
    or raises if none found.
    """
    if isinstance(template.get("replicationflows"), dict) and template["replicationflows"]:
        return "replicationflows"
    if isinstance(template.get("definitions"), dict) and template["definitions"]:
        return "definitions"
    raise ValueError("Template JSON has neither 'replicationflows' nor 'definitions' section with content.")

def _apply_rf_template(template_bytes: bytes,
                       cds: ABAPCDSModel,
                       load_type: str = "INITIAL_AND_DELTA",
                       content_type: Optional[str] = None,
                       target_table: Optional[str] = None) -> Dict:
    """
    Clone a Replication Flow template that may store flows under either:
      - template['replicationflows'][<name>], or
      - template['definitions'][<name>]
    Then patch:
      - RF label, load type, content type
      - Source object name (ABAP CDS)
      - Target table name
    """
    template = json.loads(template_bytes.decode("utf-8"))
    section = _detect_rf_shape(template)

    rf_map = template["replicationflows"] if section == "replicationflows" else template["definitions"]
    if not rf_map:
        raise ValueError(f"No objects found under '{section}' in template.")

    base_key = sorted(rf_map.keys())[0]
    base_obj = copy.deepcopy(rf_map[base_key])

    new_name = _sanitize(f"RF_{cds.name}")
    obj = copy.deepcopy(base_obj)

    # --- Label
    if "@EndUserText.label" in obj:
        obj["@EndUserText.label"] = new_name

    # --- RF-wide settings (content type)
    contents = obj.get("contents", {})
    rfs = contents.get("replicationFlowSetting", {})
    if isinstance(rfs, dict) and content_type:
        # Examples: "Native Type" or "Template Type" depending on tenant release
        rfs["ABAPcontentType"] = content_type
        rfs["ABAPcontentTypeDisabled"] = False
        obj.setdefault("contents", {})["replicationFlowSetting"] = rfs

    # --- Task load types and source/target names
    tasks = contents.get("replicationTasks", [])
    for t in tasks:
        if not isinstance(t, dict):
            continue
        # Map UI choice to tenant loadType keywords
        if "loadType" in t:
            lt = load_type.upper().replace(" ", "_")
            # Normalize common aliases
            if lt == "INITIAL_ONLY": lt = "INITIAL"
            if lt == "DELTA_ONLY":   lt = "DELTA"
            t["loadType"] = lt
        src = t.get("sourceObject")
        if isinstance(src, dict) and "name" in src:
            src["name"] = cds.name  # ABAP CDS entity name
        tgt = t.get("targetObject")
        if isinstance(tgt, dict) and "name" in tgt:
            tgt["name"] = target_table or _sanitize(cds.name)
    obj.setdefault("contents", {})["replicationTasks"] = tasks

    # --- Return a package in the same shape as the template
    pkg = {"$version": template.get("$version", "1.0"),
           "version": template.get("version", {"csn": "1.0"})}
    if section == "replicationflows":
        pkg["replicationflows"] = { new_name: obj }
    else:
        pkg["definitions"] = { new_name: obj }
    return pkg

# ======================================================================
# MAIN ENTRY — build zip bytes & manifest
# ======================================================================

def build_csn_artifacts_zip(
    *,
    package_name: str,
    cv_model: Optional[CVModel],
    sql_views: List[SQLViewModel],
    procedures: List[ProcedureModel],
    graph: Optional[Dict[str, ArtifactNode]],
    table_mode: str = "view_only",        # 'view_only' | 'tables_only' | 'local_stub'
    view_mode: str = "sql",
    include_analytic: bool = False,
    native_template_bytes: Optional[bytes] = None,
    native_single_file: bool = False,      # reserved
    table_schemas: Optional[Dict[str, dict]] = None,
    native_output_mode: str = "neutral",   # "neutral" | "native" | "both"
    # --- NEW for Replication Flow (ABAP CDS)
    abap_cds: Optional[ABAPCDSModel] = None,
    rf_load_type: str = "INITIAL_AND_DELTA",
    rf_content_type: Optional[str] = None,
    rf_target_table: Optional[str] = None,
) -> Tuple[bytes, dict]:
    """
    Builds a zip that contains one or more of:
      - csn.json (+ manifest.json) for Neutral
      - native_csn.json for Native SQL View (when 'both' mode)
      - replication_csn.json or csn.json for Replication Flow (ABAP CDS)
      - views_sql/<name>.sql for readable SQL snippets (neutral)
      - README.md with guidance
    """
    sql_views = list(sql_views or [])
    procedures = list(procedures or [])
    table_schemas = table_schemas or {}

    # ---------------- Collect sources from graph (for stubs if needed)
    base_sources = _collect_sources_from_graph(graph)

    # ---------------- Neutral CSN build (tables + neutral views)
    neutral = _make_neutral_csn(
        package_name=package_name,
        sql_views=sql_views,
        base_sources=base_sources,
        view_mode=view_mode,
        table_mode=table_mode,
        include_analytic=include_analytic,
        cv_model=cv_model,
        procedures=procedures,
        table_schemas=table_schemas,
    )
    created_tables = neutral["created_tables"]
    csn = neutral["csn"]

    # ---------------- Manifest (common)
    manifest = _simple_manifest(
        package_name, sql_views, cv_model, procedures, graph,
        table_mode, view_mode, include_analytic, created_tables
    )

    # ---------------- Write zip
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as z:
        
# If we're generating a Replication Flow, never write the neutral package.
        if abap_cds:
            write_neutral = False
        else:
            write_neutral = (
                (native_output_mode == "neutral")
                or (native_output_mode == "both")
                or not native_template_bytes
            )
        write_native = (
            native_template_bytes is not None
            and native_output_mode in ("native", "both")
        )

        # ============== Neutral CSN (tables + neutral views) ==============
        if write_neutral:
            z.writestr("csn.json", json.dumps(csn, indent=2))
            z.writestr("manifest.json", json.dumps(manifest, indent=2))
            # for convenience, write SELECT bodies for views
            if table_mode != "tables_only":
                for v in sql_views:
                    z.writestr(f"views_sql/{v.name}.sql", _sql_select_body(v.sql))

        # ============== Native SQL Views (template) =======================
        if write_native and sql_views and table_mode != "tables_only" and not abap_cds:
            template = _load_template(native_template_bytes)
            merged_defs = {}
            pkg = {"$version": "1.0", "version": {"csn": "1.0"}}
            for v in sql_views:
                obj = _apply_native_template(template, v)
                merged_defs.update(obj["definitions"])
            pkg["definitions"] = merged_defs
            if native_output_mode == "native":
                # Overwrite "csn.json" with the native package if native-only was requested
                z.writestr("csn.json", json.dumps(pkg, indent=2))
                z.writestr("manifest.json", json.dumps(manifest, indent=2))
            else:
                # Include it alongside the neutral package
                z.writestr("native_csn.json", json.dumps(pkg, indent=2))

        # ============== Replication Flow (ABAP CDS) =======================
        if abap_cds and native_template_bytes:
            rf_pkg = _apply_rf_template(
                native_template_bytes, abap_cds,
                load_type=rf_load_type,
                content_type=rf_content_type,
                target_table=rf_target_table or _sanitize(abap_cds.name)
            )
            if native_output_mode == "native":
                # If exclusive native requested (for RF we always consider native)
                z.writestr("csn.json", json.dumps(rf_pkg, indent=2))
                z.writestr("manifest.json", json.dumps(manifest, indent=2))
            else:
                # Save next to neutral
                z.writestr("replication_csn.json", json.dumps(rf_pkg, indent=2))

        # ============== README ===========================================
        if abap_cds:
            z.writestr(
                "README.md",
                "This package contains a Replication Flow CSN generated from an ABAP CDS view/entity. "
                "After import in Datasphere, adjust source/target connections if prompted, deploy, then run.\n"
            )
        elif table_mode == 'tables_only' and created_tables:
            z.writestr(
                "README.md",
                "This package contains table entities only. Import csn.json and deploy tables before creating views.\n"
            )
        elif table_mode == 'view_only':
            if write_neutral and not write_native:
                z.writestr(
                    "README.md",
                    "Neutral CSN (views only). Ensure referenced tables exist before deploying views.\n"
                )
            elif write_native and not write_neutral:
                z.writestr(
                    "README.md",
                    "Native SQL View package (views only). SQL will appear in the SQL editor after import.\n"
                )
            else:
                z.writestr(
                    "README.md",
                    "Both neutral (csn.json) and native (native_csn.json) packages included.\n"
                )

    out.seek(0)
    return out.read(), manifest