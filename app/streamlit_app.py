# streamlit_app.py
import os
import re
import json
import base64
import tempfile
from datetime import datetime
from typing import Dict, List, Optional

import streamlit as st

# ------------------------------ Page config ------------------------------
st.set_page_config(
    page_title="SAP Datasphere Program Accelerator",
    page_icon="🟦",
    layout="wide",
    initial_sidebar_state="collapsed",
)

APP_LOGO = os.path.join(os.path.dirname(__file__), "assets", "logo.png")

# ------------------------------ Session defaults (preferences) ------------------------------
# Document defaults
if "out_name" not in st.session_state:
    st.session_state.out_name = f"Rebuild_Guide_{datetime.now().strftime('%Y%m%d_%H%M%S')}.docx"
if "doc_title" not in st.session_state:
    st.session_state.doc_title = ""

# Export preferences
st.session_state.setdefault("pref_generation_mode", "Create View(s) only (no tables)")
st.session_state.setdefault("pref_view_mode", "SQL View (recommended)")
st.session_state.setdefault("pref_native_output", "Neutral only (csn.json)")

# ------------------------------ Project imports ------------------------------
from hdbcv2dsp.parse_cv import parse_hdbcalculationview, topo_order
from hdbcv2dsp.parse_sql_view import parse_hdbview_or_sql, SQLViewModel
from hdbcv2dsp.parse_procedure import parse_hdbprocedure_or_sql, ProcedureModel
from hdbcv2dsp.parse_abap_cds import parse_abap_cds_text, ABAPCDSModel  # NEW
from hdbcv2dsp.unify import (
    graph_from_cv,
    graph_from_sql_views,
    graph_from_procedures,
    graph_from_abap_cds,  # NEW
    merge_graphs,
)
from hdbcv2dsp.render_docx_general import render_docx_general
from hdbcv2dsp.csn_exporter import build_csn_artifacts_zip

# ------------------------------ Small helpers ------------------------------
def _logo_img_tag(path: str, height_px: int = 72, alt: str = "Blueprint Technologies") -> str:
    """Return an <img> tag (base64) for the app header; sized for a prominent masthead."""
    if not os.path.exists(path):
        return ""
    with open(path, "rb") as f:
        b64 = base64.b64encode(f.read()).decode("ascii")
    return (
        f'<img alt="{alt}" src="data:image/png;base64,{b64}" '
        f'style="height:{height_px}px; width:auto; display:block;" />'
    )

def sanitize_filename(name: str) -> str:
    name = (name or "").strip()
    if not name:
        name = "Rebuild_Guide.docx"
    # Remove generally illegal characters for filenames across platforms
    name = re.sub(r'[\/:*?"<>|\\]+', "", name)
    name = name.strip(" .")
    if not name.lower().endswith(".docx"):
        name += ".docx"
    return name or "Rebuild_Guide.docx"

def _guess_cds_type(sql_type: str) -> Dict[str, object]:
    t = (sql_type or "").strip().upper()
    if t.startswith("INT") or t in {"INTEGER", "BIGINT", "SMALLINT"}:
        return {"type": "cds.Integer"}
    if (
        t.startswith("DEC")
        or t.startswith("NUM")
        or t.startswith("REAL")
        or t.startswith("DOUBLE")
        or t.startswith("FLOAT")
    ):
        return {"type": "cds.Decimal", "precision": 38, "scale": 10}
    if t == "DATE":
        return {"type": "cds.Date"}
    if t == "TIME":
        return {"type": "cds.Time"}
    if t in {"TIMESTAMP", "DATETIME"}:
        return {"type": "cds.Timestamp"}
    m = re.match(r'(?:VAR)?CHAR\s*\((\d+)\)', t)
    if m:
        return {"type": "cds.String", "length": int(m.group(1))}
    m2 = re.match(r'(?:N)?VAR?CHAR\s*\((\d+)\)', t)
    if m2:
        return {"type": "cds.String", "length": int(m2.group(1))}
    return {"type": "cds.String", "length": 500}

def _split_columns_block(cols_block: str) -> List[str]:
    parts: List[str] = []
    buf: List[str] = []
    depth = 0
    for ch in cols_block:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth = max(0, depth - 1)
        if ch == "," and depth == 0:
            parts.append("".join(buf).strip())
            buf = []
        else:
            buf.append(ch)
    if buf:
        parts.append("".join(buf).strip())
    return parts

def _parse_ddl_sql(text: str) -> Optional[Dict[str, dict]]:
    """Parse simple CREATE TABLE DDL -> canonical schema.
    Returns {table_name: {"columns": [{"name":..., "type":..., ...}]}}
    """
    try:
        m = re.search(
            r'CREATE\s+TABLE\s+(["\w\.]+)\s*\((.*)\)\s*;?',
            text,
            flags=re.I | re.S,
        )
        if not m:
            return None
        table_name = m.group(1).strip().strip('"')
        cols_block = m.group(2)
        parts = _split_columns_block(cols_block)
        columns = []
        for p in parts:
            if re.match(r"^(PRIMARY\s+KEY|FOREIGN\s+KEY|CONSTRAINT)\b", p, flags=re.I):
                continue
            toks = p.split()
            if len(toks) < 2:
                continue
            col_name = toks[0].strip().strip('"')
            col_type = " ".join(toks[1:])
            elem = _guess_cds_type(col_type)
            elem_dict = {"name": col_name, **elem}
            if re.search(r"PRIMARY\s+KEY", p, flags=re.I):
                elem_dict["key"] = True
            columns.append(elem_dict)
        if not columns:
            return None
        return {table_name: {"columns": columns}}
    except Exception:
        return None

def _parse_csv_header(text: str, inferred_table_name: str) -> Optional[Dict[str, dict]]:
    try:
        first = text.splitlines()[0]
        headers = [h.strip() for h in first.split(",") if h.strip()]
        if not headers:
            return None
        cols = [{"name": h, "type": "cds.String", "length": 500} for h in headers]
        return {inferred_table_name: {"columns": cols}}
    except Exception:
        return None

def _parse_json_schema(text: str, inferred_table_name: str) -> Optional[Dict[str, dict]]:
    try:
        obj = json.loads(text)
        if isinstance(obj, dict) and "columns" in obj:
            return {inferred_table_name: {"columns": obj["columns"]}}
        if isinstance(obj, dict) and obj:
            k = list(obj.keys())[0]
            return {k: obj[k]}
        return None
    except Exception:
        return None

def parse_uploaded_schemas(files) -> Dict[str, dict]:
    schemas: Dict[str, dict] = {}
    if not files:
        return schemas
    for fx in files:
        name = fx.name
        ext = os.path.splitext(name)[1].lower()
        content = fx.read().decode("utf-8", errors="ignore")
        parsed = None
        if ext == ".sql":
            parsed = _parse_ddl_sql(content)
        elif ext == ".csv":
            base = os.path.splitext(os.path.basename(name))[0]
            parsed = _parse_csv_header(content, base)
        elif ext == ".json":
            base = os.path.splitext(os.path.basename(name))[0]
            parsed = _parse_json_schema(content, base)
        if parsed:
            schemas.update(parsed)
    return schemas

# ------------------------------ Header ------------------------------
def render_header():
    # Tunables for look & feel
    LOGO_HEIGHT = 80        # increase/decrease to taste (e.g., 72–96)
    TITLE_FONTSIZE = 38     # large title like your old masthead
    SUBTITLE_FONTSIZE = 16  # subtle subtitle
    MAX_WIDTH = 1400        # keeps header from stretching too wide

    logo_html = _logo_img_tag(APP_LOGO, height_px=LOGO_HEIGHT)

    st.markdown(
        f"""
<style>
/* Contain header width on very wide screens */
.header-wrap {{
  max-width: {MAX_WIDTH}px;
  margin: 0 auto 0.75rem auto;
}}
.header-row {{
  display: grid;
  grid-template-columns: auto 1fr;
  column-gap: 18px;
  align-items: center;
}}
.header-title {{
  margin: 0;
  font-weight: 800;
  line-height: 1.15;
  font-size: {TITLE_FONTSIZE}px;
}}
.header-sub {{
  margin: 6px 0 0 0;
  font-size: {SUBTITLE_FONTSIZE}px;
  color: var(--text-color-secondary, rgba(49,51,63,.7));
}}
/* Respect Streamlit themes (light/dark) */
:root {{
  --text-color-primary: var(--text-color, rgba(49,51,63,1));
  --text-color-secondary: rgba(49,51,63,.7);
}}
/* Reduce title size slightly on small screens */
@media (max-width: 720px) {{
  .header-title {{ font-size: {max(26, TITLE_FONTSIZE-8)}px; }}
  .header-row {{ grid-template-columns: auto 1fr; column-gap: 12px; }}
}}
</style>


<div class="header-wrap">
  <div class="header-row">
    <div class="header-logo">{logo_html}</div>
    <div class="header-text">
      <h1 class="header-title">SAP Datasphere Program Accelerator</h1>
      <div class="header-sub">
        Upload HANA Calculation Views, SQL Views, or Stored Procedures → auto‑generate a Datasphere rebuild guide and importable CSN/JSON.
      </div>
    </div>
  </div>
</div>
""",
        unsafe_allow_html=True,
    )

# Render it once at the very top
render_header()

# ------------------------------ Tabs (DEFINE BEFORE USING) ------------------------------
main_tab, export_tab = st.tabs(
    ["📄 Rebuild Guide (DOCX)", "📦 Export (CSN/JSON)"]
)

# =====================================================================
# DOCX TAB
# =====================================================================
with main_tab:
    st.markdown("#### 📤 Upload")
    uploaded = st.file_uploader(
        "Upload a Calculation View (.hdbcalculationview/.xml), SQL View (.hdbview/.sql), Stored Procedure (.hdbprocedure/.sql), or ABAP CDS (.cds/.txt)",
        type=["hdbcalculationview", "xml", "hdbview", "hdbprocedure", "sql", "cds", "txt"],
        key="uploader_main",
    )

    st.markdown("#### 📝 Document Settings")
    with st.container():
        colA, colB = st.columns([2, 1])
        with colA:
            st.session_state.doc_title = st.text_input(
                "Document Title (optional)",
                value=st.session_state.doc_title,
                key="doc_title_input_main",
            )
            st.session_state.out_name = st.text_input(
                "Output filename",
                value=st.session_state.out_name,
                key="outname_main",
            )
        with colB:
            if st.button("⏱️ Use timestamp filename", key="ts_btn_main"):
                st.session_state.out_name = (
                    f"Rebuild_Guide_{datetime.now().strftime('%Y%m%d_%H%M%S')}.docx"
                )

    generate_and_download = st.button("🧾 Generate DOCX", type="primary", key="gen_docx_main")

    if uploaded:
        try:
            tmp_dir = tempfile.gettempdir()
            safe_name = os.path.basename(uploaded.name) or "uploaded.sql"
            tmp_path = os.path.join(tmp_dir, safe_name)
            content = uploaded.read()
            with open(tmp_path, "wb") as f:
                f.write(content)

            ext = os.path.splitext(safe_name)[1].lower()
            cv_model = None
            sql_views: List[SQLViewModel] = []
            procedures: List[ProcedureModel] = []
            abap_cds_list: List[ABAPCDSModel] = []

            if ext in [".hdbcalculationview", ".xml"]:
                cv_model = parse_hdbcalculationview(tmp_path)
                with st.expander("🧩 Calculation View summary", expanded=True):
                    st.write(f"**ID:** `{cv_model.cv_id}`")
                    if getattr(cv_model, "description", None):
                        st.write(f"**Description:** {cv_model.description}")
                    st.write(
                        f"**Output View Type:** `{cv_model.output_view_type}` "
                        f"Data Category: {cv_model.data_category}",
                        unsafe_allow_html=True,
                    )
                    if getattr(cv_model, "parameters", None):
                        st.write("**Parameters:**")
                        for p in cv_model.parameters:
                            st.code(
                                f"{p['id']} ({p['sqlType']}), default={p.get('defaultValue')}, "
                                f"mandatory={p.get('isMandatory')}"
                            )
                    if getattr(cv_model, "data_sources", None):
                        st.write("**Data Sources:**")
                        for ds_id, uri in cv_model.data_sources.items():
                            st.code(f"{ds_id} -> {uri}")
                    st.write("**Nodes (topological order):**")
                    order = topo_order(cv_model)
                    for idx, nid in enumerate(order, start=1):
                        n = cv_model.nodes[nid]
                        st.write(f"{idx}. `{n.node_id}` — {n.node_type}")

            elif ext in [".hdbview", ".sql", ".hdbprocedure"]:
                text = content.decode("utf-8", errors="ignore")
                text_u = text.upper()
                is_proc = bool(
                    re.search(r"\b(CREATE|ALTER)\s+(OR\s+REPLACE\s+)?(PROCEDURE|PROC)\b", text_u)
                )
                is_view = (
                    ext == ".hdbview"
                    or bool(re.search(r"\b(CREATE|ALTER)\s+(OR\s+REPLACE)\s+VIEW\b", text_u))
                    or bool(re.search(r"\b(CREATE|ALTER)\s+VIEW\b", text_u))
                )
                if is_proc:
                    proc = parse_hdbprocedure_or_sql(tmp_path)
                    procedures.append(proc)
                    with st.expander("🛠️ Stored Procedure summary", expanded=True):
                        st.code(
                            f"""Name: {proc.name}
Parameters: {[f"{x['mode']} {x['name']} {x['type']}" for x in proc.parameters]}
Reads: {getattr(proc, 'reads_from', [])}
Writes: {getattr(proc, 'writes_to', [])}
Temp tables: {getattr(proc, 'temp_tables', [])}
Calls: {getattr(proc, 'calls', [])}"""
                        )
                elif is_view:
                    view = parse_hdbview_or_sql(tmp_path)
                    sql_views.append(view)
                    with st.expander("🧾 SQL View summary", expanded=True):
                        cols_preview = ", ".join(view.columns[:10]) + (" ..." if len(view.columns) > 10 else "")
                        st.code(f"Name: {view.name}\nInputs: {view.inputs}\nColumns: {cols_preview}")
                else:
                    st.warning("Unrecognized SQL content. Expecting CREATE/ALTER VIEW or CREATE/ALTER PROCEDURE/PROC.")
            else:
                # Assume ABAP CDS (.cds / .txt)
                txt = content.decode("utf-8", errors="ignore")
                cds = parse_abap_cds_text(txt)
                abap_cds_list.append(cds)
                with st.expander("📘 ABAP CDS summary", expanded=True):
                    st.write(f"**Name:** `{cds.name}`")
                    if cds.sql_view_name:
                        st.write(f"**SQL View:** `{cds.sql_view_name}`")
                    st.write(f"**Extraction enabled:** `{cds.extraction_enabled}`")
                    if cds.cdc_annotation:
                        st.write(f"**CDC:** `{cds.cdc_annotation}`")
                    st.write(f"**Parameters:** {len(cds.parameters)}")
                    if cds.sources:
                        st.code("Sources: " + ", ".join(cds.sources))

            # Build union graph
            graph: Dict[str, object] = {}
            if cv_model:
                graph = merge_graphs(graph, graph_from_cv(cv_model))
            if sql_views:
                graph = merge_graphs(graph, graph_from_sql_views(sql_views))
            if procedures:
                graph = merge_graphs(graph, graph_from_procedures(procedures))
            if abap_cds_list:
                graph = merge_graphs(graph, graph_from_abap_cds(abap_cds_list[0]))

            if generate_and_download:
                tmp_docx_path = os.path.join(tmp_dir, sanitize_filename(st.session_state.out_name))
                render_docx_general(
                    output_path=tmp_docx_path,
                    title=st.session_state.doc_title or None,
                    cv_model=cv_model,
                    sql_views=sql_views,
                    procedures=procedures,
                    graph=graph if graph else None,
                    abap_cds_list=abap_cds_list,  # NEW
                )
                with open(tmp_docx_path, "rb") as f:
                    data = f.read()
                st.download_button(
                    "⬇️ Download Rebuild Guide (.docx)",
                    data,
                    file_name=os.path.basename(tmp_docx_path),
                    mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                )
        except Exception as e:
            st.error(f"Failed to parse/generate: {e}")
    else:
        st.info(
            "Upload a Calculation View (.hdbcalculationview/.xml), SQL View (.hdbview/.sql), Procedure (.hdbprocedure/.sql), or ABAP CDS (.cds/.txt) to begin."
        )

# =====================================================================
# EXPORT TAB — three modes (Views-only, Tables-only, Replication Flow)
# =====================================================================
with export_tab:
    st.markdown("#### 📦 Export to CSN/JSON (Datasphere)")

    with st.expander("🧭 Choose what to generate", expanded=True):
        generation_mode = st.radio(
            "Mode",
            [
                "Create View(s) only (no tables)",
                "Create Local Table(s) from uploaded schemas",
                "Replication Flow (ABAP CDS)",  # NEW
            ],
            index=0,
            key="generation_mode",
            horizontal=True,
        )
        st.session_state["pref_generation_mode"] = generation_mode

        colA, colB = st.columns(2)
        with colA:
            include_analytic = st.checkbox(
                "Include Analytic Model (template)",
                value=False,
                key="include_analytic",
            )
        with colB:
            view_mode = st.selectbox(
                "View representation",
                ["SQL View (recommended)", "Graphical View (experimental)"],
                index=0 if st.session_state["pref_view_mode"].startswith("SQL View") else 1,
                key="view_mode",
            )
            st.session_state["pref_view_mode"] = view_mode

    # Prepare holders
    col_left, col_right = st.columns([2, 1])
    uploaded_exportas_files = None
    table_schemas: Dict[str, dict] = {}
    abap_cds_e: Optional[ABAPCDSModel] = None

    # ---------------------- VIEW-ONLY MODE ----------------------
    if generation_mode.startswith("Create View"):
        with col_left:
            with st.expander("📤 Upload artifact for export", expanded=True):
                uploaded_export = st.file_uploader(
                    "Upload .hdbcalculationview / .xml / .hdbview / .hdbprocedure / .sql",
                    type=["hdbcalculationview", "xml", "hdbview", "hdbprocedure", "sql"],
                    key="uploader_export",
                )
        with col_right:
            with st.expander("⚙️ Native SQL View template (optional)", expanded=True):
                st.caption(
                    "Upload a SQL View JSON exported from Datasphere. We'll clone its shape and inject your SELECT so the editor shows it."
                )
                native_template = st.file_uploader(
                    "Upload Native SQL View JSON",
                    type=["json"],
                    key="native_sqlview_template",
                    accept_multiple_files=False,
                )
                if native_template:
                    choice = st.selectbox(
                        "Output format",
                        [
                            "Neutral only (csn.json)",
                            "Native only (csn.json)",
                            "Both (neutral + native_csn.json)",
                        ],
                        index=0
                        if st.session_state["pref_native_output"].startswith("Neutral")
                        else (1 if st.session_state["pref_native_output"].startswith("Native only") else 2),
                        key="native_output_mode",
                    )
                    st.session_state["pref_native_output"] = choice
                else:
                    st.session_state["pref_native_output"] = "Neutral only (csn.json)"

        # ---------------------- Common: Package name ----------------------
        with st.expander("🏷️ Package name", expanded=False):
            package_name = st.text_input(
                "Name to embed in csn.json",
                value=f"ds_package_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                key="pkg_name",
            )

        # ---------------------- PARSE / PREP based on selection ----------------------
        cv_model_e = None
        sql_views_e: List[SQLViewModel] = []
        procedures_e: List[ProcedureModel] = []
        graph_e: Dict[str, object] = {}
        required_tables: List[str] = []

        if uploaded_export:
            try:
                tmp_dir = tempfile.gettempdir()
                safe_name = os.path.basename(uploaded_export.name) or "uploaded.sql"
                tmp_path = os.path.join(tmp_dir, safe_name)
                with open(tmp_path, "wb") as f:
                    f.write(uploaded_export.read())
                ext = os.path.splitext(safe_name)[1].lower()

                if ext in [".hdbcalculationview", ".xml"]:
                    cv_model_e = parse_hdbcalculationview(tmp_path)
                elif ext in [".hdbview", ".sql", ".hdbprocedure"]:
                    with open(tmp_path, "r", encoding="utf-8", errors="ignore") as fh:
                        text = fh.read()
                    text_u = text.upper()
                    is_proc = bool(
                        re.search(r"\b(CREATE|ALTER)\s+(OR\s+REPLACE\s+)?(PROCEDURE|PROC)\b", text_u)
                    )
                    is_view = (
                        ext == ".hdbview"
                        or bool(re.search(r"\b(CREATE|ALTER)\s+(OR\s+REPLACE)\s+VIEW\b", text_u))
                        or bool(re.search(r"\b(CREATE|ALTER)\s+VIEW\b", text_u))
                    )
                    if is_proc:
                        procedures_e.append(parse_hdbprocedure_or_sql(tmp_path))
                    elif is_view:
                        sql_views_e.append(parse_hdbview_or_sql(tmp_path))
                    else:
                        st.warning("Unrecognized SQL content. Expecting VIEW or PROCEDURE/PROC.")
                else:
                    st.warning("Unsupported file type for export.")

                if cv_model_e:
                    graph_e = merge_graphs(graph_e, graph_from_cv(cv_model_e))
                if sql_views_e:
                    graph_e = merge_graphs(graph_e, graph_from_sql_views(sql_views_e))
                if procedures_e:
                    graph_e = merge_graphs(graph_e, graph_from_procedures(procedures_e))

                # Determine base tables required by uploaded SQL views
                req = set()
                for v in sql_views_e:
                    for src in getattr(v, "inputs", []) or []:
                        req.add(src)
                required_tables = sorted(req)
            except Exception as e:
                st.error(f"Failed to parse artifact: {e}")

        # ---------------------- Validation section (left) ----------------------
        with col_left:
            with st.expander("🔎 Validation (prerequisites)", expanded=True):
                if sql_views_e and required_tables:
                    st.markdown("**Required base tables for the uploaded SQL View(s):**")
                    for t in required_tables:
                        st.write(f"- `{t}`")
                    st.checkbox(
                        "I confirm these tables already exist in the target Datasphere space (or I have imported/created them before deploying this view).",
                        value=False,
                        key="confirm_tables_exist_export",
                    )
                else:
                    st.caption("No base table prerequisites found (or no SQL View inputs detected).")

        # ---------------------- Quick actions + Generate / Download (RIGHT) ----------------------
        with col_right:
            st.markdown("#### ⚡ Quick actions")
            qa_cols = st.columns(3)
            qa_native = qa_cols[0].button(
                "⚡ View-only: Native (csn.json)",
                key="qa_view_native",
                disabled=not (uploaded_export and (sql_views_e or cv_model_e)),
            )
            qa_neutral = qa_cols[1].button(
                "⚡ View-only: Neutral (csn.json)",
                key="qa_view_neutral",
                disabled=not (uploaded_export and (sql_views_e or cv_model_e)),
            )
            qa_tables = qa_cols[2].button("⚡ Tables-only ZIP", key="qa_tables_zip", disabled=True)

            # Prepare native output mode for main Generate
            choice = st.session_state.get("native_output_mode", st.session_state["pref_native_output"])
            if choice.startswith("Neutral"):
                exporter_native_mode = "neutral"
            elif choice.startswith("Native only"):
                exporter_native_mode = "native"
            else:
                exporter_native_mode = "both"
            st.session_state["pref_native_output"] = choice

            def _do_build(selected_table_mode: str, selected_native_output: str, force_native_template: bool = False):
                try:
                    # Gates
                    if selected_table_mode == 'view_only' and sql_views_e and required_tables:
                        if not st.session_state.get("confirm_tables_exist_export", False):
                            st.error(
                                "Export skipped: Please ensure the required tables exist, or go back and create/import the table JSON first."
                            )
                            return None
                    # Normalize view mode and native bytes
                    mode_views = 'sql' if st.session_state.get('view_mode', 'SQL View (recommended)').startswith('SQL') else 'graphical'
                    nb = None
                    if native_template and (force_native_template or selected_native_output in ("native", "both")):
                        nb = native_template.read()
                    views_for_export = [] if selected_table_mode == 'tables_only' else sql_views_e
                    zip_bytes, manifest = build_csn_artifacts_zip(
                        package_name=package_name,
                        cv_model=None if selected_table_mode == 'tables_only' else cv_model_e,
                        sql_views=views_for_export,
                        procedures=[] if selected_table_mode == 'tables_only' else procedures_e,
                        graph=None if selected_table_mode == 'tables_only' else (graph_e if graph_e else None),
                        table_mode=selected_table_mode,      # 'view_only' or 'tables_only'
                        view_mode=mode_views,
                        include_analytic=include_analytic,
                        native_template_bytes=nb,
                        native_single_file=False,
                        table_schemas=table_schemas,
                        native_output_mode=selected_native_output,  # "neutral" | "native" | "both"
                    )
                    return zip_bytes, manifest
                except Exception as e:
                    st.error(f"Failed to build packages: {e}")
                    return None

            gen_csn_btn = st.button("🚀 Generate CSN/JSON", type="secondary", key="gen_csn")

        # Execute quick actions or main generate
        build_result = None
        if qa_native:
            build_result = _do_build('view_only', 'native', force_native_template=True)
        elif qa_neutral:
            build_result = _do_build('view_only', 'neutral')
        elif qa_tables:
            build_result = _do_build('tables_only', 'neutral')

        if not build_result and gen_csn_btn:
            tm = 'view_only'
            build_result = _do_build(tm, exporter_native_mode)

        with col_right:
            if build_result:
                zip_bytes, manifest = build_result
                st.success("✅ Package generated.")
                st.download_button(
                    label="⬇️ Download Export Package (ZIP)",
                    data=zip_bytes,
                    file_name=f"{package_name}.zip",
                    mime="application/zip",
                    key="dl_csn_zip",
                )
                with st.expander("🧾 Manifest preview"):
                    st.code(json.dumps(manifest, indent=2))
                st.info(
                    "If you chose a **Native** output, the SQL appears in the Datasphere SQL editor after import. Remember to **deploy** after import."
                )

    # ---------------------- TABLES-ONLY MODE ----------------------
    elif generation_mode.startswith("Create Local Table"):
        with col_left:
            with st.expander("🗄️ Table Schemas (required for table creation)", expanded=True):
                st.caption(
                    "Upload **DDL .sql**, **CSV header**, or **JSON schema** to create Local Table entities."
                )
                schemas_files = st.file_uploader(
                    "Upload table schemas",
                    type=["sql", "csv", "json"],
                    accept_multiple_files=True,
                    key="schema_files",
                )
                table_schemas = parse_uploaded_schemas(schemas_files)
                if table_schemas:
                    for t in sorted(table_schemas.keys()):
                        st.markdown(f"- ✅ `{t}`", unsafe_allow_html=True)
                else:
                    st.markdown("No schemas uploaded yet.", unsafe_allow_html=True)

        with col_right:
            st.info("View-related upload options are hidden because you selected **Tables-only** mode.")

        with st.expander("🏷️ Package name", expanded=False):
            package_name = st.text_input(
                "Name to embed in csn.json",
                value=f"ds_package_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                key="pkg_name_tables",
            )

        with col_right:
            qa_tables = st.button(
                "⚡ Tables-only ZIP",
                key="qa_tables_only_zip",
                disabled=not bool(table_schemas),
            )

        if qa_tables:
            try:
                zip_bytes, manifest = build_csn_artifacts_zip(
                    package_name=package_name,
                    cv_model=None,
                    sql_views=[],
                    procedures=[],
                    graph=None,
                    table_mode='tables_only',
                    view_mode='sql',
                    include_analytic=False,
                    native_template_bytes=None,
                    native_single_file=False,
                    table_schemas=table_schemas,
                    native_output_mode="neutral",
                )
                st.success("✅ Tables-only package generated.")
                st.download_button(
                    label="⬇️ Download Export Package (ZIP)",
                    data=zip_bytes,
                    file_name=f"{package_name}.zip",
                    mime="application/zip",
                    key="dl_tables_zip",
                )
                with st.expander("🧾 Manifest preview"):
                    st.code(json.dumps(manifest, indent=2))
                st.info(
                    "This package contains **table entities only**. Import **csn.json** and deploy tables before creating views."
                )
            except Exception as e:
                st.error(f"Failed to build packages: {e}")

    # ---------------------- REPLICATION FLOW (ABAP CDS) ----------------------
    else:
        # LEFT: Upload ABAP CDS and Replication Flow template
        with col_left:
            with st.expander("📤 Upload ABAP CDS and Replication Flow template", expanded=True):
                uploaded_export = st.file_uploader(
                    "Upload ABAP CDS file (.cds / .txt)",
                    type=["cds", "txt"],
                    key="uploader_export_cds",
                )
                native_template = st.file_uploader(
                    "Upload Replication Flow JSON (exported from Datasphere)",
                    type=["json"],
                    key="rf_template",
                    accept_multiple_files=False,
                )

            if uploaded_export:
                txt = uploaded_export.read().decode("utf-8", errors="ignore")
                abap_cds_e = parse_abap_cds_text(txt)
                with st.expander("📘 ABAP CDS summary", expanded=True):
                    st.write(f"**Name:** `{abap_cds_e.name}`")
                    if abap_cds_e.sql_view_name:
                        st.write(f"**SQL View:** `{abap_cds_e.sql_view_name}`")
                    st.write(f"**Extraction enabled:** `{abap_cds_e.extraction_enabled}`")
                    if abap_cds_e.cdc_annotation:
                        st.write(f"**CDC:** `{abap_cds_e.cdc_annotation}`")
                    st.write(f"**Parameters:** {len(abap_cds_e.parameters)}")
                    if abap_cds_e.sources:
                        st.code("Sources: " + ", ".join(abap_cds_e.sources))

            # Sanity validator
            ok_extract = bool(abap_cds_e and abap_cds_e.extraction_enabled)
            ok_params = bool(abap_cds_e and not abap_cds_e.parameters)
            with st.expander("🔎 Validation (sanity checks)", expanded=True):
                st.write("• Extraction enabled:", "✅" if ok_extract else "❌")
                st.write("• No input parameters:", "✅" if ok_params else "❌")
                if not ok_extract:
                    st.warning("Replication Flow requires `@Analytics.dataExtraction.enabled: true` on the CDS.")
                if not ok_params:
                    st.warning("Replication Flows do **not** support input parameters.")

        # RIGHT: Options + Generate
        with col_right:
            st.markdown("#### ⚙️ Options")
            rf_load_type = st.selectbox(
                "Load Type",
                ["INITIAL_ONLY", "INITIAL_AND_DELTA", "DELTA_ONLY"],
                index=1,
                key="rf_load_type",
            )
            rf_content_type = st.selectbox(
                "Content Type (ABAP sources)",
                ["Native", "Template", "Unspecified"],
                index=0,
                key="rf_content_type",
                help="For ABAP-based sources (Datasphere wave 2025.04+), choose how date/timestamp types are applied to target. If unsure, keep Native.",
            )
            target_table = st.text_input(
                "Target local table name",
                value=(f"Z_{abap_cds_e.name}" if abap_cds_e else "Z_TARGET"),
                key="rf_target_table",
            )

        with st.expander("🏷️ Package name", expanded=False):
            package_name = st.text_input(
                "Name to embed in csn.json",
                value=f"ds_package_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                key="pkg_name_rf",
            )

        gen_rf = st.button(
            "🚀 Generate CSN/JSON",
            type="secondary",
            key="gen_rf",
            disabled=not (abap_cds_e and native_template and ok_extract and ok_params),
        )

        if gen_rf:
            try:
                nb = native_template.read() if native_template else None
                zip_bytes, manifest = build_csn_artifacts_zip(
                    package_name=package_name,
                    cv_model=None,
                    sql_views=[],
                    procedures=[],
                    graph=None,
                    table_mode='view_only',
                    view_mode='sql',
                    include_analytic=False,
                    native_template_bytes=nb,
                    table_schemas=None,
                    native_output_mode="native",  # Replication Flows are native
                    abap_cds=abap_cds_e,
                    rf_load_type=rf_load_type,
                    rf_content_type=(None if rf_content_type == "Unspecified" else rf_content_type),
                    rf_target_table=target_table,
                )
                st.success("✅ Replication Flow package generated.")
                st.download_button(
                    label="⬇️ Download Export Package (ZIP)",
                    data=zip_bytes,
                    file_name=f"{package_name}.zip",
                    mime="application/zip",
                    key="dl_rf_zip",
                )
                with st.expander("🧾 Manifest preview"):
                    st.code(json.dumps(manifest, indent=2))
                st.info("After import, set source/target connections if prompted, **deploy**, and then **run**.")
            except Exception as e:
                st.error(f"Failed to build packages: {e}")