# streamlit_app.py  — one-click "Generate & Download DOCX", no timestamp button

import os
import re
import tempfile
from datetime import datetime
from uuid import uuid4

import streamlit as st

# ------------------------
# Page Configuration / UI
# ------------------------
st.set_page_config(
    page_title="SAP Datasphere Program Accelerator",
    page_icon="🟦",
    layout="wide",
)

# Paths
LOGO_PATH = os.path.join(os.path.dirname(__file__), "assets", "logo.png")

# ------------------------
# Import your internal modules
# ------------------------
from hdbcv2dsp.parse_cv import parse_hdbcalculationview, topo_order
from hdbcv2dsp.parse_sql_view import parse_hdbview_or_sql, SQLViewModel
from hdbcv2dsp.parse_procedure import parse_hdbprocedure_or_sql, ProcedureModel
from hdbcv2dsp.unify import (
    graph_from_cv,
    graph_from_sql_views,
    graph_from_procedures,
    merge_graphs,
)
from hdbcv2dsp.render_docx_general import render_docx_general

# ------------------------
# Helpers / Session Defaults
# ------------------------
def sanitize_filename(name: str) -> str:
    """Remove Windows-illegal characters and ensure .docx extension."""
    name = (name or "").strip()
    if not name:
        name = "Rebuild_Guide.docx"
    # Remove invalid characters: \ / : * ? " < >
    name = re.sub(r'[\\/:*?"<>]+', "", name)
    # Disallow leading/trailing dots and spaces
    name = name.strip(" .")
    if not name.lower().endswith(".docx"):
        name += ".docx"
    return name or "Rebuild_Guide.docx"

# Initialize session-state defaults once (timestamp filename on app open)
if "out_name" not in st.session_state:
    st.session_state.out_name = f"Rebuild_Guide_{datetime.now().strftime('%Y%m%d_%H%M%S')}.docx"
if "doc_title" not in st.session_state:
    st.session_state.doc_title = ""

# ------------------------
# Header Section with Logo + Title
# ------------------------
col_logo, col_title = st.columns([1, 6], vertical_alignment="center")
with col_logo:
    if os.path.exists(LOGO_PATH):
        st.image(LOGO_PATH, use_container_width=True)
with col_title:
    st.markdown(
        """
## SAP Datasphere Program Accelerator

Upload HANA Calculation Views, SQL Views, or Stored Procedures → auto-generate a Datasphere rebuild guide
""",
        unsafe_allow_html=True,
    )

# ------------------------
# Upload Section
# ------------------------
st.markdown("### Upload")
st.divider()
st.caption("Upload a Calculation View (.hdbcalculationview/.xml), SQL View (.hdbview/.sql), or Stored Procedure (.hdbprocedure/.sql)")

uploaded = st.file_uploader(
    "Upload artifact",
    type=["hdbcalculationview", "xml", "hdbview", "hdbprocedure", "sql"],
)

# ------------------------
# Document Settings
# ------------------------
st.markdown("### Document Settings")
st.divider()

st.session_state.doc_title = st.text_input(
    "Document Title (optional)",
    value=st.session_state.doc_title,
    key="doc_title_input",
)

st.session_state.out_name = st.text_input("Output filename", value=st.session_state.out_name)

# ------------------------
# Main Logic
# ------------------------
cv_model = None
sql_views: list[SQLViewModel] = []
procedures: list[ProcedureModel] = []
graph = {}

if uploaded:
    try:
        # Save uploaded file to temp
        tmp_dir = tempfile.gettempdir()
        safe_name = os.path.basename(uploaded.name) or "uploaded.sql"
        tmp_path = os.path.join(tmp_dir, safe_name)
        with open(tmp_path, "wb") as f:
            f.write(uploaded.read())

        # Preview Section
        st.markdown("### Preview")
        st.divider()

        # Routing: determine artifact type
        ext = os.path.splitext(safe_name)[1].lower()

        if ext in [".hdbcalculationview", ".xml"]:
            # Parse Calculation View XML
            cv_model = parse_hdbcalculationview(tmp_path)
            st.write(f"**Calculation View ID:** `{cv_model.cv_id}`")
            if cv_model.description:
                st.write(f"**Description:** {cv_model.description}")
            st.write(
                f"**Output View Type:** `{cv_model.output_view_type}` • "
                f"**Data Category:** `{cv_model.data_category}`"
            )
            if cv_model.parameters:
                st.write("**Parameters:**")
                for p in cv_model.parameters:
                    st.code(f"{p['id']} ({p['sqlType']}), default={p['defaultValue']}, mandatory={p['isMandatory']}")
            if cv_model.data_sources:
                st.write("**Data Sources:**")
                for ds_id, uri in cv_model.data_sources.items():
                    st.code(f"{ds_id} -> {uri}")
            st.write("**Nodes (topological order):**")
            order = topo_order(cv_model)
            for idx, nid in enumerate(order, start=1):
                n = cv_model.nodes[nid]
                st.write(f"{idx}. `{n.node_id}` — {n.node_type}")

        elif ext in [".hdbview", ".sql", ".hdbprocedure"]:
            # Read and normalize text for robust detection
            with open(tmp_path, "r", encoding="utf-8", errors="ignore") as fh:
                text = fh.read()
            text_norm = re.sub(r'&gt;', '>', text, flags=re.IGNORECASE).strip()

            PROC_DETECT = re.compile(r'\b(?:CREATE|ALTER)\s+(?:PROCEDURE|PROC)\b', re.IGNORECASE)
            VIEW_DETECT = re.compile(r'\bCREATE\s+(?:OR\s+REPLACE\s+)?VIEW\b', re.IGNORECASE)

            is_proc = (ext == ".hdbprocedure") or bool(PROC_DETECT.search(text_norm))
            is_view = (ext == ".hdbview") or bool(VIEW_DETECT.search(text_norm))

            if is_proc:
                proc = parse_hdbprocedure_or_sql(tmp_path)
                procedures.append(proc)
                st.write("**Stored Procedure**")
                st.code(
                    "Name: {}\nParameters: {}\nReads: {}\nWrites: {}\nTemp tables: {}\nCalls: {}".format(
                        proc.name,
                        [f"{x['mode']} {x['name']} {x['type']}" for x in proc.parameters],
                        proc.reads_from,
                        proc.writes_to,
                        getattr(proc, "temp_tables", []),
                        proc.calls,
                    )
                )
            elif is_view:
                view = parse_hdbview_or_sql(tmp_path)
                sql_views.append(view)
                st.write("**SQL View**")
                cols_preview = ", ".join(view.columns[:10]) + (" ..." if len(view.columns) > 10 else "")
                st.code(f"Name: {view.name}\nInputs: {view.inputs}\nColumns: {cols_preview}")
            else:
                st.warning("Unrecognized SQL content. Expecting CREATE/ALTER VIEW or CREATE/ALTER PROCEDURE/PROC.")

        # Build unified dependency graph
        if cv_model:
            graph = merge_graphs({}, graph_from_cv(cv_model))
        if sql_views:
            graph = merge_graphs(graph, graph_from_sql_views(sql_views))
        if procedures:
            graph = merge_graphs(graph, graph_from_procedures(procedures))

        # ------------------------
        # ONE-CLICK: Generate & Download
        # ------------------------
        st.markdown("### Export")
        st.divider()

        if not (cv_model or sql_views or procedures):
            st.info("Upload at least one artifact to enable DOCX export.")
        else:
            final_name = sanitize_filename(st.session_state.out_name)

            # Build the DOCX to a temp file and stream it immediately
            tmp_docx_path = os.path.join(
                tempfile.gettempdir(),
                f"rebuild_{datetime.now().strftime('%Y%m%d_%H%M%S%f')}.docx"
            )
            render_docx_general(
                output_path=tmp_docx_path,
                title=st.session_state.doc_title or None,
                cv_model=cv_model,
                sql_views=sql_views,
                procedures=procedures,
                graph=graph if graph else None,
            )
            with open(tmp_docx_path, "rb") as f:
                data = f.read()
            try:
                os.remove(tmp_docx_path)
            except Exception:
                pass

            st.download_button(
                label="⬇️ Generate & Download DOCX",
                data=data,
                file_name=final_name,
                mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                key=f"dl-{uuid4()}",
            )

    except Exception as e:
        st.error(f"Failed to parse/generate: {e}")
else:
    st.info("Upload a Calculation View (.hdbcalculationview/.xml), SQL View (.hdbview/.sql), or Procedure (.hdbprocedure/.sql) to begin.")

# ------------------------
# Footer (optional)
# ------------------------
st.markdown(
    f"""
© {datetime.now().year} — SAP Datasphere Program Accelerator
""",
    unsafe_allow_html=True,
)