import os
import sys
import tempfile
import re
from datetime import datetime

import streamlit as st

# --------------------------------------------------------------------------------------
# Make sure Python can find ../src (so imports work without setting PYTHONPATH manually)
# --------------------------------------------------------------------------------------
#sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

# ---- Import your internal modules ----
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


# =========================
# Page Configuration / UI
# =========================
st.set_page_config(
    page_title="SAP Datasphere Program Accelerator",
    page_icon="🟦",
    layout="wide",
)

# Paths
LOGO_PATH = os.path.join(os.path.dirname(__file__), "assets", "logo.png")

# -------------------------
# Global CSS Styling
# -------------------------
st.markdown(
    """
    <style>
    /* Background gradient */
    .stApp {
        background: radial-gradient(1000px 400px at 5% 5%, #E8F1FB 0%, #F7FAFC 40%, #FFFFFF 100%) !important;
    }

    /* App title & subtitle */
    .app-title {
        font-size: 30px;
        font-weight: 800;
        color: #0A6ED1; /* SAP blue */
        margin-bottom: 2px;
        padding-top: 10px;
    }
    .app-subtitle {
        font-size: 15px;
        color: #4B5563;
        margin-top: 0px;
        margin-bottom: 25px;
    }

    /* Card-like containers */
    .card {
        background: #FFFFFF;
        border: 1px solid #E5E7EB;
        border-radius: 12px;
        padding: 20px;
        margin-bottom: 18px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }

    /* Buttons */
    div.stButton > button[kind="primary"] {
        background-color: #0A6ED1;
        color: #FFFFFF;
        border-radius: 8px;
        padding: 0.6rem 1.0rem;
        border: 0;
        font-weight: 600;
    }
    div.stButton > button[kind="primary"]:hover {
        background-color: #085AAA;
    }
    div.stButton > button {
        border-radius: 8px;
    }

    /* Inputs */
    .stTextInput > div > div > input {
        border-radius: 8px !important;
    }
    .stFileUploader > div > div {
        border-radius: 12px !important;
        border: 1px dashed #CBD5E1 !important;
        background: #FFFFFF !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# -------------------------
# Header Section with Logo + Title
# -------------------------
col_logo, col_title = st.columns([1, 6], vertical_alignment="center")
with col_logo:
    if os.path.exists(LOGO_PATH):
        st.image(LOGO_PATH, use_container_width=True)
with col_title:
    st.markdown(
        """
        <div class="app-title">SAP Datasphere Program Accelerator</div>
        <div class="app-subtitle">Upload HANA Calculation Views, SQL Views, or Stored Procedures → auto-generate a Datasphere rebuild guide</div>
        """,
        unsafe_allow_html=True,
    )


# =========================
# Helpers / Session Defaults
# =========================
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


# Initialize session-state defaults once
if "out_name" not in st.session_state:
    st.session_state.out_name = f"Rebuild_Guide_{datetime.now().strftime('%Y%m%d_%H%M%S')}.docx"
if "doc_title" not in st.session_state:
    st.session_state.doc_title = ""


# =========================
# Upload Section (Card)
# =========================
st.markdown('<div class="card">', unsafe_allow_html=True)
st.caption(
    "Upload a Calculation View (.hdbcalculationview/.xml), SQL View (.hdbview/.sql), "
    "or Stored Procedure (.hdbprocedure/.sql)"
)
uploaded = st.file_uploader(
    "Upload artifact",
    type=["hdbcalculationview", "xml", "hdbview", "hdbprocedure", "sql"],
)
st.markdown("</div>", unsafe_allow_html=True)


# =========================
# Document Settings (Card)
# =========================
st.markdown('<div class="card">', unsafe_allow_html=True)
st.write("### Document Settings")
st.session_state.doc_title = st.text_input("Document Title (optional)", value=st.session_state.doc_title)
st.session_state.out_name = st.text_input("Output filename", value=st.session_state.out_name)

colA, colB = st.columns(2)
with colA:
    if st.button("Use timestamp filename"):
        st.session_state.out_name = f"Rebuild_Guide_{datetime.now().strftime('%Y%m%d_%H%M%S')}.docx"
with colB:
    generate_btn = st.button("Generate DOCX", type="primary")

st.markdown("</div>", unsafe_allow_html=True)


# =========================
# Main Logic
# =========================
if uploaded:
    try:
        # Save uploaded file to temp
        tmp_dir = tempfile.gettempdir()
        safe_name = os.path.basename(uploaded.name) or "uploaded.sql"
        tmp_path = os.path.join(tmp_dir, safe_name)
        with open(tmp_path, "wb") as f:
            f.write(uploaded.read())

        # Preview card
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.subheader("Preview")

        # Routing: determine artifact type
        ext = os.path.splitext(safe_name)[1].lower()

        cv_model = None
        sql_views: list[SQLViewModel] = []
        procedures: list[ProcedureModel] = []

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
                    st.code(
                        f"{p['id']} ({p['sqlType']}), default={p['defaultValue']}, mandatory={p['isMandatory']}"
                    )
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
            # Peek into SQL file content to decide (view vs procedure)
            with open(tmp_path, "r", encoding="utf-8", errors="ignore") as fh:
                text = fh.read()

            is_proc = (
                ext == ".hdbprocedure"
                or "CREATE PROCEDURE" in text.upper()
                or "ALTER PROCEDURE" in text.upper()
                or "ALTER PROC" in text.upper()
            )
            is_view = (ext == ".hdbview") or ("CREATE VIEW" in text.upper())

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

        st.markdown("</div>", unsafe_allow_html=True)  # end Preview card

        # Build unified dependency graph (for the combined order section in DOCX)
        graph = {}
        if cv_model:
            graph = merge_graphs(graph, graph_from_cv(cv_model))
        if sql_views:
            graph = merge_graphs(graph, graph_from_sql_views(sql_views))
        if procedures:
            graph = merge_graphs(graph, graph_from_procedures(procedures))

        # Generate DOCX
        if generate_btn:
            final_name = sanitize_filename(st.session_state.out_name)
            st.session_state.out_name = final_name
            tmp_docx_path = os.path.join(tmp_dir, final_name)

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

            st.markdown('<div class="card">', unsafe_allow_html=True)
            st.success(f"DOCX generated successfully as **{final_name}**.")
            st.download_button(
                "⬇️ Download Rebuild Guide (.docx)",
                data=data,
                file_name=final_name,
                mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            )
            st.markdown("</div>", unsafe_allow_html=True)

    except Exception as e:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.error(f"Failed to parse/generate: {e}")
        st.markdown("</div>", unsafe_allow_html=True)

else:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.info(
        "Upload a Calculation View (.hdbcalculationview/.xml), SQL View (.hdbview/.sql), "
        "or Procedure (.hdbprocedure/.sql) to begin."
    )
    st.markdown("</div>", unsafe_allow_html=True)


# =========================
# Footer (optional)
# =========================
st.markdown(
    f"""
    <div style='text-align:center; color:#6B7280; font-size:12px; padding:12px 0 0 0;'>
        © {datetime.now().year} — SAP Datasphere Program Accelerator
    </div>
    """,
    unsafe_allow_html=True,
)