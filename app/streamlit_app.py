import os
import sys
import tempfile
import re
from datetime import datetime
import streamlit as st
import streamlit.components.v1 as components
from uuid import uuid4

# --------------------------------------------
# Page Configuration / UI
# --------------------------------------------
st.set_page_config(
    page_title="SAP Datasphere Program Accelerator",
    page_icon="🟦",
    layout="wide",
)

# Paths
LOGO_PATH = os.path.join(os.path.dirname(__file__), "assets", "logo.png")

# --------------------------------------------
# Global CSS Styling (subtle UI, gradient separators, brand border for title)
# --------------------------------------------
st.markdown(
    f"""
    <style>
      :root {{
        --brand: #0A6ED1; /* from config.toml primaryColor */
        --rule-start: rgba(10, 110, 209, .25);  /* light brand */
        --rule-mid:   rgba(0, 0, 0, 0.08);
        --rule-end:   rgba(10, 110, 209, .25);
      }}

      /* General spacing/typography */
      .block-container {{ padding-top: 1.2rem; }}
      h1, h2, h3 {{ letter-spacing: .2px; }}

      /* Remove big box feel for any card-like wrappers */
      .card {{
        background: transparent !important;
        border: 0 !important;
        box-shadow: none !important;
        padding: 0 !important;
        margin: 0 0 1rem 0 !important;
      }}

      /* Subtle gradient separator using <hr> */
      hr {{
        border: 0;
        height: 1px;
        background: linear-gradient(90deg, var(--rule-start), var(--rule-mid), var(--rule-end));
        margin: .75rem 0 1rem;
      }}

      /* Inputs: gentle rounding */
      div[data-testid="stTextInput"] div[data-baseweb="input"] {{
        border-radius: 8px !important;
      }}

      /* Darker/brand border ONLY for the Document Title input */
      #doc-title-box div[data-baseweb="input"] {{
        border-color: var(--brand) !important;
        box-shadow: inset 0 0 0 1px var(--brand) !important;
        border-radius: 8px !important;
      }}
      #doc-title-box div[data-baseweb="input"]:focus-within {{
        box-shadow: inset 0 0 0 2px var(--brand) !important;
      }}

      /* Fallback link styling */
      .download-fallback a {{
        color: var(--brand);
        text-decoration: none;
        font-weight: 600;
      }}
      .download-fallback a:hover {{ text-decoration: underline; }}
    </style>
    """,
    unsafe_allow_html=True,
)

# --------------------------------------------
# Import your internal modules
# --------------------------------------------
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

# --------------------------------------------
# Helpers / Session Defaults
# --------------------------------------------
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

# --------------------------------------------
# Header Section with Logo + Title
# --------------------------------------------
col_logo, col_title = st.columns([1, 6], vertical_alignment="center")
with col_logo:
    if os.path.exists(LOGO_PATH):
        st.image(LOGO_PATH, use_container_width=True)
with col_title:
    st.markdown(
        """
        <h1> SAP Datasphere Program Accelerator </h1>
        <p style="margin-top: .25rem; color: #475569;">
          Upload HANA Calculation Views, SQL Views, or Stored Procedures → auto-generate a Datasphere rebuild guide
        </p>
        """,
        unsafe_allow_html=True,
    )

# --------------------------------------------
# Upload Section (subtle)
# --------------------------------------------
st.markdown("### Upload")
st.divider()
st.caption(
    "Upload a Calculation View (.hdbcalculationview/.xml), SQL View (.hdbview/.sql), or Stored Procedure (.hdbprocedure/.sql)"
)
uploaded = st.file_uploader(
    "Upload artifact",
    type=["hdbcalculationview", "xml", "hdbview", "hdbprocedure", "sql"],
)

# --------------------------------------------
# Document Settings (subtle, brand title border)
# --------------------------------------------
st.markdown("### Document Settings")
st.divider()

st.markdown('<div id="doc-title-box">', unsafe_allow_html=True)
st.session_state.doc_title = st.text_input(
    "Document Title (optional)",
    value=st.session_state.doc_title,
    key="doc_title_input",
)
st.markdown('</div>', unsafe_allow_html=True)

st.session_state.out_name = st.text_input("Output filename", value=st.session_state.out_name)

colA, colB = st.columns(2)
with colA:
    if st.button("Use timestamp filename"):
        st.session_state.out_name = f"Rebuild_Guide_{datetime.now().strftime('%Y%m%d_%H%M%S')}.docx"
with colB:
    generate_and_download = st.button("Generate DOCX", type="primary")

# --------------------------------------------
# Main Logic
# --------------------------------------------
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

        # Build unified dependency graph
        graph = {}
        if cv_model:
            graph = merge_graphs(graph, graph_from_cv(cv_model))
        if sql_views:
            graph = merge_graphs(graph, graph_from_sql_views(sql_views))
        if procedures:
            graph = merge_graphs(graph, graph_from_procedures(procedures))

        # Generate DOCX + auto-download with fallback link
        if 'generate_and_download' in locals() and generate_and_download:
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

            # Render the download button (visible for manual fallback)
            dl_key = f"dl-{uuid4()}"
            dl_label = "⬇️ Download Rebuild Guide (.docx)"
            st.download_button(
                label=dl_label,
                data=data,
                file_name=final_name,
                mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                key=dl_key,
            )

            # Auto-click (one-click UX)
            components.html(
                f"""
                <script>
                  const tryClick = () => {{
                    const root = window.parent.document;
                    // Prefer the anchor element with download attr
                    const a = root.querySelector('a[download="{final_name}"]');
                    if (a) {{ a.click(); return true; }}
                    // Fallback: click the button with the same label
                    const btns = [...root.querySelectorAll('button')]
                      .filter(b => b.textContent.trim() === '{dl_label}');
                    if (btns[0]) {{ btns[0].click(); return true; }}
                    return false;
                  }};
                  setTimeout(() => {{ if (!tryClick()) setTimeout(tryClick, 300); }}, 150);
                </script>
                """,
                height=0,
            )

            # Explicit fallback link if the browser blocks programmatic click
            st.markdown(
                f"<div class='download-fallback'>If your download didn't start automatically, "
                f"<a href='data:application/vnd.openxmlformats-officedocument.wordprocessingml.document;base64,"
                + __import__('base64').b64encode(data).decode('utf-8') +
                f"' download='{final_name}'>click here to download</a>.</div>",
                unsafe_allow_html=True,
            )

            st.toast(f"Downloading {final_name} …")

    except Exception as e:
        st.error(f"Failed to parse/generate: {e}")
else:
    st.info(
        "Upload a Calculation View (.hdbcalculationview/.xml), SQL View (.hdbview/.sql), or Procedure (.hdbprocedure/.sql) to begin."
    )

# --------------------------------------------
# Footer (optional)
# --------------------------------------------
st.markdown(
    f"""
    <div style='margin-top: 1.25rem; color:#64748b;'>
      © {datetime.now().year} — SAP Datasphere Program Accelerator
    </div>
    """,
    unsafe_allow_html=True,
)
