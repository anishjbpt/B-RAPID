# render_docx_general.py  (drop-in replacement)

from __future__ import annotations
from typing import Dict, List, Optional
import re

from docx import Document
from docx.shared import Pt
from docx.enum.text import WD_ALIGN_PARAGRAPH

from .parse_cv import CVModel, topo_order as cv_topo_order
from .parse_sql_view import SQLViewModel
from .parse_procedure import ProcedureModel
from .artifacts import ArtifactNode, topo_order_nodes
from .summarize import summarize_cv, summarize_sql_view, summarize_procedure  # NEW

def _title(doc: Document, text: str):
    p = doc.add_paragraph()
    r = p.add_run(text)
    r.bold = True
    r.font.size = Pt(20)
    p.alignment = WD_ALIGN_PARAGRAPH.LEFT

def _heading(doc: Document, text: str, level: int = 1):
    doc.add_heading(text, level=level)

def _bullet(doc: Document, text: str):
    doc.add_paragraph(text, style="List Bullet")

# --- NEW: step-by-step builder for SQL Views ---
def _step_by_step_sql_view(doc: Document, v: SQLViewModel):
    _heading(doc, "Step-by-step modeling in Datasphere", 2)

    # 1. Sources
    if v.inputs:
        _bullet(doc, "Prepare source objects as Remote/Replicated Tables: " + ", ".join(v.inputs))
    else:
        _bullet(doc, "Prepare source objects as Remote/Replicated Tables (based on upstream sources).")

    # 2. Create the view
    _bullet(doc, "Create a SQL View and port the SELECT (projections, joins, filters, CASE/expressions).")

    # 3. Aggregation semantics if present
    has_groupby = bool(re.search(r'\bGROUP\s+BY\b', v.sql, flags=re.IGNORECASE))
    has_aggs = bool(re.search(r'\b(SUM|COUNT|AVG|MIN|MAX)\s*\(', v.sql, flags=re.IGNORECASE))
    if has_groupby or has_aggs:
        _bullet(doc, "Because this view aggregates, verify keys and aggregation behavior; "
                     "consider a Graphical View with an Aggregation node for clear semantics.")

    # 4. Keys / semantics
    _bullet(doc, "Validate column data types; define keys where applicable; mark measures/attributes in the consuming layer.")

    # 5. Parameters / variables (placeholder—enable if you later add parameter detection)
    _bullet(doc, "If parameters/variables are required, implement them as Variables/Filters.")

    # 6. Testing
    _bullet(doc, "Validate results: compare row counts and sample business keys for a known time slice.")

def render_docx_general(
    output_path: str,
    title: Optional[str],
    cv_model: Optional[CVModel] = None,
    sql_views: Optional[List[SQLViewModel]] = None,
    procedures: Optional[List[ProcedureModel]] = None,
    graph: Optional[Dict[str, ArtifactNode]] = None
):
    """
    Renders a mixed-artifact DOCX guide with a consistent structure across:
      - HANA Calculation Views
      - SQL Views
      - Stored Procedures
    """
    sql_views = sql_views or []
    procedures = procedures or []

    doc = Document()
    _title(doc, title or "Rebuild Guide — SAP HANA Artifacts → SAP Datasphere")

    # Context
    _heading(doc, "Context", 1)
    doc.add_paragraph(
        "This guide consolidates design-time artifacts (HANA Calculation Views, SQL Views, and Stored Procedures) "
        "and outlines practical steps to rebuild the logic in SAP Datasphere / Business Data Cloud."
    )

    # ------------------------------
    # Calculation View section
    # ------------------------------
    if cv_model:
        _heading(doc, f"Calculation View: {cv_model.cv_id}", 1)

        # Understanding (plain-English)
        _heading(doc, "Understanding (plain-English)", 2)
        for s in summarize_cv(cv_model):
            _bullet(doc, s)

        # Quick facts
        if cv_model.description:
            _bullet(doc, f"Description: {cv_model.description}")
        _bullet(doc, f"Output View Type: {cv_model.output_view_type} • Data Category: {cv_model.data_category}")

        if cv_model.parameters:
            _heading(doc, "Parameters (define in Datasphere)", 2)
            for p in cv_model.parameters:
                _bullet(doc, f"{p['id']} ({p['sqlType']}), default '{p['defaultValue']}', mandatory: {p['isMandatory']}")

        if cv_model.data_sources:
            _heading(doc, "Source objects (prepare as Remote/Replicated Tables)", 2)
            for ds_id, uri in cv_model.data_sources.items():
                _bullet(doc, f"{ds_id} -> {uri}")

        # Consistent heading name
        _heading(doc, "Step-by-step modeling in Datasphere", 2)
        for i, nid in enumerate(cv_topo_order(cv_model), 1):
            n = cv_model.nodes[nid]
            _heading(doc, f"{i}. Create view for node '{n.node_id}' ({n.node_type})", 3)
            if n.node_type == "ProjectionView":
                _bullet(doc, "Create a Graphical View")
                if n.inputs:
                    _bullet(doc, f"Use source: {n.inputs[0]}")
                if n.mappings:
                    cols = sorted({m.target for m in n.mappings if m.target})
                    if cols:
                        _bullet(doc, "Select columns: " + ", ".join(cols))
                for flt in n.filters:
                    _bullet(doc, f"Add filter: {flt}")
            elif n.node_type == "JoinView":
                _bullet(doc, "Create a Graphical View with a Join node")
                if n.join_type:
                    _bullet(doc, f"Join type: {n.join_type}")
                if n.inputs:
                    _bullet(doc, "Inputs: " + ", ".join(n.inputs))
                if n.join_condition:
                    _bullet(doc, f"Join condition: {n.join_condition}")
            elif n.node_type == "AggregationView":
                _bullet(doc, "Create a Graphical View with an Aggregation node")
                if n.attributes:
                    _bullet(doc, "Group by attributes: " + ", ".join(n.attributes))
                if n.measures:
                    _bullet(doc, "Define measures: " + ", ".join(n.measures))
                if n.calculated_measures:
                    _bullet(doc, "Add calculated measures:")
                    for mid, fml in n.calculated_measures.items():
                        _bullet(doc, f" - {mid} = {fml}")
                if n.mappings:
                    _bullet(doc, "Map inputs to output columns:")
                    for mp in n.mappings:
                        _bullet(doc, f" - {mp.source} -> {mp.target}")
            elif n.node_type == "UnionView":
                _bullet(doc, "Create a Graphical View with a Union node")
                if n.inputs:
                    _bullet(doc, "Union inputs: " + ", ".join(n.inputs))
                _bullet(doc, "Align columns across inputs by name and type")
            else:
                _bullet(doc, "Node type not fully handled — add manual steps here.")

        # Semantics
        _heading(doc, "Business Semantics (Datasphere)", 2)
        if cv_model.logical_attributes:
            _bullet(doc, "Mark as Attributes: " + ", ".join(cv_model.logical_attributes))
        if cv_model.logical_measures:
            _bullet(doc, "Mark as Measures: " + ", ".join(cv_model.logical_measures))

    # ------------------------------
    # SQL Views section
    # ------------------------------
    if sql_views:
        _heading(doc, "SQL Views", 1)
        for v in sql_views:
            _heading(doc, f"SQL View: {v.name}", 2)
            # Understanding (plain-English)
            _heading(doc, "Understanding (plain-English)", 3)
            for s in summarize_sql_view(v):
                _bullet(doc, s)

            # Quick facts
            if v.inputs:
                _bullet(doc, "Upstream sources: " + ", ".join(v.inputs))
            if v.columns:
                preview = ", ".join(v.columns[:10]) + (" ..." if len(v.columns) > 10 else "")
                _bullet(doc, "Output columns (preview): " + preview)

            # NEW: Consistent step-by-step
            _step_by_step_sql_view(doc, v)

    # ------------------------------
    # Procedures section
    # ------------------------------
    if procedures:
        _heading(doc, "Stored Procedures", 1)
        for p in procedures:
            _heading(doc, f"Procedure: {p.name}", 2)
            # Understanding (plain-English)
            _heading(doc, "Understanding (plain-English)", 3)
            for s in summarize_procedure(p):
                _bullet(doc, s)

            # Quick facts
            if p.parameters:
                _bullet(doc, "Parameters: " + ", ".join([f"{x['mode']} {x['name']} {x['type']}" for x in p.parameters]))
            if p.reads_from:
                _bullet(doc, "Reads from: " + ", ".join(p.reads_from))
            if p.writes_to:
                _bullet(doc, "Writes to (permanent): " + ", ".join(p.writes_to))
            if hasattr(p, "temp_tables") and p.temp_tables:
                _bullet(doc, "Temp tables used: " + ", ".join(p.temp_tables))

            # RENAMED: use the same heading for consistency
            _heading(doc, "Step-by-step modeling in Datasphere", 3)

            # Parameters / Variables
            _bullet(doc, "Define required inputs:")
            if p.parameters:
                for prm in p.parameters:
                    _bullet(doc, f" - {prm['name']} ({prm['type']}) → Implement as a Datasphere Variable (or fixed constant in a Data Flow).")
            else:
                _bullet(doc, " - No parameters detected; proceed with fixed constants or filters.")

            # Staging for temp tables (CTAS/SELECT INTO)
            if hasattr(p, "temp_tables") and p.temp_tables:
                _bullet(doc, "Create staging logic for temp tables (choose one):")
                _bullet(doc, " - Option 1: SQL Views that materialize the same SELECT as each temp table.")
                _bullet(doc, " - Option 2: Data Flow(s) to persist results into a staging table for reuse/performance.")
                for t in p.temp_tables:
                    _bullet(doc, f"   • Recreate logic for {t}: (document the source, joins, filters, and grouping)")

            # Final aggregation / buckets if applicable
            _bullet(doc, "Create final SQL View(s) for aggregation/bucketing (if the procedure ends with a SELECT).")
            _bullet(doc, "Join staging tables and apply CASE/filtered SUM/aggregations as required.")
            _bullet(doc, "Return business attributes and measures with clear naming and types.")

            # Security / Filters
            _bullet(doc, "Security & filters: implement access constraints using Datasphere roles or view filters.")

            # Performance
            _bullet(doc, "Performance: for large volumes, prefer Data Flows to precompute heavy staging, and index them if applicable.")
            _bullet(doc, "Validate row counts and sample business keys against the original procedure output.")

            # Generic guidance footer
            _bullet(doc, "Implementation choice: SQL Procedure (if available) or refactor into SQL Views + Data Flows as above.")

    # ------------------------------
    # Combined dependency order (optional)
    # ------------------------------
    if graph:
        _heading(doc, "Combined Dependency Order (best effort)", 1)
        order = topo_order_nodes(graph)
        for i, node_id in enumerate(order, 1):
            node = graph[node_id]
            _bullet(doc, f"{i}. {node_id} ({node.kind})")

    # Validation & Publish
    _heading(doc, "Validation", 1)
    _bullet(doc, "Compare row counts against the source artifacts for a known time slice.")
    _bullet(doc, "Spot-check joins and calculations using representative business keys.")

    _heading(doc, "Publish as a BDC Data Product", 1)
    _bullet(doc, "Package the final Datasphere view into a Data Product with owners/tags/description.")

    # Save
    doc.save(output_path)