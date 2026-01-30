from __future__ import annotations
from typing import Dict, List, Optional
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


def render_docx_general(
    output_path: str,
    title: Optional[str],
    cv_model: Optional[CVModel] = None,
    sql_views: Optional[List[SQLViewModel]] = None,
    procedures: Optional[List[ProcedureModel]] = None,
    graph: Optional[Dict[str, ArtifactNode]] = None
):
    """
    Renders a mixed-artifact DOCX guide:
      - HANA Calculation Views (CV)
      - SQL Views
      - Stored Procedures
      - Combined dependency order (optional)
      - Adds a plain-English 'Understanding' section per artifact
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

    # --------------------------
    # Calculation View section
    # --------------------------
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

        # Steps
        _heading(doc, "Step-by-step modeling in Datasphere (CV nodes)", 2)
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

    # --------------------------
    # SQL Views section
    # --------------------------
    if sql_views:
        _heading(doc, "SQL Views", 1)
        for v in sql_views:
            _heading(doc, f"SQL View: {v.name}", 2)

            # Understanding (plain-English)
            _heading(doc, "Understanding (plain-English)", 3)
            for s in summarize_sql_view(v):
                _bullet(doc, s)

            # Quick facts + basic guidance
            if v.inputs:
                _bullet(doc, "Upstream sources: " + ", ".join(v.inputs))
            if v.columns:
                preview = ", ".join(v.columns[:10]) + (" ..." if len(v.columns) > 10 else "")
                _bullet(doc, "Output columns (preview): " + preview)
            _bullet(doc, "Datasphere: create a SQL View and port the SELECT (joins/filters/expressions).")
            _bullet(doc, "Validate column data types and key semantics.")

    # --------------------------
    # Procedures section (with Understanding + Rebuild Plan)
    # --------------------------
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

            # Rebuild Plan in Datasphere
            _heading(doc, "Rebuild Plan in Datasphere", 3)

            # Step A: Parameters / Variables
            _bullet(doc, "Define required inputs:")
            if p.parameters:
                for prm in p.parameters:
                    _bullet(doc, f" - {prm['name']} ({prm['type']}) → Implement as a Datasphere Variable (or fixed constant in a Data Flow).")
            else:
                _bullet(doc, " - No parameters detected; proceed with fixed constants or filters.")

            # Step B: Staging for temp tables (CTAS/SELECT INTO)
            if hasattr(p, "temp_tables") and p.temp_tables:
                _bullet(doc, "Create staging logic for temp tables (choose one):")
                _bullet(doc, " - Option 1: Datasphere SQL Views that materialize the same SELECT as each temp table.")
                _bullet(doc, " - Option 2: Datasphere Data Flow(s) to persist results into a staging table for reuse and performance.")
                for t in p.temp_tables:
                    _bullet(doc, f"   • Recreate logic for {t}:")
                    name_l = t.lower()
                    if "outboundquantity" in name_l:
                        _bullet(doc, "     - Source: FactMovement joined with DimProduct; filter outbound rows (e.g., DebitCredit='H'), non-zero docs, up to @ReportDate.")
                        _bullet(doc, "     - Group by CompanyKey, PlantKey, ProductKey, SpecialStock; sum outbound quantity up to @ReportDate.")
                    elif "inventoryaging" in name_l:
                        _bullet(doc, "     - Build aging by posting date: compute Cumulative Stock minus Outbound to get remaining on-hand by day.")
                        _bullet(doc, "     - Enrich with prices/GL (DimMaterialValuation / DimSalesOrderStockValuation) valid on @ReportDate.")
                        _bullet(doc, "     - Join OutboundQuantity staging on Company/Plant/Product/SpecialStock.")

            # Step C: Final aggregation / aging buckets
            _bullet(doc, "Create final SQL View for Inventory Aging buckets:")
            _bullet(doc, " - Join staging tables (e.g., InventoryAging with DimDate/DimProduct/DimPlant/DimCompany).")
            _bullet(doc, " - Implement CASE/filtered SUMs for each bucket using @Days1..@Days10 thresholds (and >= last bucket).")
            _bullet(doc, " - Return quantity and amount buckets, plus attributes (company/plant/product/base UOM/GL/profit center).")

            # Step D: Security / Filters
            _bullet(doc, "Security & filters:")
            _bullet(doc, " - Reimplement user-to-company constraints (e.g., CnfgUserCompanyAccess) using Datasphere roles or view filters.")
            _bullet(doc, " - Replicate optional filters (CompanyNo/ProductType/Plant list).")

            # Step E: Performance notes
            _bullet(doc, "Performance:")
            _bullet(doc, " - For large volumes, prefer Data Flows to precompute staging tables (like #OutboundQuantity/#InventoryAging) and index them.")
            _bullet(doc, " - Validate row counts and sample business keys against the original procedure output.")

            # Generic guidance footer
            _bullet(doc, "Implementation choice: SQL Procedure (if available) or refactor into SQL Views + Data Flows as above.")

    # --------------------------
    # Combined dependency order (optional)
    # --------------------------
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