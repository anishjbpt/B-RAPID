from __future__ import annotations
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from typing import Dict, List, Optional

NS = {
    "Calculation": "http://www.sap.com/ndb/BiModelCalculation.ecore",
    "xsi": "http://www.w3.org/2001/XMLSchema-instance",
}

@dataclass
class Mapping:
    source: str
    target: str

@dataclass
class CVNode:
    node_id: str
    node_type: str  # ProjectionView | JoinView | AggregationView | UnionView
    attributes: List[str] = field(default_factory=list)
    measures: List[str] = field(default_factory=list)
    calculated_measures: Dict[str, str] = field(default_factory=dict)
    calc_columns: Dict[str, str] = field(default_factory=dict)
    filters: List[str] = field(default_factory=list)
    join_type: Optional[str] = None
    join_condition: Optional[str] = None
    inputs: List[str] = field(default_factory=list)  # node or DS ids (no leading '#')
    mappings: List[Mapping] = field(default_factory=list)

@dataclass
class CVModel:
    cv_id: str
    description: str
    output_view_type: str
    data_category: str
    parameters: List[Dict[str, str]] = field(default_factory=list)
    data_sources: Dict[str, str] = field(default_factory=dict)  # id -> resourceUri
    nodes: Dict[str, CVNode] = field(default_factory=dict)
    logical_attributes: List[str] = field(default_factory=list)
    logical_measures: List[str] = field(default_factory=list)

def parse_hdbcalculationview(xml_path: str) -> CVModel:
    tree = ET.parse(xml_path)
    root = tree.getroot()
    cv_id = root.attrib.get("id", "UNKNOWN")
    description = root.attrib.get("description", "")
    output_view_type = root.attrib.get("outputViewType", "")
    data_category = root.attrib.get("dataCategory", "")

    model = CVModel(cv_id, description, output_view_type, data_category)

    # Parameters
    params = root.find("parameters")
    if params is not None:
        for p in params.findall("parameter"):
            model.parameters.append({
                "id": p.attrib.get("id"),
                "sqlType": p.attrib.get("sqlType"),
                "defaultValue": p.attrib.get("defaultValue", ""),
                "isMandatory": p.attrib.get("isMandatory", "false")
            })

    # Data Sources
    ds = root.find("dataSources")
    if ds is not None:
        for d in ds.findall("DataSource"):
            rid = d.attrib["id"]
            uri = d.findtext("resourceUri")
            model.data_sources[rid] = uri

    # Calculation Views (nodes)
    cviews = root.find("calculationViews")
    if cviews is not None:
        for cv in cviews:
            node_type = cv.attrib.get(f"{{{NS['xsi']}}}type", cv.tag.split("}", 1)[-1])
            node_id = cv.attrib.get("id", "")
            node = CVNode(node_id=node_id, node_type=node_type.split(":")[-1])

            for va in cv.findall("viewAttributes"):
                for vattr in va.findall("viewAttribute"):
                    node.attributes.append(vattr.attrib.get("id"))

            meas_parent = cv.find("measures")
            if meas_parent is not None:
                for m in meas_parent.findall("measure"):
                    node.measures.append(m.attrib.get("id"))

            calc_meas = cv.find("calculatedMeasures")
            if calc_meas is not None:
                for cm in calc_meas.findall("calculatedMeasure"):
                    cm_id = cm.attrib.get("id")
                    formula_el = cm.find("formula")
                    node.calculated_measures[cm_id] = (formula_el.text.strip() if formula_el is not None else "")

            filters_el = cv.find("filters")
            if filters_el is not None:
                for flt in filters_el.findall("filter"):
                    if flt.text:
                        node.filters.append(flt.text.strip())

            jt = cv.find("joinType")
            if jt is not None and jt.text:
                node.join_type = jt.text.strip()

            for inp in cv.findall("input"):
                left = inp.attrib.get("left")
                right = inp.attrib.get("right")
                node_ref = inp.attrib.get("node")
                if left:
                    node.inputs.append(left.replace("#", ""))
                if right:
                    node.inputs.append(right.replace("#", ""))
                if node_ref:
                    node.inputs.append(node_ref.replace("#", ""))

                jc = inp.find("joinCondition")
                if jc is not None:
                    expr = jc.find("expression")
                    if expr is not None and expr.text:
                        node.join_condition = expr.text.strip()

                for mp in inp.findall("mapping"):
                    src = mp.attrib.get("source")
                    tgt = mp.attrib.get("target")
                    if src and tgt:
                        node.mappings.append(Mapping(src, tgt))

            model.nodes[node.node_id] = node

    # logical model
    logical = root.find("logicalModel")
    if logical is not None:
        attrs = logical.find("attributes")
        if attrs is not None:
            for a in attrs.findall("attribute"):
                node_id = a.attrib.get("id")
                if node_id:
                    model.logical_attributes.append(node_id)
        meas = logical.find("measures")
        if meas is not None:
            for m in meas.findall("measure"):
                mid = m.attrib.get("id")
                if mid:
                    model.logical_measures.append(mid)

    return model

def topo_order(model: CVModel) -> list[str]:
    from collections import defaultdict, deque
    indeg = {nid: 0 for nid in model.nodes}
    adj = defaultdict(list)

    def is_node(x: str) -> bool:
        return x in model.nodes

    for nid, node in model.nodes.items():
        for inp in node.inputs:
            if is_node(inp):
                adj[inp].append(nid)
                indeg[nid] += 1

    q = deque([nid for nid, d in indeg.items() if d == 0])
    order = []
    while q:
        u = q.popleft()
        order.append(u)
        for v in adj[u]:
            indeg[v] -= 1
            if indeg[v] == 0:
                q.append(v)

    for nid in model.nodes:
        if nid not in order:
            order.append(nid)
    return order