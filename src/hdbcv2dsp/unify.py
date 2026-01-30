from typing import Dict, List
from .artifacts import ArtifactNode
from .parse_cv import CVModel
from .parse_sql_view import SQLViewModel
from .parse_procedure import ProcedureModel

def graph_from_cv(model: CVModel) -> Dict[str, ArtifactNode]:
    g: Dict[str, ArtifactNode] = {}
    for nid, n in model.nodes.items():
        g[nid] = ArtifactNode(id=nid, kind="CV", inputs=list(n.inputs))
    # Optional: include data sources as standalone 'Table' nodes
    for ds_id in model.data_sources.keys():
        g.setdefault(ds_id, ArtifactNode(id=ds_id, kind="Table", inputs=[]))
    return g

def graph_from_sql_views(views: List[SQLViewModel]) -> Dict[str, ArtifactNode]:
    g: Dict[str, ArtifactNode] = {}
    for v in views:
        g[v.name] = ArtifactNode(id=v.name, kind="SQLView", inputs=list(v.inputs))
        for src in v.inputs:
            g.setdefault(src, ArtifactNode(id=src, kind="Table", inputs=[]))
    return g

def graph_from_procedures(procs: List[ProcedureModel]) -> Dict[str, ArtifactNode]:
    g: Dict[str, ArtifactNode] = {}
    for p in procs:
        deps = sorted(set(p.reads_from + p.calls))
        g[p.name] = ArtifactNode(id=p.name, kind="Procedure", inputs=deps)
        for t in p.reads_from + p.writes_to:
            g.setdefault(t, ArtifactNode(id=t, kind="Table", inputs=[]))
    return g

def merge_graphs(*graphs: Dict[str, ArtifactNode]) -> Dict[str, ArtifactNode]:
    merged: Dict[str, ArtifactNode] = {}
    for g in graphs:
        for k, v in g.items():
            if k not in merged:
                merged[k] = v
            else:
                merged[k].inputs = sorted(set(merged[k].inputs + v.inputs))
    return merged