from dataclasses import dataclass, field
from typing import Dict, List

@dataclass
class ArtifactNode:
    id: str                     # object name (CV node / view / table / procedure)
    kind: str                   # 'CV' | 'SQLView' | 'Procedure' | 'Table'
    inputs: List[str] = field(default_factory=list)

def topo_order_nodes(nodes: Dict[str, ArtifactNode]) -> List[str]:
    """Topologically order a mixed set of artifacts (best-effort with safety net)."""
    from collections import defaultdict, deque

    indeg = {nid: 0 for nid in nodes}
    adj = defaultdict(list)

    for nid, node in nodes.items():
        for dep in node.inputs:
            if dep in nodes:
                adj[dep].append(nid)
                indeg[nid] += 1

    q = deque([nid for nid, d in indeg.items() if d == 0])
    order: List[str] = []

    while q:
        u = q.popleft()
        order.append(u)
        for v in adj[u]:
            indeg[v] -= 1
            if indeg[v] == 0:
                q.append(v)

    # Safety net—append any isolated/cyclic nodes to the end
    for nid in nodes:
        if nid not in order:
            order.append(nid)

    return order