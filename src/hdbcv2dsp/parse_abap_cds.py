# hdbcv2dsp/parse_abap_cds.py
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional
import re

@dataclass
class ABAPCDSModel:
    name: str
    sql_view_name: Optional[str] = None                 # @AbapCatalog.sqlViewName (classic DEFINE VIEW only)
    extraction_enabled: bool = False                    # @Analytics.dataExtraction.enabled: true
    cdc_annotation: Optional[str] = None                # @Analytics.dataExtraction.delta.changeDataCapture...
    parameters: List[str] = field(default_factory=list) # raw parameter items (if any)
    keys: List[str] = field(default_factory=list)       # list of key <col> tokens in select list
    sources: List[str] = field(default_factory=list)    # FROM/JOIN base identifiers
    associations: List[str] = field(default_factory=list)  # association to <target>

def _strip_comments(txt: str) -> str:
    txt = re.sub(r"/\*.*?\*/", " ", txt, flags=re.S)  # /* ... */
    txt = re.sub(r"//.*?$", " ", txt, flags=re.M)     # // ...
    txt = re.sub(r"--.*?$", " ", txt, flags=re.M)     # -- ...
    return txt

def parse_abap_cds_text(text: str) -> ABAPCDSModel:
    t = _strip_comments(text or "")

    # 1) Name from DEFINE VIEW / DEFINE VIEW ENTITY
    m = re.search(r"\bdefine\s+view(?:\s+entity)?\s+([A-Za-z_]\w*)", t, flags=re.I)
    name = m.group(1) if m else "UNKNOWN_CDS"

    # 2) Classic SQL view (DEFINE VIEW with @AbapCatalog.sqlViewName)
    m = re.search(r"@AbapCatalog\.sqlViewName\s*:\s*'([^']+)'", t, flags=re.I)
    sql_view = m.group(1) if m else None

    # 3) Extraction + CDC annotations
    extraction_enabled = bool(re.search(r"@Analytics\.dataExtraction\.enabled\s*:\s*true", t, flags=re.I))
    cdc_m = re.search(r"@Analytics\.dataExtraction\.delta\.changeDataCapture\.[^\s\}]+", t, flags=re.I)
    cdc_ann = cdc_m.group(0) if cdc_m else None

    # 4) Parameters
    pm = re.search(r"\bdefine\s+view(?:\s+entity)?\s+[A-Za-z_]\w*\s*\((.*?)\)\s+as\s+select", t, flags=re.I | re.S)
    params: List[str] = []
    if pm:
        raw = pm.group(1)
        parts, buf, depth = [], [], 0
        for ch in raw:
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth = max(0, depth - 1)
            if ch == ',' and depth == 0:
                parts.append(''.join(buf).strip()); buf = []
            else:
                buf.append(ch)
        if buf: parts.append(''.join(buf).strip())
        params = [p for p in parts if p]

    # 5) Keys in select list
    keys = re.findall(r"\bkey\s+([A-Za-z_][\w\.]*)", t, flags=re.I)

    # 6) Sources (FROM / JOIN)
    sources: List[str] = []
    for m in re.finditer(r"\bfrom\s+([A-Za-z_][\w\.]*)", t, flags=re.I):
        sources.append(m.group(1))
    for m in re.finditer(r"\bjoin\s+([A-Za-z_][\w\.]*)", t, flags=re.I):
        sources.append(m.group(1))
    sources = sorted(set(sources))

    # 7) Associations
    associations = [m.group(1) for m in re.finditer(r"\bassociation\s+to\s+([A-Za-z_][\w\.]*)", t, flags=re.I)]

    return ABAPCDSModel(
        name=name,
        sql_view_name=sql_view,
        extraction_enabled=extraction_enabled,
        cdc_annotation=cdc_ann,
        parameters=params,
        keys=sorted(set(keys)),
        sources=sources,
        associations=sorted(set(associations)),
    )