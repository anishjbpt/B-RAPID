from dataclasses import dataclass, field
from typing import Dict, List
import re

@dataclass
class ProcedureModel:
    name: str
    sql: str
    parameters: List[Dict[str, str]] = field(default_factory=list)  # {'mode','name','type'}
    reads_from: List[str] = field(default_factory=list)
    writes_to: List[str] = field(default_factory=list)
    calls: List[str] = field(default_factory=list)
    temp_tables: List[str] = field(default_factory=list)            # NEW
    ctas_targets: List[str] = field(default_factory=list)           # NEW (MVP detection)

def parse_hdbprocedure_or_sql(path: str) -> ProcedureModel:
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        sql = f.read()

    # --- 1) Name: CREATE/ALTER + PROCEDURE/PROC ---
    name_m = re.search(
        r'\b(CREATE|ALTER)\s+(PROCEDURE|PROC)\s+("?[\w:#.$/\[\]\.]+?"?)',
        sql, re.IGNORECASE
    )
    raw_name = name_m.group(3) if name_m else "UNKNOWN_PROCEDURE"
    name = raw_name.strip('"').strip('[]')  # normalize [] or " "
    # Optional: collapse [schema].[name] to schema.name
    name = name.replace('[', '').replace(']', '')

    # --- 2) Parameters: support both ( ... ) and inline before AS ---
    params: List[Dict[str, str]] = []

    # Try ( ... ) form first
    paren = re.search(
        r'\b(PROCEDURE|PROC)\s+[^(\s]+\s*\((.*?)\)\s*AS\b',
        sql, re.IGNORECASE | re.DOTALL
    )

    raw = ""
    if paren:
        raw = paren.group(2)
    else:
        # Inline: grab everything after name up to the first AS
        after = re.search(
            r'\b(CREATE|ALTER)\s+(PROCEDURE|PROC)\s+("?[\w:#.$/\[\]\.]+?"?)\s+(.*?)\bAS\b',
            sql, re.IGNORECASE | re.DOTALL
        )
        if after:
            raw = after.group(4)

    if raw:
        for part in re.split(r',(?![^(]*\))', raw):
            ptxt = part.strip()
            if not ptxt:
                continue
            pm = re.match(
                r'\s*(?:(IN|OUT|INOUT)\s+)?("?[\w:#.$/\[\]\.]+?"?)\s+([\w\(\)]+)',
                ptxt, re.IGNORECASE
            )
            if pm:
                mode = (pm.group(1) or 'IN').upper()
                pname = pm.group(2).strip('"').strip('[]')
                ptype = pm.group(3)
                params.append({'mode': mode, 'name': pname, 'type': ptype})

    # --- 3) Dependencies (MVP regex) ---
    reads, writes, calls = set(), set(), set()

    for grp in re.findall(r'\bFROM\s+([^\s,();]+)|\bJOIN\s+([^\s,();]+)', sql, re.IGNORECASE):
        for t in grp:
            if t:
                reads.add(t.strip('"'))

    for tgt in re.findall(r'\bINSERT\s+INTO\s+([^\s(;,]+)', sql, re.IGNORECASE):
        writes.add(tgt.strip('"'))
    for tgt in re.findall(r'\bUPDATE\s+([^\s(;,]+)', sql, re.IGNORECASE):
        writes.add(tgt.strip('"'))
    for tgt in re.findall(r'\bMERGE\s+INTO\s+([^\s(;,]+)', sql, re.IGNORECASE):
        writes.add(tgt.strip('"'))

    for c in re.findall(r'\bCALL\s+([^\s(;,]+)', sql, re.IGNORECASE):
        calls.add(c.strip('"'))

    # --- 4) Temp tables and CTAS (Synapse/MPP style) ---
    temp_tables = set()
    # CREATE TABLE #Temp ...
    for t in re.findall(r'\bCREATE\s+TABLE\s+(#\w+)', sql, re.IGNORECASE):
        temp_tables.add(t)
    # SELECT ... INTO #Temp ...
    for t in re.findall(r'\bINTO\s+(#\w+)', sql, re.IGNORECASE):
        temp_tables.add(t)

    # CTAS targets (MVP): CREATE TABLE <name> WITH (...) AS SELECT
    ctas_targets = set()
    for m in re.findall(r'\bCREATE\s+TABLE\s+([#\w\.\[\]]+)\s+WITH\s*\(.*?\)\s+AS\s+SELECT',
                        sql, re.IGNORECASE | re.DOTALL):
        ctas_targets.add(m.replace('[', '').replace(']', ''))

    return ProcedureModel(
        name=name, sql=sql, parameters=params,
        reads_from=sorted(reads), writes_to=sorted(writes), calls=sorted(calls),
        temp_tables=sorted(temp_tables), ctas_targets=sorted(ctas_targets)
    )