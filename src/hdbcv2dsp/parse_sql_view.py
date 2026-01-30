from dataclasses import dataclass, field
from typing import List
import re

@dataclass
class SQLViewModel:
    name: str
    sql: str
    columns: List[str] = field(default_factory=list)
    inputs: List[str] = field(default_factory=list)  # upstream tables/views

def parse_hdbview_or_sql(path: str) -> SQLViewModel:
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        sql = f.read()

    # 1) View name
    m = re.search(r'CREATE\s+(OR\s+REPLACE\s+)?VIEW\s+("?[\w:#.$/]+"?)', sql, re.IGNORECASE)
    name = m.group(2).strip('"') if m else "UNKNOWN_VIEW"

    # 2) Select list (very simple—good enough for MVP)
    cols: List[str] = []
    sel = re.search(r'SELECT\s+(.*?)\s+FROM\s', sql, re.IGNORECASE | re.DOTALL)
    if sel:
        # split commas not within parentheses
        cols = [c.strip() for c in re.split(r',(?![^(]*\))', sel.group(1)) if c.strip()]

    # 3) Upstream sources from FROM/JOIN
    sources = set()
    for pair in re.findall(r'\bFROM\s+([^\s,()]+)|\bJOIN\s+([^\s,()]+)', sql, re.IGNORECASE):
        for t in pair:
            if t:
                sources.add(t.strip('"'))

    return SQLViewModel(name=name, sql=sql, columns=cols, inputs=sorted(sources))