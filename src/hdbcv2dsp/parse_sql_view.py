from dataclasses import dataclass, field
from typing import List, Set
import re

@dataclass
class SQLViewModel:
    name: str
    sql: str
    columns: List[str] = field(default_factory=list)
    inputs: List[str] = field(default_factory=list)  # upstream tables/views

# split on commas that are NOT inside parentheses
_COMMA_OUTSIDE_PARENS = re.compile(r',(?=(?:[^()]*\([^()]*\))*[^()]*$)')

# CREATE [OR REPLACE] VIEW <identifier>
_VIEW_NAME_RE = re.compile(
    r'CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+'
    r'((?:"[^"]+"|\w+)(?:\.(?:"[^"]+"|\w+))?)',
    re.IGNORECASE
)

# SELECT ... FROM  (capture the select list)
_SELECT_LIST_RE = re.compile(
    r'SELECT\s+(.*?)\s+FROM\b',
    re.IGNORECASE | re.DOTALL
)

# FROM / JOIN sources (schema-qualified, quoted or unquoted)
_SOURCE_TOKENS_RE = re.compile(
    r'\bFROM\s+((?:"[^"]+"|\w+)(?:\.(?:"[^"]+"|\w+))?)'
    r'|'
    r'\b(?:LEFT|RIGHT|FULL|INNER|OUTER|CROSS)?\s*JOIN\s+((?:"[^"]+"|\w+)(?:\.(?:"[^"]+"|\w+))?)',
    re.IGNORECASE
)

# Correctly handle HTML &gt; -> >
_HTML_GT = re.compile(r'&gt;', re.IGNORECASE)

def _norm_ident(s: str) -> str:
    s = s.strip()
    if s.startswith('"') and s.endswith('"') and len(s) >= 2:
        return s[1:-1]
    return s

def parse_hdbview_or_sql(path: str) -> SQLViewModel:
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        sql = f.read()

    # normalize common HTML entity if present in uploads
    sql = _HTML_GT.sub('>', sql)

    # 1) view name
    m = _VIEW_NAME_RE.search(sql)
    name = _norm_ident(m.group(1)) if m else "UNKNOWN_VIEW"

    # 2) select list -> columns[]
    columns: List[str] = []
    sel = _SELECT_LIST_RE.search(sql)
    if sel:
        raw = sel.group(1).strip()
        columns = [p.strip() for p in _COMMA_OUTSIDE_PARENS.split(raw) if p.strip()]

    # 3) upstream sources -> inputs[]
    srcs: Set[str] = set()
    for g1, g2 in _SOURCE_TOKENS_RE.findall(sql):
        ident = g1 or g2
        if ident:
            srcs.add(_norm_ident(ident))

    return SQLViewModel(name=name, sql=sql, columns=columns, inputs=sorted(srcs))