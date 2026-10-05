"""Render the silver ER diagram straight from the table specs, so the picture can never
drift from the code.

    uv run python scripts/render_er_diagram.py      # writes docs/er_silver.{dot,mmd} (+ .png/.svg if Graphviz is installed)
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from tpch_lakehouse.silver import SPECS  # noqa: E402

DOCS = ROOT / "docs"
# layout: left -> right follows the foreign keys (child -> parent)
HEADER = "#1F3A5F"


def _fk_columns(spec) -> set[str]:
    return {c for fk in spec.foreign_keys for c in fk.columns}


def dot() -> str:
    lines = [
        "digraph silver {",
        '  graph [rankdir=LR, nodesep=0.35, ranksep=0.9, bgcolor="white", fontname="Helvetica"];',
        '  node  [shape=plain, fontname="Helvetica", fontsize=11];',
        '  edge  [color="#555555", arrowhead=normal, arrowtail=crow, dir=both, fontname="Helvetica", fontsize=9];',
    ]
    for table, spec in SPECS.items():
        fks = _fk_columns(spec)
        rows = [f'<tr><td colspan="3" bgcolor="{HEADER}"><font color="white"><b>{table}</b></font></td></tr>']
        for name, dtype in spec.columns.values():
            tags = "/".join(t for t, on in (("PK", name in spec.primary_key), ("FK", name in fks)) if on)
            label = f"<b>{name}</b>" if name in spec.primary_key else name
            rows.append(
                f'<tr><td align="left" port="{name}">{label}</td>'
                f'<td align="left"><font color="#666666">{dtype}</font></td>'
                f'<td><font color="#B8500C">{tags or " "}</font></td></tr>'
            )
        lines.append(
            f'  {table} [label=<<table border="1" cellborder="0" cellspacing="0" cellpadding="3">{"".join(rows)}</table>>];'
        )
    for table, spec in SPECS.items():
        for fk in spec.foreign_keys:
            lines.append(f'  {table} -> {fk.parent} [label="{", ".join(fk.columns)}"];')
    lines.append("}")
    return "\n".join(lines)


def mermaid() -> str:
    lines = ["erDiagram"]
    for table, spec in SPECS.items():
        for fk in spec.foreign_keys:
            lines.append(f'    {fk.parent.upper()} ||--o{{ {table.upper()} : "{", ".join(fk.columns)}"')
    for table, spec in SPECS.items():
        fks = _fk_columns(spec)
        lines.append(f"    {table.upper()} {{")
        for name, dtype in spec.columns.values():
            keys = ", ".join(t for t, on in (("PK", name in spec.primary_key), ("FK", name in fks)) if on)
            lines.append(f"        {dtype.replace('(', '_').replace(',', '_').replace(')', '')} {name} {keys}".rstrip())
        lines.append("    }")
    return "\n".join(lines)


if __name__ == "__main__":
    DOCS.mkdir(exist_ok=True)
    (DOCS / "er_silver.dot").write_text(dot())
    (DOCS / "er_silver.mmd").write_text(mermaid())
    if shutil.which("dot"):
        for fmt in ("png", "svg"):
            args = ["dot", f"-T{fmt}", str(DOCS / "er_silver.dot"), "-o", str(DOCS / f"er_silver.{fmt}")]
            subprocess.run(args + (["-Gdpi=160"] if fmt == "png" else []), check=True)
        print("wrote docs/er_silver.png and .svg")
    else:
        print("Graphviz not found - wrote .dot and .mmd only")
