from __future__ import annotations

import nbformat as nbf

from .config import NOTEBOOKS_DIR, ensure_directories


def create_notebooks() -> None:
    ensure_directories()

    nb_main = nbf.v4.new_notebook()
    nb_main.cells = [
        nbf.v4.new_markdown_cell(
            "# Notebook Principal EV\n\n"
            "Notebook único para revisión ejecutiva y técnica del pipeline oficial EV.\n\n"
            "Objetivo: ejecutar y auditar **solo** la ruta oficial `ev_*`."
        ),
        nbf.v4.new_code_cell(
            "from src.run_pipeline import run_pipeline\n"
            "from src.ev_release_gate import run_release_gate"
        ),
        nbf.v4.new_code_cell(
            "# Recomendado: manter generate_data=False para evitar regeneração desnecessária.\n"
            "result = run_pipeline(generate_data=False)\n"
            "result"
        ),
        nbf.v4.new_code_cell(
            "gate = run_release_gate()\n"
            "gate"
        ),
    ]

    with open(NOTEBOOKS_DIR / "01_notebook_principal.ipynb", "w", encoding="utf-8") as f:
        nbf.write(nb_main, f)


if __name__ == "__main__":
    create_notebooks()
