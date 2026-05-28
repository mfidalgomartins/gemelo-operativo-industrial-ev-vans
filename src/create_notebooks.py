from __future__ import annotations

import nbformat as nbf

from .config import NOTEBOOKS_DIR, ensure_directories


def create_notebooks() -> None:
    ensure_directories()

    nb_main = nbf.v4.new_notebook()
    # IDs de celda fijos: nbformat genera UUIDs aleatorios por defecto, lo que
    # ensucia el árbol git en cada ejecución del pipeline. Los fijamos para que
    # el notebook sea byte-estable entre regeneraciones.
    cells = [
        ("md-intro", nbf.v4.new_markdown_cell(
            "# Notebook Principal EV\n\n"
            "Notebook único para revisión ejecutiva y técnica del pipeline oficial EV.\n\n"
            "Objetivo: ejecutar y auditar **solo** la ruta oficial `ev_*`."
        )),
        ("cell-imports", nbf.v4.new_code_cell(
            "from src.run_pipeline import run_pipeline\n"
            "from src.ev_release_gate import run_release_gate"
        )),
        ("cell-run-pipeline", nbf.v4.new_code_cell(
            "# Recomendado: conservar generate_data=False para evitar regeneración innecesaria.\n"
            "result = run_pipeline(generate_data=False)\n"
            "result"
        )),
        ("cell-release-gate", nbf.v4.new_code_cell(
            "gate = run_release_gate()\n"
            "gate"
        )),
    ]
    for cell_id, cell in cells:
        cell["id"] = cell_id
    nb_main.cells = [cell for _, cell in cells]

    with open(NOTEBOOKS_DIR / "01_notebook_principal.ipynb", "w", encoding="utf-8") as f:
        nbf.write(nb_main, f)


if __name__ == "__main__":
    create_notebooks()
