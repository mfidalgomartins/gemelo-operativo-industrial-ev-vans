from __future__ import annotations

import nbformat as nbf

from .config import NOTEBOOKS_DIR, ensure_directories


def create_notebooks() -> None:
    ensure_directories()

    nb_main = nbf.v4.new_notebook()
    nb_main.cells = [
        nbf.v4.new_markdown_cell(
            "# Notebook Principal\n\n"
            "Gemelo Operativo para transición EV: generación de datos, modelado SQL, análisis y scoring."
        ),
        nbf.v4.new_code_cell(
            "from src.data_generation import generate_raw_data\n"
            "from src.data_quality import run_data_quality_audit\n"
            "from src.sql_modeling import run_sql_modeling\n"
            "from src.analysis import run_analysis\n"
            "from src.scenario_engine import run_scenario_engine\n"
            "from src.dashboard_builder import build_dashboard\n"
            "from src.reports import build_reports"
        ),
        nbf.v4.new_code_cell(
            "generate_raw_data()\n"
            "run_data_quality_audit()\n"
            "run_sql_modeling()\n"
            "run_analysis()\n"
            "run_scenario_engine()\n"
            "build_dashboard()\n"
            "build_reports()"
        ),
    ]

    nb_sim = nbf.v4.new_notebook()
    nb_sim.cells = [
        nbf.v4.new_markdown_cell(
            "# Simulación Operativa\n\n"
            "Exploración de sensibilidad del throughput y SLA ante cambios del mix EV."
        ),
        nbf.v4.new_code_cell(
            "import pandas as pd\n"
            "import matplotlib.pyplot as plt\n"
            "import seaborn as sns\n"
            "from src.scenario_engine import run_scenario_engine\n"
            "sns.set_theme(style='whitegrid')"
        ),
        nbf.v4.new_code_cell(
            "df = run_scenario_engine()\n"
            "display(df)"
        ),
        nbf.v4.new_code_cell(
            "plt.figure(figsize=(10,4))\n"
            "sns.lineplot(data=df, x='mix_ev_pct', y='throughput_diario_unidades', marker='o')\n"
            "sns.lineplot(data=df, x='mix_ev_pct', y='cumplimiento_sla_pct', marker='o')\n"
            "plt.title('Trade-off throughput vs SLA durante transición EV')\n"
            "plt.show()"
        ),
    ]

    with open(NOTEBOOKS_DIR / "01_notebook_principal.ipynb", "w", encoding="utf-8") as f:
        nbf.write(nb_main, f)

    with open(NOTEBOOKS_DIR / "02_simulacion_operativa.ipynb", "w", encoding="utf-8") as f:
        nbf.write(nb_sim, f)


if __name__ == "__main__":
    create_notebooks()
