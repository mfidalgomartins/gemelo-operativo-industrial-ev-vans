from pathlib import Path
import os
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Evita fallos del backend macOS de matplotlib en ejecución headless de tests.
os.environ.setdefault("MPLBACKEND", "Agg")
