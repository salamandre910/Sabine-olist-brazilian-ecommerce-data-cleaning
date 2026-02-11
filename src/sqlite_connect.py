# --- Connexion DB (SQLite database) ---

from pathlib import Path
from sqlalchemy import create_engine

DB_PATH = Path("db/olist.sqlite")
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

ENGINE = create_engine(f"sqlite:///{DB_PATH}", future=True)
