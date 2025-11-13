import importlib
from sqlalchemy import text
m = importlib.import_module('app')

m.init_db()
with m.engine.begin() as conn:
    rows = conn.execute(text('SELECT key, value FROM site_settings ORDER BY key ASC')).fetchall()
    for r in rows:
        print(f"{r.key} = {r.value}")
