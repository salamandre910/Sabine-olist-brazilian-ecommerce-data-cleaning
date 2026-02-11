from sqlite_connect import ENGINE

with ENGINE.connect() as conn:
    conn.exec_driver_sql("SELECT 1;")

print("OK: SQLite connection works")
