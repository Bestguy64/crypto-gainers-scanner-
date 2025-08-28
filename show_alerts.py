# show_alerts.py
import sqlite3, pandas as pd
con = sqlite3.connect("alerts.db")
try:
    df = pd.read_sql_query("SELECT * FROM alerts ORDER BY alert_time DESC LIMIT 10", con)
    print(df.to_string(index=False))
finally:
    con.close()
