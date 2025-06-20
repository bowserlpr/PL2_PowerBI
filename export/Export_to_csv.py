import mysql.connector
import pandas as pd
import os


conn = mysql.connector.connect(
  host="localhost",
  user="root",
  password="cD!1HVF&Pxos3cjv",
  database = "datamart"
)

cursor = conn.cursor()

output_folder = "export_csv"
os.makedirs(output_folder, exist_ok=True)

# Alle Tabellen ermitteln
cursor.execute("SHOW TABLES")
tables = [row[0] for row in cursor.fetchall()]

for table in tables:
    print(f"Exportiere Tabelle: {table}")
    
    # Daten aus Tabelle lesen
    df = pd.read_sql(f"SELECT * FROM `{table}`", conn)
    
    # Als CSV speichern
    csv_path = os.path.join(output_folder, f"{table}.csv")
    df.to_csv(csv_path, index=False, encoding='utf-8')
    
    print(f"{table} → {csv_path}")

# Verbindung schließen
cursor.close()
conn.close()
