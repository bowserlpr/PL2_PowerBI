import mysql.connector
import pandas as pd
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
import os

path_base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
path_mitarbeiterliste_initial = os.path.join(path_base, 'Data', 'T_IDMA_initial.csv')

datawarehousedb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="cD!1HVF&Pxos3cjv",
  database = "datawarehouse"
)

def get_df_diff(df_source, df_target, key_column=None, compare_columns=None):
    if key_column is None:
        key_column = df_source.columns[0]
    if isinstance(key_column, str):
      key_column = [key_column]

    if compare_columns is None:
        compare_columns = [col for col in df_source.columns if col not in key_column]

    merged = df_source.merge(df_target, on=key_column, how='left', suffixes=('', '_old'))

    df_insert = merged[merged[[f"{col}_old" for col in compare_columns]].isna().all(axis=1)][key_column + compare_columns]


    update_mask = merged[[f"{col}_old" for col in compare_columns]].notna().any(axis=1) & \
                  merged[compare_columns].ne(merged[[f"{col}_old" for col in compare_columns]].values).any(axis=1)
    df_update = merged[update_mask][key_column + compare_columns]

    return df_insert, df_update

def employee_import(conn, df_update, table_name="mitarbeiter"):
    # Setze Gültigkeitszeitraum auf den ersten Tag des aktuellen Monats
    gültig_von = date.today().replace(day=1)
    # Setze Gültig_bis auf den letzten Tag des Vormonats
    gültig_bis = (date.today().replace(day=1) - timedelta(days=1))
    cursor = conn.cursor()

    for _, row in df_update.iterrows():
        # Aktualisiere Gültig_bis für den aktuellen Datensatz
        cursor.execute(f"""
            UPDATE {table_name}
            SET Gueltig_bis = %s
            WHERE PNR = %s AND Gueltig_bis IS NULL
        """, (gültig_bis, row['PNR']))

        # Füge neuen Datensatz mit Gültig_von und NULL für Gültig_bis ein
        cursor.execute(f"""
            INSERT INTO {table_name}
                (PNR, Vorname, Nachname, Gueltig_von, Gueltig_bis, Kostenstellen_ID, Abteilungs_ID)
            VALUES (%s, %s, %s, %s, NULL, %s, %s)
        """, (
            row['PNR'],
            row["Vorname"],
            row["Nachname"],
            gültig_von,
            row["Kostenstellen_ID"],
            row["Abteilungs_ID"]
        ))

def apply_df_diff(conn, table_name, df_insert=pd.DataFrame(), df_update=pd.DataFrame(), key_column=None):
    cursor = conn.cursor()

    if df_insert.empty and df_update.empty:
        return
    
    if key_column is None:
        key_column = df_insert.columns[0] if not df_insert.empty else df_update.columns[0]


    if not df_insert.empty:
        placeholders = ", ".join(["%s"] * len(df_insert.columns))
        cols = ", ".join(df_insert.columns)
        sql_insert = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
        save_df_insert = df_insert.astype("object").where(pd.notna(df_insert), None).values.tolist()
        cursor.executemany(sql_insert, save_df_insert)
        print(f"[{table_name}] {len(df_insert)} INSERTs.")

    if not df_update.empty:
        compare_columns = [col for col in df_update.columns if col != key_column]
        for _, row in df_update.iterrows():
            set_clause = ", ".join([f"{col} = %s" for col in compare_columns])
            sql_update = f"UPDATE {table_name} SET {set_clause} WHERE {key_column} = %s"
            values = [None if pd.isna(row[col]) else row[col] for col in compare_columns] + [row[key_column]]
            cursor.execute(sql_update, values)
        print(f"[{table_name}] {len(df_update)} UPDATEs.")

    conn.commit()
    cursor.close()

def get_df(conn, sql ):
    cursor = conn.cursor()
    cursor.execute(sql)
    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=columns)
    cursor.close()
    df = df.where(pd.notna(df), None)
    return df


# Von Datawarehouse Mitarbeiterdaten extrahieren
sql = """
SELECT * 
FROM mitarbeiter 
Where gueltig_bis is null
"""
df_target_mitarbeiter = get_df(datawarehousedb, sql)

sql = """
    SELECT *
    FROM abteilung
"""
df_target_abteilung = get_df(datawarehousedb, sql)

sql = """
SELECT * 
FROM kostenstelle
"""
df_target_kostenstelle = get_df(datawarehousedb, sql)

# Von CSV Mitarbeiterdaten extrahieren

df_mitarbeiterliste_initial = pd.read_csv(path_mitarbeiterliste_initial,sep=';',quotechar='\'',encoding='utf8', dtype=str)
mitarbeiter = (df_mitarbeiterliste_initial[["PNR","Vorname","Nachname","Kostenstellennummer","OE-Nummer"]])
mitarbeiter = mitarbeiter.rename(columns={"Kostenstellennummer":"Kostenstellen_ID","OE-Nummer":"Abteilungs_ID"})
df_source_mitarbeiter = mitarbeiter.where(pd.notnull(mitarbeiter), None)
df_source_mitarbeiter = df_source_mitarbeiter.astype("object").where(pd.notna(df_source_mitarbeiter), None)


kostenstelle = (df_mitarbeiterliste_initial[["Kostenstellennummer","Kostenstellenbezeichnung"]])
kostenstelle = kostenstelle.rename(columns={"Kostenstellennummer":"Kostenstellen_ID"})
kostenstelle = kostenstelle.drop_duplicates()
df_source_kostenstelle = kostenstelle[kostenstelle["Kostenstellen_ID"].notnull()]

abteilung = (df_mitarbeiterliste_initial[["OE-Nummer","OE-Bezeichnung"]])
abteilung = abteilung.rename(columns={"OE-Nummer":"Abteilungs_ID","OE-Bezeichnung":"Abteilungsbezeichnung"})
abteilung = abteilung.drop_duplicates()
df_source_abteilung = abteilung[abteilung["Abteilungs_ID"].notnull()]





df_insert_mitarbeiter, df_update_mitarbeiter = get_df_diff(df_source_mitarbeiter, df_target_mitarbeiter,key_column="PNR", compare_columns=["Vorname", "Nachname", "Kostenstellen_ID", "Abteilungs_ID"])
df_insert_mitarbeiter["Gueltig_von"] = (date.today().replace(day=1) - relativedelta(years=1))

df_insert_abteilung, df_update_abteilung = get_df_diff(df_source_abteilung, df_target_abteilung)
df_insert_kostenstelle, df_update_kostenstelle = get_df_diff(df_source_kostenstelle, df_target_kostenstelle)

apply_df_diff(datawarehousedb, "kostenstelle", df_insert_kostenstelle, df_update_kostenstelle)
apply_df_diff(datawarehousedb, "abteilung", df_insert_abteilung, df_update_abteilung)

apply_df_diff(datawarehousedb, "mitarbeiter", df_insert_mitarbeiter)
employee_import(datawarehousedb, df_update_mitarbeiter)


###############
# Verbindung schließen
##############
datawarehousedb.close()