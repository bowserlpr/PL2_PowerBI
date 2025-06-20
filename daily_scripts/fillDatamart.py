import mysql.connector
import pandas as pd
import numpy as np
from decimal import Decimal

from sqlalchemy import create_engine, types


datawarehousedb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="cD!1HVF&Pxos3cjv",
  database = "datawarehouse"
)


datamartdb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="cD!1HVF&Pxos3cjv",
  database = "datamart"
)


#####################################
# Functions
#####################################

def ensure_dummy_in_df(df, expected_name="keine Buchung", key_column=None, id_val=None, name_col=None):
    if key_column is None:
      key_column = df.columns[0]

    name_columns = [name_col] if name_col else [col for col in df.columns if col != key_column]

    #Wenn Dummywert nicht angegeben, dann anhand des Typs bestimmen
    if id_val is None:
        sample_type = type(df[key_column].iloc[0])
        if sample_type in [int, np.int64, np.int32]:
            id_val = -1
        elif sample_type in [float, np.float64]:
            id_val = -1.0
        elif sample_type == str:
            id_val = "-1"
        else:
            id_val = "-1"

    # suche nach Dummy
    dummy_row = df[df[key_column] == id_val]

    if dummy_row.empty:
        # Dummy fehlt → neue Zeile einfügen
        new_row = {col: expected_name for col in name_columns}
        new_row[key_column] = id_val
        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    else:
        # Werte aktualisieren, wenn ungleich
        for col in name_columns:
            if dummy_row.iloc[0][col] != expected_name:
                df.loc[df[key_column] == id_val, col] = expected_nameD
    return df

def get_df_diff(df_source, df_target, key_column=None, compare_columns=None):
    if key_column is None:
        key_column = df_source.columns[0]
    if isinstance(key_column, str):
      key_column = [key_column]

    if compare_columns is None:
        compare_columns = [col for col in df_source.columns if col not in key_column]
    print(f"Comparing columns: {compare_columns}")

    print("df_source columns:", df_source.columns.tolist())
    print("df_target columns:", df_target.columns.tolist())
    merged = df_source.merge(df_target, on=key_column, how='left', suffixes=('', '_old'))


    df_insert = merged[merged[[f"{col}_old" for col in compare_columns]].isna().all(axis=1)][key_column + compare_columns]


    update_mask = merged[[f"{col}_old" for col in compare_columns]].notna().any(axis=1) & \
                  merged[compare_columns].ne(merged[[f"{col}_old" for col in compare_columns]].values).any(axis=1)
    df_update = merged[update_mask][key_column + compare_columns]

    return df_insert, df_update


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
            print(f"Executing SQL: {sql_update} with values {values}")
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


#####################################
# extract from Data Warehouse
#####################################



sql = """
SELECT *
FROM abteilung 
"""
df_source_abteilung = get_df(datawarehousedb, sql )



sql = """
SELECT *
FROM buchungstyp
"""
df_source_buchungstyp = get_df(datawarehousedb, sql )

sql = """
SELECT * 
FROM kostenstelle
"""
df_source_kostenstelle = get_df(datawarehousedb, sql )

sql = """
SELECT `PNR`, `Vorname`, `Nachname` 

FROM Mitarbeiter 
Where Gueltig_bis is null
"""
df_source_mitarbeiter = get_df(datawarehousedb, sql )

sql = """
SELECT `Projekt_ID`, `Projektname`
FROM projekt
"""
df_source_projekt = get_df(datawarehousedb, sql )


sql = """
SELECT `SAP_Anwendungssystem_ID`, `SAP_Anwendungssystem`
FROM sap_anwendung
"""
df_source_sap = get_df(datawarehousedb, sql )




sql = """
SELECT 
  DATE_FORMAT(wl.Startzeitpunkt, '%Y-%m-01') AS Zeitstempel, 
  ROUND(IFNULL(SUM(wl.Gebuchte_Zeit) / 3600, 0), 1) AS Gebuchte_Zeit, 
  m.PNR, 
  m.Abteilungs_ID, 
  p.Projekt_ID 
  
FROM projekt p 
LEFT JOIN jiraissue ji ON ji.Projekt_ID = p.Projekt_ID 
LEFT JOIN worklog wl 
     ON wl.Issue_ID = ji.Issue_ID 
LEFT JOIN mitarbeiter m 
	ON wl.Mitarbeiter_ID = m.Mitarbeiter_ID 
GROUP BY DATE_FORMAT(wl.Startzeitpunkt, '%Y-%m-01'), p.Projekt_ID, p.Projektname, m.Abteilungs_ID, m.PNR 
ORDER BY Zeitstempel, p.Projekt_ID, PNR; 
"""

df_source_fact_buchungen = get_df(datawarehousedb, sql)
df_source_fact_buchungen['Zeitstempel'] = pd.to_datetime(df_source_fact_buchungen['Zeitstempel'])

#Monatswerte generieren
months = pd.date_range(start='2024-06-01', end=pd.Timestamp.today(), freq='MS')
#Alle Projekte extrahieren
projects = df_source_fact_buchungen['Projekt_ID'].unique()

#Projekte rausfiltern, die gar keine Buchungen haben. Diese werden später ergänzt
df_source_fact_buchungen = df_source_fact_buchungen[df_source_fact_buchungen['Zeitstempel'].notna()]

# Alle Kombinationen von Monaten und Projekten erstellen
projects_months = pd.MultiIndex.from_product(
    [months, projects],
    names=['Zeitstempel', 'Projekt_ID']
).to_frame(index=False)

# Nur die monate rausfiltern, die in den Monaten noch fehlen
missing_projects_months = pd.merge(
    projects_months,
    df_source_fact_buchungen,
    on=['Zeitstempel', 'Projekt_ID'],
    how='left',
    indicator=True
).query("_merge == 'left_only'").drop(columns="_merge")

# Fehlende Monate und Projekte mit Dummy-Werten auffüllen
missing_projects_months['PNR'] = "-1"
missing_projects_months['Abteilungs_ID'] = "-1"
missing_projects_months['Gebuchte_Zeit'] = Decimal("0.0")

# Fehlende Monate und Projekte zu den bestehenden Daten hinzufügen
df_source_fact_buchungen = pd.concat([df_source_fact_buchungen, missing_projects_months], ignore_index=True)


sql = """
    SELECT 
        DATE_FORMAT(DATE(wl.Startzeitpunkt), '%Y-%m-01') AS Zeitstempel,
        ROUND(SUM(wl.Gebuchte_Zeit) / 3600, 1) AS Gebuchte_Zeit,
        m.PNR,
        m.Abteilungs_ID,
        p.Projekt_ID,
        ji.Buchungstyp_ID,
        m.Kostenstellen_ID,
        ji.SAP_Anwendungssystem_ID
    FROM projekt p
    JOIN jiraissue ji ON ji.Projekt_ID = p.Projekt_ID
    JOIN worklog wl ON wl.Issue_ID = ji.Issue_ID
    LEFT JOIN mitarbeiter m ON wl.Mitarbeiter_ID = m.Mitarbeiter_ID
    GROUP BY 
        DATE_FORMAT(DATE(wl.Startzeitpunkt), '%Y-%m-01'),
        p.Projekt_ID,
        m.PNR,
        m.Abteilungs_ID,
        ji.Buchungstyp_ID,
        m.Kostenstellen_ID,
        ji.SAP_Anwendungssystem_ID

    ORDER BY Zeitstempel, Projekt_ID;
  """

df_source_fact_jira_buchungen = get_df(datawarehousedb, sql)
df_source_fact_jira_buchungen['Zeitstempel'] = pd.to_datetime(df_source_fact_jira_buchungen['Zeitstempel'])


#####################################
# extract from Target datamart
#####################################


sql = """
SELECT *
FROM dim_abteilung 
"""
df_target_abteilung = get_df(datamartdb, sql )



sql = """
SELECT *
FROM dim_buchungstyp
"""
df_target_buchungstyp = get_df(datamartdb, sql )

sql = """
SELECT * 
FROM dim_kostenstelle
"""
df_target_kostenstelle = get_df(datamartdb, sql )

sql = """
SELECT *

FROM dim_mitarbeiter 
"""
df_target_mitarbeiter = get_df(datamartdb, sql )

sql = """
SELECT *
FROM dim_projekt
"""
df_target_projekt = get_df(datamartdb, sql )


sql = """
SELECT *
FROM dim_sap_anwendung
"""
df_target_sap = get_df(datamartdb, sql )

sql = """
SELECT *
FROM fact_buchungen
"""
df_target_fact_buchungen = get_df(datamartdb, sql)

sql = """
SELECT *
FROM fact_jira_buchungen
"""
df_target_fact_jira_buchungen = get_df(datamartdb, sql)

#####################################
# transform 
#####################################
df_source_abteilung = ensure_dummy_in_df(df_source_abteilung)
df_source_buchungstyp = ensure_dummy_in_df(df_source_buchungstyp)
df_source_kostenstelle = ensure_dummy_in_df(df_source_kostenstelle)
df_source_mitarbeiter = ensure_dummy_in_df(df_source_mitarbeiter)
df_source_sap = ensure_dummy_in_df(df_source_sap, expected_name = "fehlende SAP-Anwendung")


df_insert_abteilung, df_update_abteilung = get_df_diff(df_source_abteilung, df_target_abteilung)
df_insert_buchungstyp, df_update_buchungstyp = get_df_diff(df_source_buchungstyp, df_target_buchungstyp)
df_insert_kostenstelle, df_update_kostenstelle = get_df_diff(df_source_kostenstelle, df_target_kostenstelle)
df_insert_mitarbeiter, df_update_mitarbeiter = get_df_diff(df_source_mitarbeiter, df_target_mitarbeiter)
df_insert_projekt, df_update_projekt = get_df_diff(df_source_projekt, df_target_projekt)
df_insert_sap, df_update_sap = get_df_diff(df_source_sap, df_target_sap)
df_insert_fact_buchungen, df_update_fact_buchungen = get_df_diff(df_source_fact_buchungen, df_target_fact_buchungen, key_column=['Zeitstempel','PNR', 'Abteilungs_ID', 'Projekt_ID'])
df_insert_fact_jira_buchungen, df_update_fact_jira_buchungen = get_df_diff(df_source_fact_jira_buchungen, df_target_fact_jira_buchungen, key_column=['Zeitstempel','PNR', 'Abteilungs_ID', 'Projekt_ID', 'Buchungstyp_ID', 'Kostenstellen_ID', 'SAP_Anwendungssystem_ID'])


apply_df_diff(datamartdb, 'dim_abteilung', df_insert_abteilung, df_update_abteilung)
apply_df_diff(datamartdb, 'dim_buchungstyp', df_insert_buchungstyp, df_update_buchungstyp)
apply_df_diff(datamartdb, 'dim_kostenstelle', df_insert_kostenstelle, df_update_kostenstelle)
apply_df_diff(datamartdb, 'dim_mitarbeiter', df_insert_mitarbeiter, df_update_mitarbeiter)
apply_df_diff(datamartdb, 'dim_projekt', df_insert_projekt, df_update_projekt)
apply_df_diff(datamartdb, 'dim_sap_anwendung', df_insert_sap, df_update_sap)



apply_df_diff(datamartdb, 'fact_buchungen', df_insert_fact_buchungen, df_update_fact_buchungen)
apply_df_diff(datamartdb, 'fact_jira_buchungen', df_insert_fact_jira_buchungen, df_update_fact_jira_buchungen)

