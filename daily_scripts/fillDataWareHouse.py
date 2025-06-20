import mysql.connector
import pandas as pd
import os
from datetime import date, timedelta
import numpy as np


path_base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
log_dir = os.path.join(path_base, 'Logs', 'Missing Data')
path_mitarbeiterliste = os.path.join(path_base, 'Data', 'T_IDMA.csv')

myJiradb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="cD!1HVF&Pxos3cjv",
  database = "jira"
)

datawarehousedb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="cD!1HVF&Pxos3cjv",
  database = "datawarehouse"
)
#####################################
# functions
# #####################################

def get_df_diff(df_source, df_target, key_column=None, compare_columns=None):
    
    if key_column is None:
        key_column = df_source.columns[0]
    
    if compare_columns is None:
        compare_columns = [col for col in df_source.columns if col != key_column]
    print(f"Comparing columns: {compare_columns}")


    merged = df_source.merge(df_target, on=key_column, how='left', suffixes=('', '_old'))


    df_insert = merged[merged[[f"{col}_old" for col in compare_columns]].isna().all(axis=1)][[key_column] + compare_columns]

    update_mask = merged[[f"{col}_old" for col in compare_columns]].notna().any(axis=1) & \
                  merged[compare_columns].ne(merged[[f"{col}_old" for col in compare_columns]].values).any(axis=1)
    df_update = merged[update_mask][[key_column] + compare_columns]


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
            cursor.execute(sql_update, values)
        print(f"[{table_name}] {len(df_update)} UPDATEs.")
        print(values)

    conn.commit()
    cursor.close()

def get_df(conn, sql):
    cursor = conn.cursor()
    cursor.execute(sql)
    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=columns)
    cursor.close()
    df = df.where(pd.notna(df), None)
    #df = df.astype("object").where(pd.notna(df), None)
    return df

def employee_import(conn, df_update, table_name="mitarbeiter"):
    gültig_von = date.today().replace(day=1)
    gültig_bis = (date.today().replace(day=1) - timedelta(days=1))
    cursor = conn.cursor()

    for _, row in df_update.iterrows():

      cursor.execute(f"""
            UPDATE {table_name}
            SET Gueltig_bis = %s
            WHERE PNR = %s AND Gueltig_bis IS NULL
        """, (gültig_bis, row['PNR']))
      
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
      
def assign_mitarbeiter_to_worklog(df_worklog, df_mitarbeiter):
  df_worklog = df_worklog.copy()
  df_mitarbeiter = df_mitarbeiter.copy()

  df_worklog["Startzeitpunkt"] = pd.to_datetime(df_worklog["Startzeitpunkt"])
  df_mitarbeiter["Gueltig_von"] = pd.to_datetime(df_mitarbeiter["Gueltig_von"])
  df_mitarbeiter["Gueltig_bis"] = pd.to_datetime(df_mitarbeiter["Gueltig_bis"]).fillna(pd.Timestamp("2099-12-31"))

  # Merge über PNR
  df_merged = df_worklog.merge(
      df_mitarbeiter,
      left_on="PNR",
      right_on="PNR",
      how="left",
      suffixes=("", "_mitarbeiter")
  )

  # Filter: Zeitraum gültig zum Zeitpunkt der Buchung
  df_filtered = df_merged[
      (df_merged["Startzeitpunkt"] >= df_merged["Gueltig_von"]) &
      (df_merged["Startzeitpunkt"] <= df_merged["Gueltig_bis"])
  ].copy()
  cols = [col for col in df_worklog.columns if col != "PNR"]
  df_result = df_filtered[cols + ["Mitarbeiter_ID"]]

  return df_result

def ensure_dummy_in_df(df, expected_name="keine Buchung", key_column=None, id_val=None, name_col=None):
    # Suche Zeile mit der Dummy-ID
    if key_column is None:
      key_column = df.columns[0]

    name_columns = [name_col] if name_col else [col for col in df.columns if col != key_column]

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

    dummy_row = df[df[key_column] == id_val]

    if dummy_row.empty:
        # Dummy fehlt → neue Zeile einfügen
        new_row = {col: expected_name for col in name_columns}
        new_row[key_column] = id_val
        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        print(f"Dummy ({id_val}) eingefügt.")
    else:
        # Werte aktualisieren, wenn ungleich
        for col in name_columns:
            if dummy_row.iloc[0][col] != expected_name:
                df.loc[df[key_column] == id_val, col] = expected_name
                print(f"Dummy ({id_val}) Spalte '{col}' korrigiert.")
        if all(dummy_row.iloc[0][col] == expected_name for col in name_columns):
            print(f"Dummy ({id_val}) bereits korrekt.")

    return df
    
#####################################
# extract from Jira Database
#####################################

sql = "SELECT ID, PNAME FROM project;"
df_source_project = get_df(myJiradb, sql)
df_source_project = df_source_project.rename(columns={"ID":"Projekt_ID", "PNAME":"Projektname"})


sql = """
SELECT DISTINCT cfo.ID, cfo.customvalue 

FROM jira.jiraissue as issue 
Left JOIN jira.customfieldvalue as customvalue ON customvalue.issue = issue.id 
Left JOIN Jira.customfieldoption as cfo ON customvalue.stringvalue = cfo.id 

where customvalue.customfield = 10333
"""
df_source_buchungstyp = get_df(myJiradb, sql)
df_source_buchungstyp = df_source_buchungstyp.rename(columns={"ID":"Buchungstyp_ID", "customvalue":"Buchungstyp"})


sql = "\
SELECT DISTINCT cfo.ID, cfo.customvalue \
\
FROM jira.jiraissue as issue \
Left JOIN jira.customfieldvalue as customvalue ON customvalue.issue = issue.id \
Left JOIN Jira.customfieldoption as cfo ON customvalue.stringvalue = cfo.id \
\
where customvalue.customfield = 10502 \
"
df_source_sap = get_df(myJiradb, sql)
df_source_sap = df_source_sap.rename(columns={"ID":"SAP_Anwendungssystem_ID", "customvalue":"SAP_Anwendungssystem"})

df_source_sap = ensure_dummy_in_df(df_source_sap, expected_name = "fehlende SAP-Anwendung")
df_source_sap = df_source_sap.astype({"SAP_Anwendungssystem_ID": int})



sql = '\
SELECT issue.id as Issue_ID, issue.project as Projekt_ID, cfo1.id as SAP_Anwendungssystem_ID, cfo2.id AS Buchungstyp_ID \
FROM jira.jiraissue as issue \
Left JOIN jira.customfieldvalue as cfv1 \
	ON cfv1.issue = issue.id AND cfv1.CUSTOMFIELD = 10502 \
Left JOIN Jira.customfieldoption as cfo1 \
	ON cfv1.stringvalue = cfo1.id \
Left JOIN jira.customfieldvalue as cfv2  \
	ON cfv2.issue = issue.id AND cfv2.CUSTOMFIELD = 10333 \
Left JOIN Jira.customfieldoption as cfo2 \
	ON cfv2.stringvalue = cfo2.id '

df_source_jiraissue = get_df(myJiradb, sql)

df_source_jiraissue['SAP_Anwendungssystem_ID'] = df_source_jiraissue['SAP_Anwendungssystem_ID'].fillna(-1)


sql = '\
SELECT id, issueid, updateauthor, startdate, timeworked \
FROM jira.worklog'

df_source_worklog = get_df(myJiradb, sql)
df_source_worklog = df_source_worklog.rename(columns={"id":"Worklog_ID", "issueid":"Issue_ID", "updateauthor":"PNR", "startdate":"Startzeitpunkt", "timeworked":"Gebuchte_Zeit"})




#####################################
# Read personal Data
#####################################
df_mitarbeiterliste = pd.read_csv(path_mitarbeiterliste,sep=';',quotechar='\'',encoding='utf8', dtype=str)

mitarbeiter = (df_mitarbeiterliste[["PNR","Vorname","Nachname","Kostenstellennummer","OE-Nummer"]])
mitarbeiter = mitarbeiter.rename(columns={"Kostenstellennummer":"Kostenstellen_ID","OE-Nummer":"Abteilungs_ID"})
mitarbeiterColumns = ','.join(mitarbeiter.columns)
mitarbeiterValues=','.join(['%s' for i in mitarbeiter.columns])
df_source_mitarbeiter = mitarbeiter.where(pd.notnull(mitarbeiter), None)
df_source_mitarbeiter = df_source_mitarbeiter.astype("object").where(pd.notna(df_source_mitarbeiter), None)


kostenstelle = (df_mitarbeiterliste[["Kostenstellennummer","Kostenstellenbezeichnung"]])
kostenstelle = kostenstelle.rename(columns={"Kostenstellennummer":"Kostenstellen_ID"})
kostenstelle = kostenstelle.drop_duplicates()
df_source_kostenstelle = kostenstelle[kostenstelle["Kostenstellen_ID"].notnull()]

abteilung = (df_mitarbeiterliste[["OE-Nummer","OE-Bezeichnung"]])
abteilung = abteilung.rename(columns={"OE-Nummer":"Abteilungs_ID","OE-Bezeichnung":"Abteilungsbezeichnung"})
abteilung = abteilung.drop_duplicates()
df_source_abteilung = abteilung[abteilung["Abteilungs_ID"].notnull()]

#####################################
# extract from Data Warehouse
#####################################
sql = '\
SELECT * \
FROM jiraissue'

df_target_jiraissue = get_df(datawarehousedb, sql)

sql = """
    SELECT *
    FROM abteilung
"""
df_target_abteilung = get_df(datawarehousedb, sql)

sql = '\
SELECT * \
FROM buchungstyp'
df_target_buchungstyp = get_df(datawarehousedb, sql)

sql = """
SELECT * 
FROM kostenstelle
"""
df_target_kostenstelle = get_df(datawarehousedb, sql)

sql = """
SELECT * 
FROM mitarbeiter 
Where gueltig_bis is null
"""
df_target_mitarbeiter = get_df(datawarehousedb, sql)

sql = '\
SELECT * \
FROM projekt'
df_target_project = get_df(datawarehousedb, sql)

sql = '\
SELECT * \
FROM sap_anwendung'
df_target_sap = get_df(datawarehousedb, sql)

sql = '\
SELECT * \
FROM worklog'
df_target_worklog = get_df(datawarehousedb, sql)
#df_target_worklog["Mitarbeiter_ID"] = df_target_worklog["Mitarbeiter_ID"].astype(int)




######################################
# Transform Data
######################################

df_insert_mitarbeiter, df_update_mitarbeiter = get_df_diff(df_source_mitarbeiter, df_target_mitarbeiter,key_column="PNR", compare_columns=["Vorname", "Nachname", "Kostenstellen_ID", "Abteilungs_ID"])
df_insert_mitarbeiter["Gueltig_von"] = date.today().replace(day=1)
#Mitarbeiter Daten schonmal importieren, damit die Mitarbeiter in der Worklog Tabelle zugeordnet werden können

employee_import(datawarehousedb, df_update_mitarbeiter)
apply_df_diff(datawarehousedb, "mitarbeiter", df_insert_mitarbeiter)

sql = """
SELECT * \
FROM mitarbeiter
"""
df_current_mitarbeiter = get_df(datawarehousedb, sql)

#in den worklog die PNR mit den passenden Mitarbeiter_IDs ersetzen
df_source_worklog = assign_mitarbeiter_to_worklog(df_source_worklog, df_current_mitarbeiter)
#nach Mitarbeiter_Id zuordnung, muss die Reihenfolge der Spalten angepasst werden, damit sie mit der Ziel Tabelle übereinstimmt
df_source_worklog = df_source_worklog[df_target_worklog.columns]

df_insert_worklog, df_update_worklog = get_df_diff(df_source_worklog, df_target_worklog)
df_insert_abteilung, df_update_abteilung = get_df_diff(df_source_abteilung, df_target_abteilung)
df_insert_buchungstyp, df_update_buchungstyp = get_df_diff(df_source_buchungstyp, df_target_buchungstyp)
df_insert_jiraissue, df_update_jiraissue = get_df_diff(df_source_jiraissue, df_target_jiraissue)
df_insert_kostenstelle, df_update_kostenstelle = get_df_diff(df_source_kostenstelle, df_target_kostenstelle)
df_insert_project, df_update_project = get_df_diff(df_source_project, df_target_project)
df_insert_sap, df_update_sap = get_df_diff(df_source_sap, df_target_sap)


#####################################
# Load into Data Warehouse
#####################################

apply_df_diff(datawarehousedb, "projekt", df_insert_project, df_update_project)
apply_df_diff(datawarehousedb, "kostenstelle", df_insert_kostenstelle, df_update_kostenstelle)
apply_df_diff(datawarehousedb, "abteilung", df_insert_abteilung, df_update_abteilung)
apply_df_diff(datawarehousedb, "sap_anwendung", df_insert_sap, df_update_sap)
apply_df_diff(datawarehousedb, "buchungstyp", df_insert_buchungstyp, df_update_buchungstyp)
apply_df_diff(datawarehousedb, "jiraissue", df_insert_jiraissue, df_update_jiraissue)
apply_df_diff(datawarehousedb, "worklog", df_insert_worklog, df_update_worklog)