from sqlalchemy import create_engine
import pandas as pd
import os
import mysql.connector

myJiradb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="cD!1HVF&Pxos3cjv",
  database = "jira"
)

#Basisverzeichnis ermitteln
path_base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Pfad zusammensetzen
path_jiraissue = os.path.join(path_base, 'Data', 'jiraissue.csv')
path_worklog = os.path.join(path_base, 'Data', 'worklog.csv')
path_customfield = os.path.join(path_base, 'Data', 'customfield.csv')
path_customfieldoption = os.path.join(path_base, 'Data', 'customfieldoption.csv')
path_customfieldvalue = os.path.join(path_base, 'Data', 'customfieldvalue.csv')
path_nodeassociation = os.path.join(path_base, 'Data', 'nodeassociation.csv')
path_project = os.path.join(path_base, 'Data', 'project.csv')
path_projectcategory = os.path.join(path_base, 'Data', 'projectcategory.csv')

#Funktionen Definieren

def convert_datetime(value):
    if pd.isna(value):
        return value
    if '-' in value:
        value = value.split('-')[2].split(' ')[0] + '/' + value.split('-')[1] + '/' + value.split('-')[0] + ' ' + value.split(' ')[1].split(':')[0] + ':' + value.split(' ')[1].split(':')[1]
        
    try:
        return pd.to_datetime(value, dayfirst=True, errors='raise')
    except Exception as e:
        print(f"Fehler bei '{value}': {e}")
        return pd.NaT
    
#von CSV Extrahieren und Datenformat direkt anpassen
print("jiraisseue")
df_source_jiraissue = pd.read_csv(path_jiraissue,sep=';',quotechar='\'',encoding='utf8')
df_source_jiraissue['created'] = df_source_jiraissue['created'].apply(convert_datetime)
df_source_jiraissue['updated'] = df_source_jiraissue['updated'].apply(convert_datetime)
df_source_jiraissue['duedate'] = df_source_jiraissue['duedate'].apply(convert_datetime)
df_source_jiraissue['resulutiondate'] = df_source_jiraissue['resulutiondate'].apply(convert_datetime)

print("worklog")
df_source_worklog = pd.read_csv(path_worklog,sep=';',quotechar='\"',encoding='utf8')
df_source_worklog['created'] = df_source_worklog['created'].apply(convert_datetime)
df_source_worklog['updated'] = df_source_worklog['updated'].apply(convert_datetime)
df_source_worklog['startdate'] = df_source_worklog['startdate'].apply(convert_datetime)

print("customfield")
df_source_customfield = pd.read_csv(path_customfield,sep=';',quotechar='\'',encoding='utf8')
df_source_customfield['lastvalueupdate'] = df_source_customfield['lastvalueupdate'].apply(convert_datetime)

df_source_customfieldoption = pd.read_csv(path_customfieldoption,sep=';',quotechar='\"',encoding='utf8')

df_source_customfieldvalue = pd.read_csv(path_customfieldvalue,sep=';',quotechar='\"',encoding='utf8')
df_source_customfieldvalue['datevalue'] = df_source_customfieldvalue['datevalue'].apply(convert_datetime)

df_source_project = pd.read_csv(path_project,sep=';',quotechar='\"',encoding='utf8')

df_source_nodeassociation = pd.read_csv(path_nodeassociation,sep=';',quotechar='\"',encoding='utf8')

df_source_projectcategory = pd.read_csv(path_projectcategory,sep=';',quotechar='\"',encoding='utf8')

#########################
# Tabellen leeren
#########################

jiraCursor = myJiradb.cursor()

jiraCursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
jiraCursor.execute("SHOW TABLES;")
JiraTables = jiraCursor.fetchall()

for (table_name,) in JiraTables:
    print(f"Leere Tabelle: {table_name}")
    jiraCursor.execute(f"TRUNCATE TABLE `{table_name}`;")

jiraCursor.execute("SET FOREIGN_KEY_CHECKS = 1;")

myJiradb.commit()


#Daten importieren
engine = create_engine('mysql://root:cD!1HVF&Pxos3cjv@localhost/jira')


df_source_project.to_sql('project',con=engine,index=False,if_exists='append')
df_source_jiraissue.to_sql('jiraissue',con=engine,index=False,if_exists='append')
df_source_customfield.to_sql('customfield',con=engine,index=False,if_exists='append')
df_source_customfieldoption.to_sql('customfieldoption',con=engine,index=False,if_exists='append')
df_source_customfieldvalue.to_sql('customfieldvalue',con=engine,index=False,if_exists='append')
df_source_worklog.to_sql('worklog',con=engine,index=False,if_exists='append')
df_source_nodeassociation.to_sql('nodeassociation',con=engine,index=False,if_exists='append')
df_source_projectcategory.to_sql('projectcategory',con=engine,index=False,if_exists='append')