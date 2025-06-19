import mysql.connector


mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="cD!1HVF&Pxos3cjv"
)

def create_database(dbConnector, dbName):
  dbName = dbName.lower()
  mycursor = dbConnector.cursor()
  mycursor.execute("SHOW DATABASES")

  dbExist = bool(False)

  for x in mycursor:
    if(x[0] == dbName):
      dbExist = bool(True)

  if not dbExist:
    mycursor.execute(f"CREATE DATABASE {dbName}")

  mycursor.close

print("Erstelle Datenbanken...")
create_database(mydb, "jira")
create_database(mydb, "datawarehouse")
create_database(mydb, "DataMart")

mydb.close()

#Verbindungen zu den Datenbanken können erst hergestellt werden, 
mydb_Jira = mysql.connector.connect(
  host="localhost",
  user="root",
  password="cD!1HVF&Pxos3cjv",
  database="jira"
)

mydb_dw = mysql.connector.connect(
  host="localhost",
  user="root",
  password="cD!1HVF&Pxos3cjv",
  database="datawarehouse"
)

mydb_dm = mysql.connector.connect(
  host="localhost",
  user="root",
  password="cD!1HVF&Pxos3cjv",
  database="datamart"
)



###################
# Erstellung von SQL Befehle für die Datenbank Jira
###################

SQL_jira_project = """
    CREATE TABLE project (
        id numeric(18,0) PRIMARY KEY, 
        pname nvarchar(255), 
        url nvarchar(255), 
        `lead` nvarchar(255), 
        description text, 
        pkey nvarchar(255), 
        pcounter numeric(18,0), 
        assigneetype numeric(18,0), 
        avatar numeric(18,0), 
        originalkey nvarchar(255), 
        projecttype nvarchar(255)
    )
    """

SQL_jira_jiraissue = """
    CREATE TABLE jiraissue (
        id numeric(18,0) PRIMARY KEY, 
        pkey nvarchar(255), 
        issuenum numeric(18,0), 
        project numeric(18,0), 
        Reporter nvarchar(255), 
        assignee nvarchar(255), 
        creator nvarchar(255), 
        issuetype nvarchar(255), 
        summary nvarchar(255), 
        description text, 
        environment text, 
        priority nvarchar(255), 
        resolution nvarchar(255), 
        issuestatus nvarchar(255), 
        created datetime, 
        updated datetime, 
        duedate datetime, 
        resulutiondate datetime, 
        votes numeric(18,0), 
        watches numeric(18,0), 
        timeestimate numeric(18,0), 
        timeoriginalestimate numeric(18,0), 
        timespent numeric(18,0), 
        worklflow_id numeric(18,0), 
        security numeric(18,0), 
        fixfor numeric(18,0), 
        component numeric(18,0), 
        archievedby nvarchar(255), 
        archiveddate datetime, 
        archived nvarchar(255)
    )
    """

SQL_jira_customfield = """
    CREATE TABLE customfield (
        id numeric(18,0) PRIMARY KEY,
        customfieldtypekey nvarchar(255),
        CUSTOMFIELDSEARCHERKEY nvarchar(255),
        cfname nvarchar(255),
        DESCRIPTION text,
        defaultvalue nvarchar(255),
        FIELDTYPE numeric(18,0),
        PROJECT numeric(18,0),
        ISSUETYPE nvarchar(255),
        cfkey nvarchar(255),
        lastvalueupdate datetime,
        issueswithvalue numeric(18,0)
    )
    """

SQL_jira_customfieldvalue = """
    CREATE TABLE customfieldvalue (
        id numeric(18,0) PRIMARY KEY, 
        issue numeric(18,0), 
        customfield numeric(18,0), 
        parentkey nvarchar(255), 
        stringvalue nvarchar(255), 
        numbervalue numeric(18,0), 
        textvalue nvarchar(255),
        datevalue datetime, 
        valuetype nvarchar(255), 
        updated numeric(18,0)
    )
    """

SQL_jira_customfieldoption = """
    CREATE TABLE customfieldoption (
        id numeric(18,0) PRIMARY KEY,
        customfield numeric(18,0),
        customfieldconfig numeric(18,0),
        parentoptionid numeric(18,0),
        sequence numeric(18,0),
        customvalue nvarchar(255),
        optiontype nvarchar(255),
        disabled nvarchar(255)
    )
    """
    
SQL_jira_worklog = """
    CREATE TABLE worklog (
        id int PRIMARY KEY, 
        issueid int, 
        author nvarchar(255), 
        grouplevel nvarchar(255), 
        rolevel numeric(18,0), 
        worklogbody text, 
        created datetime, 
        updateauthor nvarchar(255),
        updated datetime,
        startdate datetime,
        timeworked int
    )
    """

SQL_jira_nodeassociation = """
    CREATE TABLE nodeassociation (
        source_node_id numeric(18,0), 
        soure_node_entity nvarchar(60), 
        sink_node_id numeric(18,0), 
        sind_node_entity nvarchar(60), 
        association_type varchar(60), 
        sequence int,
        PRIMARY KEY (source_node_id, soure_node_entity, sink_node_id, sind_node_entity, association_type)
    )
    """

SQL_jira_projectcategory = """
    CREATE TABLE projectcategory (
        id numeric(18,0) PRIMARY KEY,
        cname nvarchar(255),
        description text
    )
    """




###################
# Erstellung von SQL Befehle für die Datenbank DataWarehouse
###################

SQL_DW_projekt = """
    CREATE TABLE PROJEKT (
        Projekt_ID INT PRIMARY KEY,
        Projektname VARCHAR(255)
    )
"""

SQL_DW_kostenstelle = """
    CREATE TABLE kostenstelle (
        Kostenstellen_ID VARCHAR(6) PRIMARY KEY,
        Kostenstellenbezeichnung VARCHAR(255)
    )
    """

SQL_DW_abteilung = """
    CREATE TABLE abteilung (
        Abteilungs_ID VARCHAR(6) PRIMARY KEY,
        Abteilungsbezeichnung VARCHAR(255)
    )
    """

SQL_DW_mitarbeiter = """
    CREATE TABLE mitarbeiter (
        Mitarbeiter_ID INT AUTO_INCREMENT PRIMARY KEY,
        PNR VARCHAR(7),
        Vorname VARCHAR(255),
        Nachname VARCHAR(255),
        Gueltig_von DATETIME,
        Gueltig_bis DATETIME,
        Kostenstellen_ID VARCHAR(6),
        Abteilungs_ID VARCHAR(6),
        FOREIGN KEY (Kostenstellen_ID) REFERENCES KOSTENSTELLE(Kostenstellen_ID),
        FOREIGN KEY (Abteilungs_ID) REFERENCES ABTEILUNG(Abteilungs_ID)
    )
"""

SQL_DW_SAP_Anwendung = """
    CREATE TABLE SAP_Anwendung (
        SAP_Anwendungssystem_ID INT PRIMARY KEY,
        SAP_Anwendungssystem VARCHAR(255)
    )

"""

SQL_DW_Buchungstyp = """
    CREATE TABLE BUCHUNGSTYP (
        Buchungstyp_ID INT PRIMARY KEY,
        Buchungstyp VARCHAR(255)
    )
"""

SQL_DW_jiraissue = """
    CREATE TABLE JIRAISSUE (
        Issue_ID INT PRIMARY KEY,
        Projekt_ID INT,
        SAP_Anwendungssystem_ID INT,
        Buchungstyp_ID INT,
        FOREIGN KEY (Projekt_ID) REFERENCES PROJEKT(Projekt_ID),
        FOREIGN KEY (SAP_Anwendungssystem_ID) REFERENCES SAP_ANWENDUNG(SAP_Anwendungssystem_ID),
        FOREIGN KEY (Buchungstyp_ID) REFERENCES BUCHUNGSTYP(Buchungstyp_ID)
    )
"""

SQL_DW_worklog = """
    Create TABLE WORKLOG (
        Worklog_ID INT PRIMARY KEY,
        Issue_ID INT,
        Mitarbeiter_ID vARCHAR(7),
        Startzeitpunkt DATETIME,
        Gebuchte_Zeit INT,
        FOREIGN KEY (Issue_ID) REFERENCES JIRAISSUE(Issue_ID)
    )
"""

###################
# Erstellung von SQL Befehle für die Datenbank Datamart
###################

SQL_DM_DIM_Abteilung = """
    Create TABLE dim_abteilung (
        Abteilungs_ID VARCHAR(6) PRIMARY KEY,
        Abteilungsbezeichnung VARCHAR(255)
    )
"""
SQL_DM_DIM_Kostenstelle = """
    Create TABLE dim_kostenstelle (
        Kostenstellen_ID VARCHAR(6) PRIMARY KEY,
        Kostenstellenbezeichnung VARCHAR(255)
    )
"""

SQL_DM_DIM_Mitarbeiter = """
    Create TABLE dim_mitarbeiter (
        PNR VARCHAR(7) PRIMARY KEY,
        Vorname VARCHAR(255),
        Nachname VARCHAR(255)
    )
"""

SQL_DM_DIM_Projekt = """
    Create TABLE dim_projekt (
        Projekt_ID INT PRIMARY KEY,
        Projektname VARCHAR(255)
    )
"""

SQL_DM_DIM_Buchungstyp = """
    Create TABLE dim_buchungstyp (
        Buchungstyp_ID INT PRIMARY KEY,
        Buchungstyp VARCHAR(255)
    )
"""

SQL_DM_DIM_SAP_Anwendung = """
    Create TABLE dim_sap_anwendung (
        SAP_Anwendungssystem_ID INT PRIMARY KEY,
        SAP_Anwendungssystem VARCHAR(255)
    )
"""

SQL_DM_FACT_Buchungen = """
    Create TABLE fact_jira_buchungen (
        Zeitstempel DATETIME,
        PNR VARCHAR(7),
        Abteilungs_ID VARCHAR(6),
        Projekt_ID INT,
        Buchungstyp_ID INT,
        Kostenstellen_ID VARCHAR(6),
        SAP_Anwendungssystem_ID INT,
        Gebuchte_Zeit DECIMAL(10,1),
        PRIMARY KEY (Zeitstempel, PNR, Abteilungs_ID, Projekt_ID, Buchungstyp_ID, Kostenstellen_ID, SAP_Anwendungssystem_ID),
        FOREIGN KEY (PNR) REFERENCES DIM_MITARBEITER(PNR),
        FOREIGN KEY (Abteilungs_ID) REFERENCES DIM_ABTEILUNG(Abteilungs_ID),
        FOREIGN KEY (Projekt_ID) REFERENCES DIM_PROJEKT(Projekt_ID),
        FOREIGN KEY (Buchungstyp_ID) REFERENCES DIM_BUCHUNGSTYP(Buchungstyp_ID),
        FOREIGN KEY (Kostenstellen_ID) REFERENCES DIM_KOSTENSTELLE(Kostenstellen_ID),
        FOREIGN KEY (SAP_Anwendungssystem_ID) REFERENCES DIM_SAP_ANWENDUNG(SAP_Anwendungssystem_ID)
    )
"""

SQL_DM_FACT_Projektzeiten = """
    Create TABLE fact_buchungen (
        Zeitstempel DATETIME,
        PNR VARCHAR(7),
        Abteilungs_ID VARCHAR(6),
        Projekt_ID INT,
        Gebuchte_Zeit DECIMAL(10,1),
        PRIMARY KEY (Zeitstempel, PNR, Abteilungs_ID, Projekt_ID),
        FOREIGN KEY (PNR) REFERENCES DIM_MITARBEITER(PNR),
        FOREIGN KEY (Abteilungs_ID) REFERENCES DIM_ABTEILUNG(Abteilungs_ID),
        FOREIGN KEY (Projekt_ID) REFERENCES DIM_PROJEKT(Projekt_ID)
    )
"""




#######################
# Ausführung
#######################

print("Erstelle Tabellen in Jira Datenbank.")


mycursor = mydb_Jira.cursor()

mycursor.execute(SQL_jira_project)
mycursor.execute(SQL_jira_jiraissue)
mycursor.execute(SQL_jira_customfield)
mycursor.execute(SQL_jira_customfieldvalue)
mycursor.execute(SQL_jira_customfieldoption)
mycursor.execute(SQL_jira_worklog)
mycursor.execute(SQL_jira_nodeassociation)
mycursor.execute(SQL_jira_projectcategory)

mycursor.close

print("Erstelle Tabellen in DataWarehouse Datenbank.")

mycursor = mydb_dw.cursor()

mycursor.execute(SQL_DW_projekt)
mycursor.execute(SQL_DW_kostenstelle)
mycursor.execute(SQL_DW_abteilung)
mycursor.execute(SQL_DW_mitarbeiter)
mycursor.execute(SQL_DW_SAP_Anwendung)
mycursor.execute(SQL_DW_Buchungstyp)
mycursor.execute(SQL_DW_jiraissue)
mycursor.execute(SQL_DW_worklog)

mycursor.close

print("Erstelle Tabellen in DataMart Datenbank.")

mycursor = mydb_dm.cursor()

mycursor.execute(SQL_DM_DIM_Abteilung)
mycursor.execute(SQL_DM_DIM_Kostenstelle)
mycursor.execute(SQL_DM_DIM_Mitarbeiter)
mycursor.execute(SQL_DM_DIM_Projekt)
mycursor.execute(SQL_DM_DIM_Buchungstyp)
mycursor.execute(SQL_DM_DIM_SAP_Anwendung)
mycursor.execute(SQL_DM_FACT_Buchungen)
mycursor.execute(SQL_DM_FACT_Projektzeiten)

mycursor.close


################
# schließen der Datenbankverbindungen
#######################
mydb_Jira.close()
mydb_dw.close()
mydb_dm.close()
