import os
import pandas as pd

# Basisverzeichnis ermitteln
base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # z. B. von /PL2_PowerBI/Code/ zu /PL2_PowerBI

# Pfad zusammensetzen
csv_path = os.path.join(base_path, 'Data', 'jiraissue.csv')

# Einlesen
#df = pd.read_csv(csv_path, sep=';', quotechar='\'', encoding='utf8')

print(csv_path)