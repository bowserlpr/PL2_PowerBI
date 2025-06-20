import pandas as pd

Source_Projekte = {
  "ID": ["1", "2", "2"],
  "Monat": [6, 6, 7],
  "data": ["a", "b", "c"],
}
Source_Projekte = pd.DataFrame(Source_Projekte)

print ("Source Projekte:")
print (Source_Projekte)

Monate = {6, 7, 8}

print (f"Monate: {Monate}")

Projekte = Source_Projekte["ID"].unique()

print (f"Projekte: {Projekte}")

projects_months = pd.MultiIndex.from_product(
    [Monate, Projekte],
    names=['Monat', 'ID']
).to_frame(index=False)

print(f"Projekte zu den Monaten: \n{projects_months}")

missing_projects_months = pd.merge(
    projects_months,
    Source_Projekte,
    on=['Monat', 'ID'],
    how='left',
    indicator=True
)
print(f"Fehlende Projekte zu den Monaten: \n{missing_projects_months}")

missing_projects_months = missing_projects_months.query("_merge == 'left_only'").drop(columns="_merge")

print(f"Fehlende Projekte zu den Monaten: \n{missing_projects_months}")

Source_Projekte = pd.concat([Source_Projekte, missing_projects_months], ignore_index=True)

print(f"Source_Projekte nach dem Hinzufügen der fehlenden Projekte: \n{Source_Projekte}")

data2 = {
  "name": ["Sally", "Peter", "Micky"],
  "age": [77, 44, 22]
}



#newdf = df1.merge(df2, how='right')

#print(newdf)
