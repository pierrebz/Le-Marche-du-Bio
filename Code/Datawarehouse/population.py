import pandas as pd
pd.set_option('mode.chained_assignment', None) #ignore SettingWithCopyWarning
from pymongo import MongoClient
from io import StringIO
from unidecode import unidecode

import CloudStorage
from Code.SetParameters import public_settings

####### Preparation des données
# list des fichiers
list_file = CloudStorage.Cloud(public_settings.path_key_bucket, public_settings.bucket).list_files_blod("population/")
filename = [a.split("/")[1].split(".")[0] for a in list_file]

# créer un dataframe par fichier
list_dataset = []
for idx, row in enumerate(list_file):
    dataset = pd.read_csv(StringIO(CloudStorage.Cloud(key= public_settings.path_key_bucket,
                          bucket_name= public_settings.bucket).download_from_cloud_storage(file= row).dataset), ";")
    # ajoute la col année
    dataset["Annee"] = int(filename[idx].split("_")[1])

    list_dataset.append(dataset)

all_dataset = pd.concat(list_dataset, ignore_index=True)

# sup les lignes France métro et Dom
all_dataset.dropna(inplace= True)

# regl encodage
all_dataset.Departement = all_dataset.Departement.apply(unidecode)
all_dataset.Departement = all_dataset.Departement.str.upper()

# on enleve l espace blanc pour les valeur numeriques
for col in range(2, len(all_dataset.columns) - 1):
    all_dataset.iloc[:, col] = all_dataset.iloc[:, col].str.replace(" ", "")

#
population = {}
for i in all_dataset["Departement"].unique():
    population[i] = [{"Annee": all_dataset["Annee"][j].item(),
                      "Age_0_19": {"Hommes": int(all_dataset["Hommes 0 à 19 ans"][j]), "Femmes": int(all_dataset["Femmes 0 à 19 ans"][j])},
                      "Age_20_39": {"Hommes": int(all_dataset["Hommes 20 à 39 ans"][j]), "Femmes": int(all_dataset["Femmes 20 à 39 ans"][j])},
                      "Age_40_59": {"Hommes": int(all_dataset["Hommes 40 à 59 ans"][j]), "Femmes": int(all_dataset["Femmes 40 à 59 ans"][j])},
                      "Age_60_74": {"Hommes": int(all_dataset["Hommes 60 à 74 ans"][j]), "Femmes": int(all_dataset["Femmes 60 à 74 ans"][j])},
                      "Age_75_plus": {"Hommes": int(all_dataset["Hommes 75 ans et plus"][j]), "Femmes": int(all_dataset["Femmes 75 ans et plus"][j])}}
            for j in all_dataset[all_dataset["Departement"] == i].index]


######## ingest dans mongoDB
client = MongoClient("localhost", public_settings.port_DWH)
db = client["BIO"] # database
collection = db["departements"] #collection

# ajoute la variable commune
for row in population.keys():
    db.departements.update_one({"departement": row}, {"$set": {"population": population[row]}})