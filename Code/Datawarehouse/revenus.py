import pandas as pd
pd.set_option('mode.chained_assignment', None) #ignore SettingWithCopyWarning
from pymongo import MongoClient
from io import StringIO
from unidecode import unidecode

import CloudStorage
from Code.SetParameters import public_settings

####### Preparation des données
# list des fichiers
list_file = CloudStorage.Cloud(public_settings.path_key_bucket, public_settings.bucket).list_files_blod("impots/")
filename = [a.split("/")[1].split(".")[0] for a in list_file]

# créer un dataframe par fichier
list_dataset = []
for idx, row in enumerate(list_file):
    dataset = pd.read_csv(StringIO(CloudStorage.Cloud(key= public_settings.path_key_bucket,
                          bucket_name= public_settings.bucket).download_from_cloud_storage(file= row).dataset), ";")
    # ajoute la col année
    dataset["Annee"] = int(filename[idx].split("_")[-1][-4:]) #nt(filename[idx].split("_")[1])

    list_dataset.append(dataset)

all_dataset = pd.concat(list_dataset, ignore_index=True)

# sup les lignes France métro et Dom
all_dataset.dropna(inplace= True)

# regl encodage
all_dataset.Departement = all_dataset.Departement.apply(unidecode)
all_dataset.Departement = all_dataset.Departement.str.upper()

# selection des colonnes Departement, Revenu fiscal de référence par tranche (en euros) et Nombre de foyers fiscaux
all_dataset_ss = all_dataset[["Departement", "Revenu fiscal de référence par tranche (en euros)",
                              "Nombre de foyers fiscaux", "Annee"]]

# on fait pivoter pour avoir les tranches de revenus en colonne
all_dataset_ss_pivot = all_dataset_ss.pivot(columns = "Revenu fiscal de référence par tranche (en euros)",
                                            index= ["Departement", "Annee"])
# on change le nom des colonnes
all_dataset_ss_pivot.columns = pd.Index([a[0] + ' ' + a[1] for a in all_dataset_ss_pivot.columns.tolist()])

# on fait flatten des index
all_dataset_ss_pivot.reset_index(inplace= True)

revenus = {}
for departement in all_dataset_ss_pivot.Departement.unique():
    revenus[departement] = [{"Annee": all_dataset_ss_pivot["Annee"][j].item(),
                             "Revenus_0_10K": all_dataset_ss_pivot["Nombre de foyers fiscaux 0 à 10 000"][j],
                            "Revenus_10k_12k": all_dataset_ss_pivot["Nombre de foyers fiscaux 10 001 à 12 000"][j],
                             "Revenus_12k_15k": all_dataset_ss_pivot["Nombre de foyers fiscaux 12 001 à 15 000"][j],
                             "Revenus_15k_20k": all_dataset_ss_pivot["Nombre de foyers fiscaux 15 001 à 20 000"][j],
                             "Revenus_20k_30k": all_dataset_ss_pivot["Nombre de foyers fiscaux 20 001 à 30 000"][j],
                             "Revenus_30k_50k": all_dataset_ss_pivot["Nombre de foyers fiscaux 30 001 à 50 000"][j],
                             "Revenus_50k_100k": all_dataset_ss_pivot["Nombre de foyers fiscaux 50 001 à 100 000"][j],
                             "Revenus_100k_plus": all_dataset_ss_pivot["Nombre de foyers fiscaux + de 100 000"][j]}
                            for j in all_dataset_ss_pivot[all_dataset_ss_pivot["Departement"] == departement].index]

# on supprime les valeurs NaN
for depart in revenus.keys():
    for year in revenus[depart]:
        for tranche in ["Revenus_0_10K", "Revenus_10k_12k", "Revenus_12k_15k",
                           "Revenus_15k_20k", "Revenus_20k_30k", "Revenus_30k_50k",
                        "Revenus_50k_100k", "Revenus_100k_plus"]:
            try:
                year[tranche] = int(year[tranche])
            except ValueError:
                del year[tranche]
                continue

######## ingest dans mongoDB
client = MongoClient("localhost", public_settings.port_DWH)
db = client["BIO"] # database
collection = db["departements"] #collection

# ajoute la variable commune
for row in revenus.keys():
    db.departements.update_one({"departement": row}, {"$set": {"revenus": revenus[row]}})