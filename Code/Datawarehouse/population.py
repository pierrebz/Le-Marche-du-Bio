import pandas as pd
pd.set_option('mode.chained_assignment', None) #ignore SettingWithCopyWarning
from pymongo import MongoClient
from io import StringIO
import os
from unidecode import unidecode

import CloudStorage
from Code.SetParameters import public_settings, private_settings

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
