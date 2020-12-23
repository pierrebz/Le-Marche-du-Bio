import pandas as pd
pd.set_option('mode.chained_assignment', None) #ignore SettingWithCopyWarning
from pymongo import MongoClient
from io import StringIO
from unidecode import unidecode

import CloudStorage
from Code.SetParameters import public_settings


####### Preparation des données
# on télécharge le fichier depuis GCP
file = CloudStorage.Cloud(key= public_settings.path_key_bucket,
                          bucket_name= public_settings.bucket).download_from_cloud_storage(file="mags_bio.csv")

dataset = pd.read_csv(StringIO(file.dataset), ",")

# imput des Nan
dataset["address"].fillna(dataset["vicinity"], inplace = True)

# on supprime la col vicinity
dataset.drop(columns= ["vicinity", "Unnamed: 0"], inplace= True)

# on crée un col commune
dataset["commune"] = dataset.address.split(",")[1].strip()

#communes = [dataset.address[i].split(",")[1] for i in range(dataset.shape[0])]
communes = []
idx = []
for i in range(dataset.shape[0]):
    try:
        communes.append(dataset.address[i].split(",")[1].strip())
    except IndexError:
        idx.append(i)
        #communes.append(dataset.address[i])
        continue