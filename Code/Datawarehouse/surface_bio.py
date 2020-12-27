import pandas as pd
pd.set_option('mode.chained_assignment', None) #ignore SettingWithCopyWarning
from pymongo import MongoClient
from io import StringIO
from unidecode import  unidecode

import CloudStorage
from Code.SetParameters import public_settings, private_settings


####### Preparation des données
# on télécharge le fichier depuis GCP
dataset_surface = pd.read_csv(StringIO(CloudStorage.Cloud(key= public_settings.path_key_bucket,
                          bucket_name= public_settings.bucket).download_from_cloud_storage(file="surface_agri/Donnees_Surfaces_Dept_depuis2011_AgenceBio.csv").dataset),
                              ",")

dataset_cheptels = pd.read_csv(StringIO(CloudStorage.Cloud(key= public_settings.path_key_bucket,
                          bucket_name= public_settings.bucket).download_from_cloud_storage(file="surface_agri/Donnees_Cheptels_Dept_depuis2011_AgenceBio.csv").dataset),
                              ",")

dataset_surface_2 = pd.read_csv(StringIO(CloudStorage.Cloud(key= public_settings.path_key_bucket,
                          bucket_name= public_settings.bucket).download_from_cloud_storage(file="surface_agri/surface_bio_2008_2011.csv").dataset),
                              sep= ",")