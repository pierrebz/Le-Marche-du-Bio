import pandas as pd
pd.set_option('mode.chained_assignment', None) #ignore SettingWithCopyWarning
from pymongo import MongoClient
from io import StringIO
from unidecode import  unidecode

import CloudStorage
from Code.SetParameters import public_settings, private_settings


####### Preparation des données
# on télécharge le fichier depuis GCP
file = CloudStorage.Cloud(key= public_settings.path_key_bucket,
                          bucket_name= public_settings.bucket).download_from_cloud_storage(file="operateursEngagesEnAgriculture.csv")

dataset = pd.read_csv(StringIO(file.dataset), ";")

# mettre en upper et supprimer les accents les nom de département et région
dataset.Region = dataset.Region.apply(unidecode)
dataset.Region = dataset.Region.str.upper()

dataset.Departement = dataset.Departement.apply(unidecode)
dataset.Departement = dataset.Departement.str.upper()


# imbrication des données
list_departement = dataset.Departement.unique()
dataset_group = dataset.groupby(["Annee", "Departement"])

dict_annee = {departement: dataset.groupby(["Departement"]).get_group(departement).Annee.values.tolist()
              for departement in list_departement}

dict_operateurs = {departement: [{"operateurs_"+str(annee) : {"Annee": annee,
                                                             "Distributeurs": dataset_group.get_group((annee, departement)).Distributeurs.values.item(0),
                                                             "Importateurs": dataset_group.get_group((annee, departement)).Importateurs.values.item(0),
                                                             "Producteurs": dataset_group.get_group((annee, departement)).Producteurs.values.item(0),
                                                             "Preparateurs": dataset_group.get_group((annee, departement)).Producteurs.values.item(0)}}
                                 for annee in dict_annee[departement]] for departement in list_departement}

######
for depart in dict_operateurs.keys():
    for year in dict_operateurs[depart]:
        for row in year.values():
            for operateurs in ["Distributeurs", "Importateurs", "Producteurs", "Preparateurs"]:
                try:
                    row[operateurs] = int(row[operateurs])
                except ValueError:
                    del row[operateurs]
                continue

######## ingest dans mongoDB
client = MongoClient("localhost", public_settings.port_DWH)
db = client["BIO"] # database
collection = db["departements"] #collection

# ajoute la variable commune
for row in dict_operateurs.keys():
    for field in dict_operateurs[row]:
        db.departements.update_one({"departement": row}, {"$set": {list(field.keys())[0]: list(field.values())[0]}})




