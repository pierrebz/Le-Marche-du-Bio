import pandas as pd
pd.set_option('mode.chained_assignment', None) #ignore SettingWithCopyWarning
from pymongo import MongoClient

import tram_departements
from Code.SetParameters import public_settings, private_settings

##############  Préparation des données
# importation des données
data = tram_departements.cleaning_dataset(filename="communes-departement-region.csv",
                                             my_agent= private_settings.my_agent)
data.rename(columns= {"code_departement": "_id"}, inplace= True)

# liste des regions
region_list = data.nom_region.unique()

# liste des departements
departement_list = data.nom_departement.unique()

# grouper les communes par département
departement_dict = {a: data.groupby("nom_departement").get_group(a).nom_commune_complet.unique().tolist()
                    for a in departement_list}

# grouper les département par region
region_grp = data.groupby("nom_region")
region_dict = {a: {"nom_region": a,
               "id_region": region_grp.get_group(a).code_region.unique().tolist()[0],
               "departements": region_grp.get_group(a).nom_departement.unique().tolist()} for a in region_list}

data_mongo = data[["_id", "nom_departement", "nom_region"]].drop_duplicates(ignore_index= True)
data_mongo.rename(columns= {"nom_departement": "departement", "nom_region": "region"}, inplace= True)


############## Insert MongoDB
client = MongoClient("localhost", public_settings.port_DWH)
db = client["BIO"] # database
collection = db["departements"] #collection

records = data_mongo.to_dict(orient='records')
collection.insert_many(records)


############## update database
# ajoute la variable commune
for row in data_mongo.departement:
    db.departements.update_one({"departement": row}, {"$set": {"communes": departement_dict[row]}})

# maj de la variable region
for row in data_mongo.region:
    db.departements.update_many({"region":row}, {"$set": {"region": region_dict[row]}})