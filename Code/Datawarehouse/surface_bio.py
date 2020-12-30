import pandas as pd
pd.set_option('mode.chained_assignment', None) #ignore SettingWithCopyWarning
from pymongo import MongoClient
from io import StringIO
from unidecode import  unidecode

import CloudStorage
from Code.SetParameters import public_settings, private_settings


####### Preparation des données
# on télécharge le fichier depuis GCP
dataset_surface_1 = pd.read_csv(StringIO(CloudStorage.Cloud(key= public_settings.path_key_bucket,
                          bucket_name= public_settings.bucket).download_from_cloud_storage(file="surface_agri/Donnees_Surfaces_Dept_depuis2011_AgenceBio.csv").dataset),
                              ",")

dataset_cheptels = pd.read_csv(StringIO(CloudStorage.Cloud(key= public_settings.path_key_bucket,
                          bucket_name= public_settings.bucket).download_from_cloud_storage(file="surface_agri/Donnees_Cheptels_Dept_depuis2011_AgenceBio.csv").dataset),
                              ",")

dataset_surface_2 = pd.read_csv(StringIO(CloudStorage.Cloud(key= public_settings.path_key_bucket,
                          bucket_name= public_settings.bucket).download_from_cloud_storage(file="surface_agri/surface_bio_2008_2011.csv").dataset),
                              sep= ",")

#### Pré-traitement surface
# surface renommer les colonnes
dataset_surface_1.rename(columns= {"libelle_Region": "Region", "Numero_Dept": "id_departement",
                                   "Libelle_Dept": "Departement", "SurfBio": "Surfaces certifiées bio",
                                   "SurfC1": "Surfaces C1", "SurfC2": "Surfaces C2", "SurfC3": "Surfaces C3",
                                   "Libelle": "Cultures"}, inplace= True)

# selection des colonnes
col_surface = ["Annee", "Departement", "Cultures", "Surfaces certifiées bio",
               "Surfaces C1", "Surfaces C2", "Surfaces C3"]
dataset_surface_1 = dataset_surface_1[col_surface]
dataset_surface_2 = dataset_surface_2[col_surface]

# concat les 2 dataframes
dataset_surface = pd.concat([dataset_surface_2, dataset_surface_1], ignore_index= True)

# mettre en upper et supprimer les accents les nom de département et région
dataset_surface.Departement = dataset_surface.Departement.apply(unidecode)
dataset_surface.Departement = dataset_surface.Departement.str.upper()

dataset_surface.Cultures = dataset_surface.Cultures.apply(unidecode)
dataset_surface.Cultures = dataset_surface.Cultures.str.upper()


def replace_specific(columns):
    """
    Fonction qui remplace les "," par un "."
    :param columns:
    :return:
    """
    for col in columns:
        dataset_surface[col].replace({",": "."}, regex=True, inplace=True)
        dataset_surface[col].replace({r"[a-z]": ""}, regex=True, inplace=True)
        dataset_surface[col] = pd.to_numeric(dataset_surface[col], downcast='float')


replace_specific(dataset_surface.columns[-4:])

# on supprime les doublons
dataset_surface.drop_duplicates(inplace= True, ignore_index= True, subset= ["Annee",
                                                                            "Departement",
                                                                            "Cultures"])

# rotation des colonnes
dataset_surface_Bio = dataset_surface[["Annee", "Departement", "Cultures", "Surfaces certifiées bio"]]
dataset_surface_C1 = dataset_surface[["Annee", "Departement", "Cultures", "Surfaces C1"]]
dataset_surface_C2 = dataset_surface[["Annee", "Departement", "Cultures", "Surfaces C2"]]
dataset_surface_C3 = dataset_surface[["Annee", "Departement", "Cultures", "Surfaces C3"]]

# on fait pivoter pour avoir les tranches de revenus en colonne
dataset_surface_Bio_pivot = dataset_surface_Bio.pivot(columns = "Cultures", values= "Surfaces certifiées bio",
                                            index= ["Departement", "Annee"])
dataset_surface_C1_pivot = dataset_surface_C1.pivot(columns = "Cultures", values= "Surfaces C1",
                                            index= ["Departement", "Annee"])
dataset_surface_C2_pivot = dataset_surface_C2.pivot(columns = "Cultures", values= "Surfaces C2",
                                            index= ["Departement", "Annee"])
dataset_surface_C3_pivot = dataset_surface_C3.pivot(columns = "Cultures", values= "Surfaces C3",
                                            index= ["Departement", "Annee"])

# reset index
dataset_surface_Bio_pivot.reset_index(inplace= True)
dataset_surface_C1_pivot.reset_index(inplace= True)
dataset_surface_C2_pivot.reset_index(inplace= True)
dataset_surface_C3_pivot.reset_index(inplace= True)

# des tests ont permis de vérifier la correspondance entre le nom des colonnes. contenu colonne Annee, Departement

surface = {}
for departement in dataset_surface.Departement.unique():
    surface[departement] = [{"Annee": dataset_surface_Bio_pivot["Annee"][j].item(),
                             "Surface BIO": {nom: dataset_surface_Bio_pivot[nom][j].item() for nom in dataset_surface_Bio_pivot.columns[2:]},
                             "Surface C1": {nom: dataset_surface_C1_pivot[nom][j].item() for nom in dataset_surface_C1_pivot.columns[2:]},
                             "Surface C2": {nom: dataset_surface_C2_pivot[nom][j].item() for nom in dataset_surface_C2_pivot.columns[2:]},
                             "Surface C3": {nom: dataset_surface_C3_pivot[nom][j].item() for nom in dataset_surface_C3_pivot.columns[2:]}}
                            for j in dataset_surface_Bio_pivot[dataset_surface_Bio_pivot["Departement"] == departement].index]


# on supprime les valeurs NaN
for depart in surface.keys():
    for year in surface[depart]:
        for type_surface in [*year.keys()][1:]:
            for cult in dataset_surface_Bio_pivot.columns[2:]:
                if pd.isnull(year[type_surface][cult]):
                    del year[type_surface][cult]


############## Insert MongoDB
client = MongoClient("localhost", public_settings.port_DWH)
db = client["BIO"] # database
collection = db["departements"] #collection


# ajoute la variable commune
for row in surface.keys():
    db.departements.update_one({"departement": row}, {"$set": {"surfaces": surface[row]}})