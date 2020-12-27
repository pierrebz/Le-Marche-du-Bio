import pandas as pd
pd.set_option('mode.chained_assignment', None) #ignore SettingWithCopyWarning
from pymongo import MongoClient
from io import StringIO
from unidecode import unidecode
from geopy.geocoders import Nominatim
import time
import requests
import json

import CloudStorage
from Code.SetParameters import public_settings, private_settings
import tram_departements

####### Preparation des données
# on télécharge le fichier depuis GCP
file = CloudStorage.Cloud(key= public_settings.path_key_bucket,
                          bucket_name= public_settings.bucket).download_from_cloud_storage(file="mags_bio.csv")

dataset = pd.read_csv(StringIO(file.dataset), ",")

#####
# Afin d'effectuer la correspondance lat/long -> code postal, on peut passer par l'API de Google ou tout autre API
# L'inconvénient est la limitation de leur utilisation. Pour cette raison nous allons passer par une solution proposée
# par https://github.com/etalab/addok-docker
# Ce dernier nécessite l'utilisation d'un container docker.
# L'utilisation de cette solution sera détaillée en commentaire ci-dessous, mais l'exécution du code peut se faire sans
# l'installation de ce container.
#
#
#def etalab(df):
#    """
#       Fonction qui retourne un code postal en fonction des coordonnées gps (lat,long)
#    :param df:
#    :return:
#    """
#    r = requests.get("http://localhost:7878/reverse/?lon={}&lat={}".format(df.longitude,
#                                                                           df.latitude))
#
#    response = r.text
#    python_object = json.loads(response)
#
#    try:
#        return python_object["features"][0]["properties"]["postcode"]
#    except KeyError:
#        return None
#    except IndexError:
#        return None
#    except ValueError:
#        return None
#
#
#zipcode = pd.DataFrame(dataset.apply(lambda x: etalab(x), axis= 1), columns= ["CodePostal"])
#zipcode.to_csv("../../Data/zipcode_magbio/codepostal_mag.csv", sep= ";", index= False)
#
# ingest cloudstorage
#CloudStorage.Cloud(public_settings.path_key_bucket,
#                   public_settings.bucket).ingest_into_cloud_storage(path_file= "../../Data",
#                                                                     file= "zipcode_magbio/codepostal_mag.csv")
#

# on récupère les codes postaux depuis le blod du bucker
zipcode = pd.read_csv(StringIO(CloudStorage.Cloud(key= public_settings.path_key_bucket,
                          bucket_name= public_settings.bucket).download_from_cloud_storage(file="zipcode_magbio/codepostal_mag.csv").dataset), ";")

# concat zipcode and dataset
dataset_all = pd.concat([dataset, zipcode], axis= 1)

# imput des Nan
dataset_all["address"].fillna(dataset_all["vicinity"], inplace = True)

# on supprime la col vicinity
dataset_all.drop(columns= ["vicinity", "Unnamed: 0"], inplace= True)

# mettre en upper et supprimer les accents les nom de département et région
dataset_all.name = dataset_all.name.apply(unidecode)
dataset_all.name = dataset_all.name.str.upper()

dataset_all.address = dataset_all.address.apply(unidecode)
dataset_all.address = dataset_all.address.str.upper()


def set_commune(df):
    """
    Fonction qui recupère le nom des communes
    :param df:
    :return:
    """
    return df.address.split(",")[-1].strip()


# On récupère le nom des communes
dataset_all["communes"] = dataset_all.apply(lambda x: set_commune(x), axis=1)


# liste des enseignes bio en France
main_ens = ["BIO C' BON", "BIOMONDE", "NATURALIA", "BIOCOOP", "L'EAU VIVE", "LE GRAND PANIER BIO",
            "LA VIE CLAIRE", "NATUREO"]


def set_name(name):
    """
    Fonction qui retourne le nom du magasin BIO
    :param name:
    :return:
    """
    for mag in main_ens:
        if mag in name:
            ens = mag

    try:
        return ens
        #del ens
    except NameError:
        if "BIO COOP" in name:
            return "BIOCOOP"
        elif "BIO C BON" in name:
            return "BIO C' BON"
        elif "LES NOUVEAUX ROBINSON" in name:
            return "LES NOUVEAUX ROBINSON"
        else:
            return name

dataset_all["NomMag"] = dataset_all.apply(lambda x: set_name(x["name"]), axis= 1)


# On utilise le fichier commmun....csv afin d'obtenir les codes postaux par département.
data_departement = pd.read_csv(StringIO(CloudStorage.Cloud(key= public_settings.path_key_bucket,
                          bucket_name= public_settings.bucket).download_from_cloud_storage(file="communes-departement-region.csv").dataset), ",")

data_departement = data_departement[["nom_departement", "code_postal", "nom_commune_complet"]]
data_departement.dropna(inplace= True)
data_departement.reset_index(drop=True, inplace=True)

# mettre en upper et supprimer les accents les nom de département et région
data_departement.nom_departement = data_departement.nom_departement.apply(unidecode)
data_departement.nom_departement = data_departement.nom_departement.str.upper()

data_departement.nom_commune_complet = data_departement.nom_commune_complet.apply(unidecode)
data_departement.nom_commune_complet = data_departement.nom_commune_complet.str.upper()

# liste des departements
departement_list = data_departement.nom_departement.unique()

# grouper les communes par département
departement_dict = {a: data_departement.groupby("nom_departement").get_group(a).code_postal.unique().tolist()
                    for a in departement_list}


def set_departement(zip):
    """
    Fonction qui convertit les codes postaux en nom de département
    :param zip:
    :return:
    """
    for i in departement_dict.keys():
        if zip in departement_dict[i]:
            nom_depart = i

    try:
        return nom_depart
    except NameError:
        return None


# convertir les codes postaux en nom de département.
dataset_all["Departement"] = dataset_all.apply(lambda x: set_departement(x.CodePostal), axis= 1)

# set departement name pour les valeurs None
def set_none_departement(departement, communes):
    """
    Fonction qui retourne le nom du département en fonction des données du dataframe data_departement
    pour les 155 valeurs manquantes restantes
    :param departement:
    :param communes:
    :return:
    """
    if departement is None:
        try:
            return data_departement[data_departement.nom_commune_complet == communes]["nom_departement"].values[0]
        except IndexError:
            return None
    else:
        return departement

dataset_all["Departement"] = dataset_all.apply(lambda x: set_none_departement(x.Departement, x.communes), axis= 1)

# on supprime les valeurs None restantes car correspond à des pays autre que la france.
dataset_all = dataset_all[dataset_all.Departement.notna()]
dataset_all.reset_index(drop=True, inplace=True)

# on peut supprimer les colonnes :
dataset_all.drop(columns= ["latitude", "longitude", "rating", "address", "name", "CodePostal", "communes"],
                 inplace= True)

# on supprime les mags Monoprix
dataset_all = dataset_all[dataset_all.NomMag.str.contains(r"MONOP") == False]
# on supprime les mags Carrefour
dataset_all = dataset_all[dataset_all.NomMag.str.contains(r"CARREF") == False]
# on supprime les boulangeries
dataset_all = dataset_all[dataset_all.NomMag.str.contains(r"BOULANGERIE") == False]
# on supprime les LECLERC
dataset_all = dataset_all[dataset_all.NomMag.str.contains(r"LECLERC") == False]
# on supprime les PICARD
dataset_all = dataset_all[dataset_all.NomMag.str.contains(r"PICARD") == False]
# on supprime les LABORATOIRE
dataset_all = dataset_all[dataset_all.NomMag.str.contains(r"LABORATOIRE") == False]
# on supprime FRANPRIX
dataset_all = dataset_all[dataset_all.NomMag.str.contains(r"FRANPRIX") == False]
#
dataset_all = dataset_all[dataset_all.NomMag.str.contains(r"NATURE ET DECOUVERTES") == False]
dataset_all = dataset_all[dataset_all.NomMag.str.contains(r"PHARMACY") == False]
dataset_all = dataset_all[dataset_all.NomMag.str.contains(r"LIDL") == False]
# on remplace les noms de mag contenant 'Ferme' par 'Ferme'
dataset_all.loc[dataset_all.NomMag.str.contains(r"FERME"), "NomMag"] = "FERME"


# on retient dans un premier temps le mag dans la list main_ens
dataset_all = dataset_all[dataset_all.NomMag.isin(main_ens)]

# maj index
dataset_all.reset_index(drop=True, inplace=True)

# imbrication des données
list_departement = dataset_all.Departement.unique()
dataset_group = dataset_all.groupby(["NomMag", "Departement"])

dict_mag = {departement : [] for departement in list_departement}
for i in dict_mag.keys():
    for row in main_ens:
        try:
            dict_mag[i].append({row: dataset_group.get_group((row, i)).NomMag.count().item()})
        except KeyError:
            continue

######## ingest dans mongoDB
client = MongoClient("localhost", public_settings.port_DWH)
db = client["BIO"] # database
collection = db["departements"] #collection

# ajoute la variable commune
for row in dict_mag.keys():
    db.departements.update_one({"departement": row}, {"$set": {"magasins": dict_mag[row]}})