import pandas as pd
pd.set_option('mode.chained_assignment', None) #ignore SettingWithCopyWarning
from geopy.geocoders import Nominatim

import settings


def cleaning_dataset(path, my_agent):
    """
    Fonction qui nettoie le jeu de données contenant les informations sur les communes
    :param path:
    :return:
    """
    dataset = pd.read_csv(path)

    # on selectionne certaines colonnes
    data = dataset[["code_departement", "nom_departement", "nom_region", "code_region",
                    "nom_commune_complet", "code_postal", "latitude", "longitude"]]

    # supprimer les lignes avec un code_departement == NaN
    data.drop(index=data[data["code_departement"].isna()].index, inplace=True)

    # supprimer les lignes avec le departement == NaN
    data.drop(index=data[data["nom_departement"].isna()].index, inplace=True)

    # imputation des coordonnées manquantes
    nom_communes = data[data.latitude.isna()].nom_commune_complet.to_list()
    postal_communes = data[data.latitude.isna()].code_postal.to_list()
    geolocator = Nominatim(user_agent=my_agent)
    pays = "FR"
    for code, commune in enumerate(nom_communes):
        loc = geolocator.geocode(commune + ',' + pays)
        idx = data.index[(data.nom_commune_complet == commune) & (data.code_postal == postal_communes[code])]
        data.iloc[idx, 6] = loc.latitude #latitude
        data.iloc[idx, 7] = loc.longitude #longitude

    return data


if __name__ == "__main__":
    dataset = cleaning_dataset(path= "../Data/communes-departement-region.csv",
                               my_agent= settings.my_agent)