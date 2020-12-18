import os
from google.cloud import storage

import google.auth
import google.api_core
import warnings


class Cloud:
    """

    Cette classe va permettre d'ingester et de télécharger des fichiers avec google cloud platform en utilisant
    cloud storage

    """

    def __init__(self, key, bucket_name):
        """

        Initialisation de la classe pour se connecter à un bucket de cloud storage

        :param bucket_name: nom du bucket
        :param key: clé d'authentification au bucket (ex: path/file.json)
        """

        # on ajoute le fichier d'authentification aux variables d'environnement
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.abspath(key)

        # nom du bucket
        self.bucket_name = bucket_name

        # on se connecte à google cloud platform
        try:
            self.client = storage.client.Client()
        except google.auth.exceptions.DefaultCredentialsError:
            warnings.warn("Veuillez vérifier le chemin du fichier contenant les clés d'identification")

        # on accède au bucket
        try:
            self.bucket = self.client.get_bucket(self.bucket_name)
        except google.api_core.exceptions.NotFound:
            warnings.warn("Veuillez vérifier le nom du bucket")
        except AttributeError:
            warnings.warn("Problème avec le fichier contenant les clés d'identification")

    def ingest_into_cloud_storage(self, path_file, file):
        """

        Cette fonction permet d'ingester un fichier dans un bucket de cloud storage

        :param path_file: chemin où se trouve le fichier (ex: ../folder)
        :param file: nom du fichier avec son extension (ex: toto.txt, toto.csv,...)
        :return:
        """
        # on configure le nom du fichier dans le bucket, avec un chemin ou pas
        remote_file = self.bucket.blob(file)

        # on ingeste le fichier dans le bucket
        try:
            local_file = remote_file.upload_from_filename(filename=os.path.abspath(path_file) + "/" + file)
        except FileNotFoundError:
            warnings.warn("Veuillez vérifier le chemin et le nom du fichier à ingester")

    def download_from_cloud_storage(self, file, download_type = "text"):
        """

        Cette fonction permet de télécharger un fichier depuis un bucket de cloud storage

        :param file:
        :param download_type: text or string
        :return:
        """
        self.data = self.bucket.get_blob(file)

        try:
            isinstance(self.data, type(None)) == False
        except:
            warnings.warn("Veuillez vérifier le chemin et le nom du fichier à télécharger")

        if download_type.lower() == "text":
            self.dataset = self.data.download_as_text()
        elif download_type.lower() == "string":
            self.dataset = self.data.download_as_string()
        else:
            warnings.warn("Veuillez choisir entre 'text' ou 'string' pour le paramètre 'download_type' ")
            self.dataset = 0

        return self

    def list_files_blod(self, path):
        """

        Cette fonction retourne la liste des fichiers dans un blod

        :param path:
        :return:
        """
        return [file.name for file in self.client.list_blobs(self.bucket_name, prefix= path)][1:]