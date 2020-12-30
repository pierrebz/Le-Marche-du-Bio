import CloudStorage
from Code.SetParameters import public_settings


def ingest_cloud_storage(file, path_file, bucket_name, key):
    """

    Ingestion d'un fichier

    :param file:
    :param path_file:
    :param bucket_name:
    :param key:
    :return:
    """
    connect = CloudStorage.Cloud(key, bucket_name).ingest_into_cloud_storage(path_file, file)


if __name__ == "__main__":
    ingest_cloud_storage(file="surface_agri/surface_bio_2008_2011.csv", path_file="../../Data/", bucket_name= public_settings.bucket,
                         key= public_settings.path_key_bucket)