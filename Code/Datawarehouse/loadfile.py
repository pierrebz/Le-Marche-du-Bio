import CloudStorage
from Code.SetParameters import public_settings


def load_cloud_storage(file, bucket_name, key):
    """

    Téléchargement d'un fichier

    :param file:
    :param bucket_name:
    :param key:
    :return:
    """

    data = CloudStorage.Cloud(key, bucket_name).download_from_cloud_storage(file)

    return data


if __name__ == "__main__":
    dataset = load_cloud_storage(file="id_communes.csv", bucket_name= public_settings.bucket,
                                 key= public_settings.path_key_bucket)