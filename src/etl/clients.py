import yaml

import psycopg2
from minio import Minio


def get_db_client(config_path: str) -> object:
    """
    Initiates DB client.

    Parameters
    ----------
    config_path : str
        Path where the config vars for the DB are stored in.

    Returns
    -------
    psycopg2 client
        SQL DB client
    """ 
    with open(config_path, 'rb') as yaml_file:
        config = yaml.load(yaml_file, yaml.FullLoader)

    conn = psycopg2.connect(
        host=config['DB_HOST'],
        port=config['DB_PORT'],
        database=config['DB_NAME'],
        user=config['POSTGRES_USER'],
        password=config['POSTGRES_PASSWORD']
    )
    with conn.cursor() as cursor:    
        # Perform simple query to check connection
        try:
            cursor.execute(
                """SELECT * FROM files_image LIMIT 1"""
            )
        except ConnectionError:
            raise ConnectionError('Could not connect to DB')
        data = cursor.fetchall()
        if len(data) < 1:
            raise Exception(f'Bad return Value: {data}')

    return conn

def get_minio_client(config_path: str) -> object:
    """
    Initiates Minio S3 Buckt client.

    Parameters
    ----------
    config_path : str
        Path where the configuration variables are stored.

    Returns
    -------
    minio Client, object
    """
    with open(config_path, 'rb') as yaml_file:
        config = yaml.load(yaml_file, yaml.FullLoader)

    client = Minio(
        endpoint=config['MINIO_HOST'],
        access_key=config['MINIO_ACCESS_KEY'],
        secret_key=config['MINIO_SECRET_KEY'],
        secure=config['MINIO_SECURE']
    )
    try:
        client.list_buckets()
    except:
        raise ConnectionError('Could not connect to MINIO Client')

    return client