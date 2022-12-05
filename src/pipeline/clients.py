import yaml

import psycopg2
from minio import Minio


def edit_schema(db_schema: str = None, n: int = 20) -> list:
    """
    Applies a string operation on db_schema and creates a list of n db_schema elements.

    Parameters
    ----------
    db_schema: str, optional
        defines the database schema to use, default None

    n : int, optional
        list size, by default 11

    Returns
    -------
    list
        [db_schema, db_schema ...]
    """
    # add schema if given as argument
    if db_schema is not None:
        if not db_schema.endswith('.'):
            db_schema = db_schema + '.'
    else:
        db_schema = ''
    # creates list of ['schema', 'schema']
    # 11 = amount of schema is used within query
    db_schema = n*[db_schema] 

    return db_schema


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
    db_schema = edit_schema(db_schema=config['DB_SCHEMA'], n=1)

    with conn.cursor() as cursor:    
        # Perform simple query to check connection
        try:
            cursor.execute(
                """
                SELECT * FROM {}files_image
                LIMIT 10
                """.format(*db_schema)
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

