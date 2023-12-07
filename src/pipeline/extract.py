import os
import time

import pandas as pd
import numpy as np

from multiprocessing.pool import ThreadPool as Pool
from .clients import edit_schema

def get_checkpoint(conn: object, request_batch_size: int, model_config_id: str, db_schema: str = None) -> pd.DataFrame:
    """
    Returns checkpoint for data processing. Queries data from db files_image table which not have been processed by given
    model configuration.

    Parameters
    ----------
    conn : object
        psycopg2 database client

    request_batch_size : int
        batch size to process at each iteration

    model_config_id : str
        model configuration ID.

    db_schema: str, optional
        defines the database schema to use, default None

    Returns
    -------
    pd.DataFrame
        dataframe with the current objects for this iteration
    """
    if '\n' in model_config_id:
        model_config_id = model_config_id.replace('\n', '')
    print('Current Model Config ID:', model_config_id)

    db_schema = edit_schema(db_schema=db_schema, n=6)

    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    DISTINCT ON ({}files_image.file_id)
                    {}files_image.file_id,
                    {}files_image.object_name,
                    tmp_table.result_id,
                    tmp_table.config_id
                FROM
                    {}files_image
                    LEFT JOIN (
                        SELECT
                            result_id,
                            config_id,
                            file_id
                        FROM
                            {}image_results
                        WHERE
                            config_id = %s
                    ) AS tmp_table ON tmp_table.file_id = {}files_image.file_id
                WHERE
                    tmp_table.config_id IS NULL
                LIMIT
                    %s
                """.format(*db_schema),
                (model_config_id, request_batch_size)
            )
            data = cursor.fetchall()
            colnames = [desc[0] for desc in cursor.description]
    except Exception:
        raise Exception('Could not retrieve data from DB.')

    return pd.DataFrame.from_records(
        data=data,
        columns=colnames
    )


def load_image_batch(data: pd.DataFrame, size: int = 32):
    return data[data['processed'] == 0].iloc[:size]


def get_object_paths(client: object, bucket_name: str, prefix: 'str | list', file_endings: list = ['.jpg', '.png']) -> list:
    """
    Extracts the path of all objects in a bucket by given prefix.

    Parameters
    ----------
    client : object
        Minio s3 client

    bucket_name : str
        Name of the s3 storage bucket

    prefix : str | list
        Prefix to look through for relevant paths. Can be either only one or multiple paths.

    file_endings : list, optional
        Filter objects paths by file ending, by default ['.jpg', '.png']

    Returns
    -------
    list
        List of all object paths in the bucket and prefix.
    """
    object_paths = []
    if isinstance(prefix, list):
        for p in prefix:
            for element in client.list_objects(bucket_name=bucket_name, prefix=p, recursive=True):
                if any([element.object_name.endswith(f_suffix) for f_suffix in file_endings]):
                    object_paths.append(element.object_name)
    else:
        for element in client.list_objects(bucket_name=bucket_name, prefix=prefix, recursive=True):
            object_paths.append(element.object_name)

    return object_paths


def download_files(client: object, bucket_name: str, filenames: list, n_threads: int = 8):
    """
    Downloads all files given by path. Simultaneously creates similar folder structure local.

    Parameters
    ----------
    client : object
        Initiated minio client for s3 storage

    bucket_name : str
        name of the bucket

    filenames : list
        filenames to extract from bucket

    n_threads : int, optional
        number of threads to use for download, by default 8
    """
    # Create equal data structure in local repo
    path_dirs = [os.path.split(f)[0] for f in filenames]
    for new_dir in set(path_dirs):
        try:
            os.makedirs(new_dir)
        except FileExistsError as fe:
            print(fe)

    # Download an object
    def _download_file_mp(filename: str):
        print(f'Loading {filename}')
        try:
            client.fget_object(
                bucket_name=bucket_name,
                object_name=filename,
                file_path=filename
            )
        except Exception as e:
            print(e, f'Not worked for {filename}')

    start = time.perf_counter()
    # Use multiprocessing (or multithreading?)
    with Pool(processes=n_threads) as pool:
        pool.starmap(_download_file_mp, list(zip(filenames)))
    end = time.perf_counter() - start

    print(f'Extracted {len(filenames)} files in {end} seconds')


def build_mount_paths(data: pd.DataFrame, mount_path: str) -> pd.DataFrame:
    """
    Transforms the column object_name to make s3 files accessible via mounted filesystem.

    Parameters
    ----------
    data : pd.DataFrame
        checkpoint dataframe

    mount_path : str
        path where s3 filesystem is mounted

    Returns
    -------
    pd.DataFrame
        checkpoint dataframe with transformed object name
    """
    _join_mount_paths = lambda x: os.path.join(mount_path, x)
    data['object_name'] = data['object_name'].apply(_join_mount_paths)

    return data
