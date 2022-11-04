import yaml
import os
import time

import pandas as pd
import numpy as np

from minio import Minio
import psycopg2
from prefect import task

from multiprocessing.pool import ThreadPool as Pool



@task
def get_checkpoint(conn: object, request_batch_size: int) -> pd.DataFrame:
    try:
        with conn.cursor() as cursor:        
            cursor.execute(
            f"""
            SELECT files_image.file_id, files_image.object_name, image_results.result_id 
            FROM files_image 
            LEFT JOIN image_results 
            ON files_image.file_id = image_results.file_id
            WHERE image_results.result_id IS NULL
            LIMIT {request_batch_size}
            """
            )
            data = cursor.fetchall()
            colnames = [desc[0] for desc in cursor.description]
    except Exception:
        raise Exception('Could not retrieve data from DB.')

    return pd.DataFrame.from_records(
        data=data, 
        columns=colnames
    )  

@task
def load_image_batch(data: pd.DataFrame, size: int = 32):
    return data[data['processed'] == 0].iloc[:size]


@task
def get_object_paths(client: object, bucket_name: str, prefix: str | list, file_endings: list = ['.jpg', '.png']) -> list:
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

@task
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