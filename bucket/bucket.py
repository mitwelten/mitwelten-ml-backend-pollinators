"""
This script defines an API endpoint to load data from a S3 bucket.

1. Get request.
2. Load data from s3 bucket with Minio Client.
3. Save data locally.
4. Return storage path.
"""
import os
import sys
import yaml
import time

from flask import Flask
from flask import request, jsonify

from minio import Minio

from multiprocessing.pool import ThreadPool as Pool





app = Flask(__name__)

# Load configurations
with open('bucket_config.yaml', 'rb') as yaml_file:
    config = yaml.load(yaml_file)

client = Minio(
    config['HOST'], 
    access_key=config['ACCESS_KEY'],
    secret_key=config['SECRET_KEY']
)


# Load configurations from environment
#os.environ[]


def extract_sub_prefix(client: object, bucket_name: str, max_depth: int = 2, current_objects: dict = {}, recursive_depth: int = 0) -> dict:
    """
    Extract subfolders for one s3 bucket.

    Parameters
    ----------
    client : object
        minio client

    bucket_name : str
        name of the bucket

    max_depth : int, optional
        maximum folder depth to extract paths, by default 2

    current_objects : dict, optional
        param used for recursive, by default {}

    recursive_depth : int, optional
        current depth for recursive func call, by default 0

    Returns
    -------
    dictionary with folder paths.
    >>> {0: ['0344-6782/', '0863-3235/', '0863-3255/ ...], 1: [ ...]} 
    """
    if recursive_depth == 0:
        # extract root objects
        all_objects = [
            obj.object_name for obj in list(client.list_objects(bucket_name=bucket_name))
        ]
        current_objects[recursive_depth] = all_objects
        # recursive func call
        extract_sub_prefix(
            client=client,
            bucket_name=bucket_name,
            current_objects=current_objects,
            max_depth=max_depth,
            recursive_depth=recursive_depth+1
        )
    else:
        if recursive_depth < max_depth:    
            new_objects = []
            for object_ in current_objects[recursive_depth-1]:
                for element in client.list_objects(bucket_name=bucket_name, prefix=object_):
                    new_objects.append(element.object_name)
            current_objects[recursive_depth] = new_objects
            extract_sub_prefix(
                client=client,
                bucket_name=bucket_name,
                current_objects=current_objects,
                max_depth=max_depth,
                recursive_depth=recursive_depth + 1
            )
    return current_objects


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


def get_bucket_data(client: object, bucket: str, prefix: str = None) -> object:
    """

    Parameters
    ----------
    bucket : object
        _description_

    Returns
    -------
    object
        _description_
    """
    pass

def get_bulk_data():
    pass

def save_checkpoint(data: object) -> object:
    """
    Adds extracted bucket data to a text file.

    Parameters
    ----------
    data : object
        _description_

    Returns
    -------
    object
        _description_
    """
    pass



@app.route('/download_data_s3', methods=['GET'])
def download_data_s3():
    """_summary_

    Returns
    -------
    _type_
        _description_
    """
    pass

@app.route('/upload_data_s3/<bucket_name>/<filename>', methods=['PUT'])
def upload_data_s3(bucket_name, filename):
    """_summary_

    Returns
    -------
    _type_
        _description_
    """
    if request.method == 'PUT':
        pass
    else:
        raise Exception(f'Wrong method {request.method}') 




if __name__ == '__main__':

    
    app.run(debug=False, port=8000, host='0.0.0.0')
