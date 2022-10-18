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

from flask import Flask
from flask import request, jsonify

from minio import Minio
from minio.commonconfig import Tags


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


def extract_sub_prefix(client: object, bucket_name: str, max_depth: int = 2, current_objects: dict = {}, recursive_depth: int = 0):
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
