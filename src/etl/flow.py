import os
import yaml

from minio import Minio
from prefect import flow, task

from extract import (
    extract_sub_prefix, 
    get_object_paths, 
    download_files, 
    get_checkpoint,
)
from transform import dummy_transform, model_predict
#from load import 





@flow(name='foo-flow')
def etl_flow():
    # get checkpoint from SQL before loading next
    # Update and get checkpoint by querying all file ID's already processed
    checkpoint = get_checkpoint()


    # --------------------------------
    # Extract
    # --------------------------------
    # Load configurations
    with open('./bucket_config.yaml', 'rb') as yaml_file:
        config = yaml.load(yaml_file, Loader=yaml.FullLoader)
        

    client = Minio(
        config['HOST'], 
        access_key=config['ACCESS_KEY'],
        secret_key=config['SECRET_KEY']
    )

    result = extract_sub_prefix(
        client=client, 
        bucket_name=config['BUCKET_NAME'],
        max_depth=3
    )
    # take subset
    result = result[2][0]

    object_paths = get_object_paths(
        client=client, 
        bucket_name=config['BUCKET_NAME'],
        prefix=result,
        file_endings=['.jpg', '.png']
    )

    download_files(
        client=client,
        bucket_name=config['BUCKET_NAME'],
        filenames=object_paths,
        n_threads=8,
    )
    
    # --------------------------------
    # Transform
    # --------------------------------
    with open('./Pollinatordetection/config.yaml', 'rb') as yaml_file:
        model_config = yaml.load(yaml_file, Loader=yaml.FullLoader)

    model_predict(
        input_filepaths=object_paths,
        cfg=model_config
    )

    
    # --------------------------------
    # Clean up
    # --------------------------------


    
    # --------------------------------
    # Load
    # --------------------------------


if __name__ == '__main__':
    etl_flow()