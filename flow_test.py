import os
import yaml
import json

from minio import Minio
from prefect import flow
from prefect.filesystems import LocalFileSystem

from src.etl import (
    extract_sub_prefix, 
    get_object_paths, 
    download_files, 
    get_checkpoint,
    load_image_batch
)
from src.etl import dummy_transform, model_predict
from src.etl import update_processed_data


"""# Temp configs
IS_TEST = True
TEST_DATA = 'synthetic_table.csv'
BATCHSIZE = 16"""


@flow(name='test-flow')
def etl_flow(
    BATCHSIZE,
    TEST_DATA, 
    IS_TEST
):

    local_file_system_block = LocalFileSystem.load("lfs")

    
    # get checkpoint from SQL before loading next
    # Load Configurations
    with open('bucket_config.yaml', 'rb') as yaml_file:
        bucket_config = yaml.load(yaml_file, yaml.FullLoader)

    df_ckp = get_checkpoint(
        is_test=IS_TEST, 
        path=TEST_DATA
    )

    # --------------------------------
    # Extract
    # --------------------------------
    df_batch = load_image_batch(
        data=df_ckp,
        size=BATCHSIZE
    )

    # initiate s3 client
    client = Minio(
        bucket_config['HOST'], 
        access_key=bucket_config['ACCESS_KEY'],
        secret_key=bucket_config['SECRET_KEY']
    )

    download_files(
        client=client,
        bucket_name=bucket_config['BUCKET_NAME'],
        filenames=df_batch['object_name'].to_list(),
        n_threads=8,
    )
    
    # --------------------------------
    # Transform
    # --------------------------------
    with open('model_config.yaml', 'rb') as yaml_file:
        model_config = yaml.load(yaml_file, Loader=yaml.FullLoader)

    predictions = model_predict(
        data=df_ckp,
        cfg=model_config
    )
    # write results to json
    with open('results.json', 'w') as json_file:
        json.dump(predictions, json_file)
        print('------------- Predictions-File was saved', os.path.isfile('results.json'))
    
    # --------------------------------
    # Load
    # --------------------------------


    # --------------------------------
    # Update
    # --------------------------------
    df_ckp = update_processed_data(
        df=df_ckp, 
        processed_ids=df_batch['object_name'].to_list(),
        path=TEST_DATA
    )

    # Updates SQL DB

    # --------------------------------
    # Clean up
    # --------------------------------

    # Remove local files like images from s3, temporary files and so on




    
if __name__ == '__main__':
    etl_flow()