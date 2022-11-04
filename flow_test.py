import os
import yaml
import json

from minio import Minio
from prefect import flow

from src.etl.extract import (
    download_files, 
    get_checkpoint,
    load_image_batch
)
from src.etl.transform import model_predict
from src.etl.load import update_processed_data
from src.etl.clients import get_db_client, get_minio_client



@flow(name='test-flow')
def etl_flow(BATCHSIZE=16, TEST_DATA='synthetic_table.csv', IS_TEST=True):

    # get checkpoint from SQL before loading next
    # Load Configurations
    with open('bucket_config.yaml', 'rb') as yaml_file:
        bucket_config = yaml.load(yaml_file, yaml.FullLoader)

    # Init clients
    cursor = get_db_client(config_path='test_config.yaml')
    minio_client = get_minio_client(config_path='test_config.yaml')

    with open('test_config.yaml') as yaml_file:
        config = yaml.load(yaml_file, yaml.FullLoader)

    with open('model_config.yaml', 'r') as json_file:
        model_config = json.load(json_file)
    # --------------------------------
    # Extract
    # --------------------------------
    df_ckp = get_checkpoint(
        is_test=IS_TEST, 
        path=TEST_DATA
    )
    print(f'Processing {df_ckp.shape[0]} datapoints.')

    df_batch = load_image_batch(
        data=df_ckp,
        size=BATCHSIZE
    )

    download_files(
        client=minio_client,
        bucket_name=config['MINIO_BUCKET_NAME'],
        filenames=df_batch['object_name'].to_list(),
        n_threads=8,
    )
    # --------------------------------
    # Transform
    # --------------------------------
    flower_predictions, pollinator_predictions = model_predict(
        data=df_batch,
        cfg=model_config
    )
    # write results to json
    with open('flower_predictions.json', 'w') as json_file:
        json.dump(flower_predictions, json_file)
    with open('pollinator_predictions.json', 'w') as json_file:
        json.dump(pollinator_predictions, json_file)
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



    
if __name__ == '__main__':
    etl_flow()
