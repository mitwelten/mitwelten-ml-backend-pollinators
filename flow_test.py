import os
import yaml
import json

#from minio import Minio
import pandas as pd
from prefect import flow

from src.etl.extract import (
    download_files,
    get_checkpoint,
)
from src.etl.transform import (
    model_predict,
    process_flower_predictions,
    process_pollinator_predictions
)

from src.etl.load import (
    db_insert_flower_predictions,
    db_insert_image_results,
    db_insert_model_config,
    db_insert_pollinator_predictions
)
from src.etl.clients import (
    get_db_client,
    get_minio_client
)


@flow(name='test-flow')
def etl_flow(
        BATCHSIZE=4, CONFIG_PATH='test_config.yaml', MODEL_CONFIG_PATH='model_config.json', IS_TEST=False, MULTI_RESULTS_FOR_IMAGE=True):

    # get checkpoint from SQL before loading next
    # Load Configurations and Init clients
    conn = get_db_client(config_path=CONFIG_PATH)
    minio_client = get_minio_client(config_path=CONFIG_PATH)

    # Load configs
    with open(CONFIG_PATH, 'rb') as yaml_file:
        config = yaml.load(yaml_file, yaml.FullLoader)

    with open(MODEL_CONFIG_PATH, 'r') as json_file:
        model_config = json.load(json_file)

    # --------------------------------
    # Extract
    # --------------------------------
    df_ckp = get_checkpoint(
        conn=conn,
        request_batch_size=BATCHSIZE
    )

    print(f'Processing {df_ckp.shape[0]} datapoints.')

    download_files(
        client=minio_client,
        bucket_name=config['MINIO_BUCKET_NAME'],
        filenames=df_ckp['object_name'].to_list(),
        n_threads=8,
    )
    # --------------------------------
    # Transform and Load
    # --------------------------------
    # Inserts model config if not exists
    db_insert_model_config(
        conn=conn,
        model_config=model_config
    )

    flower_predictions, pollinator_predictions = model_predict(
        data=df_ckp,
        cfg=model_config
    )
    # Insert image results and get back result_ids
    result_ids = db_insert_image_results(
        conn=conn,
        data=df_ckp,
        model_config=model_config,
        allow_multiple_results=MULTI_RESULTS_FOR_IMAGE
    )
    print(result_ids)
    df_ckp['result_id'] = result_ids

    if IS_TEST:
        df_ckp.to_csv('checkpoint_df.csv', index=False)
        # Post-process flower predictions output
        # write results to json
        with open('flower_predictions.json', 'w') as json_file:
            json.dump(flower_predictions, json_file)
        with open('pollinator_predictions.json', 'w') as json_file:
            json.dump(pollinator_predictions, json_file)

    # Insert image_results

    # Flower predictions pre-processing and ingestion
    flower_predictions = process_flower_predictions(
        flower_predictions=flower_predictions,
        result_ids=df_ckp,
        model_config=model_config
    )
    flower_ids = db_insert_flower_predictions(
        conn=conn,
        data=flower_predictions
    )
    # Append IDs to flower predictions
    flower_predictions = pd.concat(
        [flower_predictions, pd.Series(flower_ids)],
        axis=1
    )
    flower_predictions = flower_predictions.rename(
        columns={0: 'flower_id'}
    )

    # Pollinator predictions pre-processing and ingestion
    pollinator_predictions = process_pollinator_predictions(
        pollinator_predictions=pollinator_predictions,
        flower_predictions=flower_predictions
    )
    db_insert_pollinator_predictions(
        conn=conn,
        data=pollinator_predictions
    )

    conn.close()


if __name__ == '__main__':
    etl_flow()
