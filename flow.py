import json
import os

import pandas as pd
import yaml

from src.pipeline.clients import get_db_client, get_minio_client
from src.pipeline.extract import get_checkpoint
from src.pipeline.load import (
    db_insert_flower_predictions,
    db_insert_image_results,
    db_insert_model_config,
    db_insert_pollinator_predictions,
)
from src.pipeline.transform import (
    model_predict,
    process_flower_predictions,
    process_pollinator_predictions,
)

def etl_flow(
    BATCHSIZE=64,
    CONFIG_PATH="source_config.yaml",
    MODEL_CONFIG_PATH="model_config.json",
    IS_TEST=False,
    MULTI_RESULTS_FOR_IMAGE=False
):
    """
    This function represents a flow implemented with prefect. A flow includes multiple smaller prefect task.
    All files cerated durnig workflow do not persist.

    Parameters
    ----------
    BATCHSIZE : int, optional
        batch size which shall be processed at a time, by default 64

    CONFIG_PATH : str, optional
        path to the configuration file (yaml-file), by default "source_config.yaml"

    MODEL_CONFIG_PATH : str, optional
        path to model configuration file, which will be uploaded to the db (JSON), by default "model_config.json"

    IS_TEST : bool, optional
        flag indicating wether the flow run is a test case or not, by default False

    MULTI_RESULTS_FOR_IMAGE : bool, optional
        if True allows multiple results per image with equal configuration, else it will through an error
        by default False

    Returns
    -------
    None
    """

    # get checkpoint from SQL before loading next
    # Load Configurations and Init clients
    conn = get_db_client(config_path=CONFIG_PATH)

    storage = {
        'client': get_minio_client(config_path=CONFIG_PATH),
        'bucket_name': config["MINIO_BUCKET_NAME"]
    }

    # Load configs
    with open(CONFIG_PATH, "rb") as yaml_file:
        config = yaml.load(yaml_file, yaml.FullLoader)

    with open(MODEL_CONFIG_PATH, "r") as json_file:
        model_config = json.load(json_file)

    # -----------------------------------------------
    # Extract
    # -----------------------------------------------
    # Extract objects to be processed
    df_ckp = get_checkpoint(
        conn=conn,
        request_batch_size=BATCHSIZE,
        model_config_id=model_config["config_id"],
        db_schema=config['DB_SCHEMA']
    )
    print(f"Processing {df_ckp.shape[0]} datapoints.")

    # interrupt flow run if there is no new data -> state cancelled
    if df_ckp.shape[0] == 0:
        print("No new data to process. Flow run cancelled.")
        return 1

    # -----------------------------------------------
    # Transform and Load
    # -----------------------------------------------
    # Inserts model config if not exists
    try:
        db_insert_model_config(
            conn=conn,
            model_config=model_config,
            db_schema=config['DB_SCHEMA']
        )
    except Exception as e:
        print(e)
        print("Model config already exists in DB.")

    flower_predictions, pollinator_predictions = model_predict(
        data=df_ckp,
        cfg=model_config,
        minio=storage
    )

    # Insert image results and get back result_ids
    result_ids = db_insert_image_results(
        conn=conn,
        data=df_ckp,
        model_config=model_config,
        allow_multiple_results=MULTI_RESULTS_FOR_IMAGE,
        db_schema=config['DB_SCHEMA']
    )
    df_ckp["result_id"] = result_ids

    if IS_TEST:
        df_ckp.to_csv("checkpoint_df.csv", index=False)
        # Post-process flower predictions output
        # write results to json if flow run is for test purpose
        with open("flower_predictions.json", "w") as json_file:
            json.dump(flower_predictions, json_file)
        with open("pollinator_predictions.json", "w") as json_file:
            json.dump(pollinator_predictions, json_file)

    if len(flower_predictions) > 0:
        # Flower predictions pre-processing and ingestion
        flower_predictions = process_flower_predictions(
            flower_predictions=flower_predictions,
            result_ids=df_ckp,
            model_config=model_config
        )
        if IS_TEST:
            flower_predictions.to_csv('flower_predictions.csv', index=False)

        flower_ids = db_insert_flower_predictions(
            conn=conn,
            data=flower_predictions,
            db_schema=config['DB_SCHEMA']
        )
        # Append IDs to flower predictions
        flower_predictions = pd.concat(
            [flower_predictions, pd.Series(flower_ids)], axis=1
        )
        flower_predictions = flower_predictions.rename(columns={0: "flower_id"})
        if len(pollinator_predictions) > 0:

            # Pollinator predictions pre-processing and ingestion
            pollinator_predictions = process_pollinator_predictions(
                pollinator_predictions=pollinator_predictions,
                flower_predictions=flower_predictions,
                model_config=model_config
            )
            if IS_TEST:
                pollinator_predictions.to_csv('pollinator_predictions.csv', index=False)

            db_insert_pollinator_predictions(
                conn=conn,
                data=pollinator_predictions,
                db_schema=config['DB_SCHEMA']
            )
    else:
        print("No Flowers or Pollinators predicted")

    # close db connection
    conn.close()


if __name__ == "__main__":
    etl_flow()
