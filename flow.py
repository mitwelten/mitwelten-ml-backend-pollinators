import json
import os

import pandas as pd
import yaml
from prefect import flow

from src.pipeline.clients import get_db_client, get_minio_client
from src.pipeline.extract import (
    download_files, 
    get_checkpoint,
    build_mount_paths
)
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


@flow(name="flower_pollinator_pipeline")
def etl_flow(
    BATCHSIZE=64,
    CONFIG_PATH="source_config.yaml",
    MODEL_CONFIG_PATH="model_config.json",
    IS_TEST=False,
    MULTI_RESULTS_FOR_IMAGE=False,
    USE_FS_MOUNT=False
):

    # get checkpoint from SQL before loading next
    # Load Configurations and Init clients
    conn = get_db_client(config_path=CONFIG_PATH)
    minio_client = get_minio_client(config_path=CONFIG_PATH)

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
    )

    print(f"Processing {df_ckp.shape[0]} datapoints.")

    if USE_FS_MOUNT:
        # transforms column object_name to show the exact name of the object 
        # in the mounted filesystem 
        df_ckp = build_mount_paths(
            data=df_ckp,
            mount_path=config["FS_MOUNT_PATH"],
        )
    else:
        # downloads file from s3
        download_files(
            client=minio_client,
            bucket_name=config["MINIO_BUCKET_NAME"],
            filenames=df_ckp["object_name"].to_list(),
            n_threads=8,
        )
    # -----------------------------------------------
    # Transform and Load
    # -----------------------------------------------
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
        allow_multiple_results=MULTI_RESULTS_FOR_IMAGE,
    )
    df_ckp["result_id"] = result_ids

    if IS_TEST:
        df_ckp.to_csv("checkpoint_df.csv", index=False)
        # Post-process flower predictions output
        # write results to json
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
        flower_ids = db_insert_flower_predictions(conn=conn, data=flower_predictions)
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

            db_insert_pollinator_predictions(conn=conn, data=pollinator_predictions)
    else:
        print("No Flowers or Pollinators predicted")

    # close db connection
    conn.close()


if __name__ == "__main__":
    etl_flow()
