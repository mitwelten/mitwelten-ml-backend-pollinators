"""
This python file includes all task regarding to loading the data from the predictions
to the database and updating all relevant columns with processed information
"""
import json

import pandas as pd

import psycopg2

from prefect import task



def is_model_config_up(conn: object, model_config: dict) -> bool:
    """Checks if model config was already uploaded to database.

    Parameters
    ----------
    conn : object
        psycopg connection object.

    model_config : dict
        model configurations

    Returns
    -------
    bool
        Returns True if the configurations is already up else False
    """
    try:
        with conn.cursor() as cursor:        
            cursor.execute(
            """
            SELECT configuration FROM pollinator_inference_config
            """
            )
            all_configs = cursor.fetchall()
            if len(all_configs) > 0:
                all_configs = [element[0] for element in all_configs]  
    except Exception:
        raise Exception('Could not request configuration')
    finally:
        conn.commit()      
    
    return model_config in all_configs

@task
def db_insert_model_config(conn: object, model_config: dict):
    """Inserts model config into db

    Parameters
    ----------
    conn : object
        psycopg connection object

    model_config : dict
        model configurations
    """ 
    if not is_model_config_up(conn=conn, model_config=model_config):
        # upload current model config to DB as json
        model_config_json = json.dumps(model_config)
        try:    
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    INSERT INTO pollinator_inference_config (config_id, configuration)
                    VALUES (%s, %s)
                    """,
                    (model_config['config_id'], model_config_json)
                )
        except Exception:
            raise Exception('Could not insert.')
        finally:
            conn.commit()
    else:
        print('Model Config alrealdy up.')

@task
def db_insert_image_results(conn: object, data: pd.DataFrame, model_config: dict):
    """Adds processed data to image_results. Insert statement looks duplicated values during insertion.

    Parameters
    ----------
    conn : object
        psycopg connection object

    data : pd.DataFrame
        dataframe where image_results are stored

    model_config : dict
        model configuration

    """
    model_id = model_config['config_id']
    data['model_id'] = model_id
    records = data[['file_id', 'model_id']].to_records(index=False)
    try:    
        with conn.cursor() as cursor:
            for record in records:  
                file_id = int(record[0])
                model_id = record[1]
                cursor.execute(
                    """
                    INSERT INTO image_results (file_id, config_id)
                    SELECT %s, %s
                    WHERE NOT EXISTS (
                        SELECT file_id, config_id FROM image_results 
                        WHERE file_id = %s AND config_id = %s
                    )
                    """,
                    (file_id, model_id, file_id, model_id)
                )
    except Exception:
        raise Exception('Could not insert.')
    finally:
        conn.commit()




@task
def update_processed_data(df: pd.DataFrame, processed_ids: list, path: str) -> pd.DataFrame:
    df.loc[df['object_name'].isin(processed_ids), 'processed'] = 1
    df.to_csv(path_or_buf=path, index=False)

    return df