"""
This python file includes all task regarding to loading the data from the predictions
to the database and updating all relevant columns with processed information
"""
import pandas as pd
import psycopg2
from prefect import task

def is_model_config_up(conn: object, model_config: dict) -> bool:
    with conn.cursor() as cursor:    
        cursor.execute(
        """
        SELECT configuration FROM pollinator_inference_config
        """
        )
        all_configs = cursor.fetchall()
        all_configs = [element[0] for element in all_configs]        
    
    return model_config in all_configs

def db_insert_model_config(conn: object, model_config: dict):
    if not is_model_config_up(conn=conn, model_config=model_config):
        # upload current model config to DB as json
        model_config_json = json.dumps(model_config)
        with conn.cursor() as cursor:
            try:    
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



def db_insert_image_results(cursor: object, files: list | pd.DataFrame):
    pass




@task
def update_processed_data(df: pd.DataFrame, processed_ids: list, path: str) -> pd.DataFrame:
    df.loc[df['object_name'].isin(processed_ids), 'processed'] = 1
    df.to_csv(path_or_buf=path, index=False)

    return df