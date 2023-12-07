import json
import pandas as pd
import numpy
from tqdm import tqdm

from psycopg2.extensions import register_adapter, AsIs

from .clients import edit_schema


# Register Adapter function for data ingstinos
# https://stackoverflow.com/questions/50626058/psycopg2-cant-adapt-type-numpy-int64
def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)

def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)

def addapt_numpy_int32(numpy_int32):
    return AsIs(numpy_int32)

def addapt_numpy_float32(numpy_float32):
    return AsIs(numpy_float32)

register_adapter(numpy.float64, addapt_numpy_float64)
register_adapter(numpy.int64, addapt_numpy_int64)
register_adapter(numpy.int32, addapt_numpy_int32)
register_adapter(numpy.float32, addapt_numpy_float32)



def is_model_config_up(conn: object, model_config: dict, db_schema: str = None) -> bool:
    """Checks if model config was already uploaded to database.

    Parameters
    ----------
    conn : object
        psycopg connection object.

    model_config : dict
        model configurations

    db_schema: str, optional
        defines the database schema to use, default None

    Returns
    -------
    bool
        Returns True if the configurations is already up else False
    """
    db_schema = edit_schema(db_schema=db_schema, n=1)

    try:
        with conn.cursor() as cursor:
            cursor.execute(
            """
            SELECT configuration FROM {}pollinator_inference_config
            """.format(*db_schema)
            )
            all_configs = cursor.fetchall()
            if len(all_configs) > 0:
                all_configs = [element[0] for element in all_configs]
    except Exception:
        raise Exception('Could not request configuration')
    finally:
        conn.commit()

    return model_config in all_configs


def db_insert_model_config(conn: object, model_config: dict, db_schema: str = None):
    """Inserts model config into db

    Parameters
    ----------
    conn : object
        psycopg connection object

    model_config : dict
        model configurations

    db_schema: str, optional
        defines the database schema to use, default None
    """
    db_schema = edit_schema(db_schema=db_schema, n=1)

    if not is_model_config_up(conn=conn, model_config=model_config, db_schema=db_schema[0]):
        # upload current model config to DB as json
        model_config_json = json.dumps(model_config)
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO {}pollinator_inference_config (config_id, configuration)
                    VALUES (%s, %s)
                    """.format(*db_schema),
                    (model_config['config_id'], model_config_json)
                )
        except Exception:
            raise Exception('Could not insert.')
        finally:
            conn.commit()
    else:
        print('Model Config alrealdy up.')


def db_insert_image_results(conn: object, data: pd.DataFrame, model_config: dict, allow_multiple_results: bool = True, db_schema: str = None):
    """Adds processed data to image_results. Insert statement looks duplicated values during insertion.

    Parameters
    ----------
    conn : object
        psycopg connection object

    data : pd.DataFrame
        dataframe where image_results are stored

    model_config : dict
        model configuration

    allow_multiple_results: bool (default; True)
        if true then allows multiple predictions for one image, else
        the program will throw an error if theres an existig prediction for an image

    db_schema: str, optional
        defines the database schema to use, default None

    Returns
    ---------
    result_ids: list
        Resulting auto generated result_id
    """
    db_schema = edit_schema(db_schema=db_schema, n=2)

    # Prpare data
    config_id = model_config['config_id']
    data['config_id'] = config_id
    records = data[['file_id', 'config_id']].to_records(index=False)

    results = []
    if not allow_multiple_results:
        try:
            with conn.cursor() as cursor:
                for record in tqdm(records):
                    file_id = int(record[0])
                    config_id = record[1]
                    cursor.execute(
                        """
                        INSERT INTO {}image_results (file_id, config_id)
                        VALUES (%s, %s)
                        ON CONFLICT ON unique_file_config DO NOTHING
                        RETURNING result_id
                        """.format(*db_schema),
                        (file_id, config_id, file_id, config_id)
                    )
                    result = cursor.fetchone()[0]
                    results.append(result)
        except Exception:
            raise Exception('Could not insert. Values might be written already to DB.')
        finally:
            conn.commit()
    else:
        try:
            with conn.cursor() as cursor:
                for record in tqdm(records):
                    file_id = int(record[0])
                    config_id = record[1]
                    cursor.execute(
                        """
                        INSERT INTO {}image_results (file_id, config_id)
                        VALUES (%s, %s)
                        RETURNING result_id
                        """.format(*db_schema),
                        (file_id, config_id)
                    )
                    result = cursor.fetchone()[0]
                    results.append(result)
        except Exception:
            raise Exception('Could not insert. Values might be written already to DB.')
        finally:
            conn.commit()

    return results


def db_get_image_results(conn: object, db_schema: str = None) -> pd.DataFrame:
    """Returns table of current image results

    Parameters
    ----------
    conn : object
        psycopg db connector object.

    db_schema: str, optional
        defines the database schema to use, default None

    Returns
    -------
    pd.DataFrame
        DataFrame with all image_results
    """
    db_schema = edit_schema(db_schema=db_schema, n=6)

    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT {}files_image.object_name, {}image_results.result_id
                FROM {}image_results
                LEFT JOIN {}files_image
                ON {}files_image.file_id = {}image_results.file_id
                """.format(*db_schema),
            )
            data = cursor.fetchall()
    except Exception:
        raise Exception('Could not query data.')
    finally:
        conn.commit()

    colnames = [desc[0] for desc in cursor.description]
    return pd.DataFrame.from_records(data=data, columns=colnames)

def db_insert_flower_predictions(conn: object, data: pd.DataFrame, db_schema: str = None):
    """Inserts flower predictions to flower table.

    Parameters
    ----------
    conn : object
        psycopg2 db connection object

    data : pd.DataFrame
        Pre-processed data ready for insertion.

    Returns
    --------
    flower_ids
        auto incremented flower ids from postgres
    """
    db_schema = edit_schema(db_schema=db_schema, n=1)

    records = data[[
        'result_id', 'flower_name', 'flower_score',
        'x0', 'y0', 'x1', 'y1']].to_records(index=False)
    flower_ids = []
    try:
        with conn.cursor() as cursor:
            for record in tqdm(records):
                cursor.execute(
                    """
                    INSERT INTO {}flowers (result_id, class, confidence, x0, y0, x1, y1)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    RETURNING flower_id
                    """.format(*db_schema),
                    record
                )
                flower_ids.append(cursor.fetchone()[0])
    except Exception:
        raise Exception('Could not insert.')
    finally:
        conn.commit()

    return flower_ids

def db_insert_pollinator_predictions(conn: object, data: pd.DataFrame, db_schema: str = None):
    """Inserts data for pollinator predictions

    Parameters
    ----------
    conn : object
        psycopg2 db connection object

    data : pd.DataFrame
        Pre-processed data ready for insertion.

    db_schema: str, optional
        defines the database schema to use, default None
    """
    db_schema = edit_schema(db_schema=db_schema, n=1)

    records = data[[
        'result_id', 'flower_id',
        'pollinator_names', 'pollinator_scores',
        'x0', 'y0', 'x1', 'y1']].to_records(index=False)
    try:
        with conn.cursor() as cursor:
            for record in tqdm(records):
                cursor.execute(
                    """
                    INSERT INTO {}pollinators (result_id, flower_id, class, confidence, x0, y0, x1, y1)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """.format(*db_schema),
                    record
                )
    except Exception:
        raise Exception('Could not insert.')
    finally:
        conn.commit()
