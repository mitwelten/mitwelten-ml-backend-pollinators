"""
This python file includes all task regarding to loading the data from the predictions
to the database and updating all relevant columns with processed information
"""
from concurrent.futures import process
import pandas as pd
from sqlalchemy import create_engine



def connect_db(user: str, password: str, hostname: str, database_name: str):
    engine = create_engine('postgresql+psycopg2://{}:{}@{}/{}'.format(
        user,
        password,
        hostname,
        database_name
    ))
    return engine


def upload_predictions(data: pd.DataFrame):
    pass

def alter_table_processed(data: pd.DataFrame):
    pass



def update_processed_data(df: pd.DataFrame, processed_ids: list) -> pd.DataFrame:
    return df.loc[df['object_name'].isin(processed_ids), 'processed'] = 1