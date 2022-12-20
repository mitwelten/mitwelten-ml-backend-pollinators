import time
import yaml

import connectorx as cx


class SQLQueryOperator:

    def __init__(self, config_path: str = None, user: str = None, password: str = None, 
                 host: str = None, port: str = None, name: str = None):
        self.config_path = config_path
        self.user = user
        self.pw = password
        self.host = host
        self.port = port
        self.name = name
        self.connection_string = self._connect()
       
    def _connect(self):
        """Connects to database"""
        if self.config_path is not None:
            with open(self.config_path, 'rb') as yaml_file:
                config = yaml.load(yaml_file, yaml.FullLoader)
            self.user = config['POSTGRES_USER']
            self.pw = config['POSTGRES_PASSWORD']
            self.host = config['DB_HOST']
            self.port = config['DB_PORT']
            self.name = config['DB_NAME']
            
        if self.user is None and self.config_path is None:
            raise ValueError('Further Args required.')

        return f"postgresql://{self.user}:{self.pw}@{self.host}:{self.port}/{self.name}"

    def __call__(self, q):
        """Queries data from DB"""
        start_time = time.perf_counter()

        data = cx.read_sql(
            conn=self.connection_string,
            query=q
        )
        end_time = time.perf_counter() - start_time
        print(
            f'Retrieved {len(data)} datapoints in {round(end_time, 3)} seconds')

        return data

