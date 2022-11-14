import sys
sys.path.append('../..')

from src.clients import get_db_client, get_minio_client

def test_clients():
    config_path = input('Type path of config file: ')
    get_db_client(config_path)
    get_minio_client(config_path)

if __name__ == '__main__':
    test_clients()
