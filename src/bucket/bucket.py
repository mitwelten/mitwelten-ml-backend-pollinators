"""
This script defines an API endpoint to load data from a S3 bucket.

1. Get request.
2. Load data from s3 bucket with Minio Client.
3. Save data locally.
4. Return storage path.
"""
import os
import sys
import yaml
import time
import yaml

from minio import Minio

from multiprocessing.pool import ThreadPool



class MinioBucketOperator:

    def __init__(self, host: str, access_key: str, secret_key: str) -> None:
        self.host = host
        self.access_key = access_key
        self.secret_key = secret_key
        self.buckets = None
        self.client = self.get_client()

    def get_client(self) -> object:

        client = Minio(
            self.host, 
            access_key=self.access_key,
            secret_key=self.secret_key
        )

        try:
            self.buckets = client.list_buckets()
        except ConnectionError as ce:
            raise ce('Connection not established')

        return client


    def extract_sub_prefix(self, bucket_name: str, max_depth: int = 2, current_objects: dict = {}, recursive_depth: int = 0) -> dict:
        """
        Extract subfolders for one s3 bucket.

        Parameters
        ----------
        client : object
            minio client

        bucket_name : str
            name of the bucket

        max_depth : int, optional
            maximum folder depth to extract paths, by default 2

        current_objects : dict, optional
            param used for recursive, by default {}

        recursive_depth : int, optional
            current depth for recursive func call, by default 0

        Returns
        -------
        dictionary with folder paths.
        >>> {0: ['0344-6782/', '0863-3235/', '0863-3255/ ...], 1: [ ...]} 
        """
        if recursive_depth == 0:
            # extract root objects
            all_objects = [
                obj.object_name for obj in list(self.client.list_objects(bucket_name=bucket_name))
            ]
            current_objects[recursive_depth] = all_objects
            # recursive func call
            self.extract_sub_prefix(
                client=self.client,
                bucket_name=bucket_name,
                current_objects=current_objects,
                max_depth=max_depth,
                recursive_depth=recursive_depth+1
            )
        else:
            if recursive_depth < max_depth:    
                new_objects = []
                for object_ in current_objects[recursive_depth-1]:
                    for element in self.client.list_objects(bucket_name=bucket_name, prefix=object_):
                        new_objects.append(element.object_name)
                current_objects[recursive_depth] = new_objects
                self.extract_sub_prefix(
                    client=self.client,
                    bucket_name=bucket_name,
                    current_objects=current_objects,
                    max_depth=max_depth,
                    recursive_depth=recursive_depth + 1
                )
        return current_objects


    def get_object_paths(self, bucket_name: str, prefix: str | list, file_endings: list = ['.jpg', '.png']) -> list:
        """
        Extracts the path of all objects in a bucket by given prefix.

        Parameters
        ----------
        client : object
            Minio s3 client

        bucket_name : str
            Name of the s3 storage bucket

        prefix : str | list
            Prefix to look through for relevant paths. Can be either only one or multiple paths.

        file_endings : list, optional
            Filter objects paths by file ending, by default ['.jpg', '.png']

        Returns
        -------
        list
            List of all object paths in the bucket and prefix.
        """
        object_paths = []
        if isinstance(prefix, list):
            for p in prefix:        
                for element in self.client.list_objects(bucket_name=bucket_name, prefix=p, recursive=True):
                    if any([element.object_name.endswith(f_suffix) for f_suffix in file_endings]):
                        object_paths.append(element.object_name)
        else:
            for element in self.client.list_objects(bucket_name=bucket_name, prefix=prefix, recursive=True):
                    object_paths.append(element.object_name)

        return object_paths


    def download_files(self, bucket_name: str, filenames: list, n_threads: int = 8):
        """
        Downloads all files given by path. Simultaneously creates similar folder structure local.

        Parameters
        ----------
        bucket_name : str
            name of the bucket

        filenames : list
            filenames to extract from bucket

        n_threads : int, optional
            number of threads to use for download, by default 8
        """
        # Create equal data structure in local repo
        path_dirs = [os.path.split(f)[0] for f in filenames]
        for new_dir in set(path_dirs):
            try:
                os.makedirs(new_dir)
            except FileExistsError as fe:
                print(fe)

        # Download an object
        def _download_file_mp(filename: str):
            print(f'Loading {filename}')
            try:
                self.client.fget_object(
                    bucket_name=bucket_name, 
                    object_name=filename, 
                    file_path=filename
                )
            except Exception as e:
                print(e, f'Not worked for {filename}')

        start = time.perf_counter()
        # Use multiprocessing (or multithreading?)
        with ThreadPool(processes=n_threads) as pool:
            pool.starmap(_download_file_mp, list(zip(filenames)))    
        end = time.perf_counter() - start

        print(f'Extracted {len(filenames)} files in {end} seconds')

    def upload_files(self, bucket_name: str, filenames: list, relative_path: str = None, n_threads: int = 8):
        """
        Uploads data to S3 with minio client.

        Parameters
        ----------
        bucket_name : str
            name of the bucket

        filenames : list
            filenames to extract from bucket

        n_threads : int, optional
            number of threads to use for download, by default 8
        """
        # Create equal data structure in local repo
        local_paths = [os.path.join(relative_path, obj) for obj in filenames]

        # Download an object
        def _upload_file_mp(data: str):
            """data = (local filename, object_name)"""
            print(f'Loading {data[0]} to {data[1]}')
            try:
                self.client.fput_object(
                    bucket_name=bucket_name, 
                    object_name=data[1], 
                    file_path=data[0]
                )
            except Exception as e:
                print(e, f'Not worked for {data[0]}')

        # build iterable
        iterable = list(zip(local_paths, filenames))

        start = time.perf_counter()
        # Use multiprocessing (or multithreading?)
        with ThreadPool(processes=n_threads) as pool:
            pool.map(_upload_file_mp, iterable)    
        end = time.perf_counter() - start

        print(f'Uploaded {len(filenames)} files in {end} seconds')
