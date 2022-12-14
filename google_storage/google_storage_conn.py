from google.cloud import storage
from datetime import datetime, timedelta
from io import StringIO
import pathlib
import os
import shutil
import zipfile
import logging


class GoogleBucketConnection:
    
    # os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = CREDENTIALS_PATH

    def __init__(self, bucket_name) -> None:
        self.storage_client = storage.Client()
        self.bucket = self.storage_client.get_bucket(bucket_name)

    
    @staticmethod
    def get_filenames(main_path:str) -> list:
        """Get all filenames into a directory
        Args:
            main_path (str): Path to search files
        Returns:
            list: List with all files presents in the directory
        """

        filenames = []
        for directory, _, files  in os.walk(main_path):
            for file in files:
                file = os.path.join(directory, file)
                filenames.append(file)
        return filenames    


    def get_bigger_file(self, directory:str) -> str:
        """After downloading all files we want to keep only the bigger one
        Args:
            directory (str): Directory where the files are stored
        Returns:
            [str]: Returns the biggerfile name
        """
        filepaths = self.get_filenames(directory)
        bigger_size = 0
        bigger_file = ''
        for file in filepaths:
            filesize = os.path.getsize(file)
            print(file, filesize)
            if filesize > bigger_size:
                bigger_file = file
                bigger_size = filesize

        return bigger_file

    def remove_smaller_files(self, bigger_file:str, directory:str):
        """
        Remove all smaller files and keeps only the bigger one
        Args:
            bigger_file (str): Bigger file name
            directory (str): Path where the files are stored
        """
        filepaths = self.get_filenames(directory)
        for file in filepaths:
            if file == bigger_file:
                continue
            else:
                os.remove(file)
                
    @staticmethod
    def make_timestamp(delta_days=0):
            """Generate a timestamp in an American format, like: if today is
            09/02/2021 the timestamp will be 20210209 if no delta_days were applied
            Args:
                delta_days (int, optional): How many days you want to subtract from today date. Defaults to None.
            Returns:
                [type]: [description]
            """
            today_date = datetime.now().date()
            timestamp = today_date - timedelta(delta_days)

            return str(timestamp)


    def list_bucket_files(self, prefix=None):
        if prefix:
            files = [file.name for file in list(self.bucket.list_blobs(prefix=prefix))]
        else:
            files = [file.name for file in list(self.bucket.list_blobs())]

        return files
    
    def clean_and_unzip_files(self, path):
        file_to_unzip = self.get_bigger_file(path)
        self.remove_smaller_files(file_to_unzip, path)


        with zipfile.ZipFile(file_to_unzip) as zip_ref:
            zip_ref.extractall(path)
            
            
    def download_file_from_bucket(self, blob_name, destiny_dir):
        try:
            print(f'Downloading {blob_name}...')
            blob = self.bucket.blob(blob_name)
            pathlib.Path(destiny_dir).mkdir(parents=True, exist_ok=True)
            with open(os.path.join(destiny_dir, blob_name.split('/')[-1]), 'wb') as f:
                self.storage_client.download_blob_to_file(blob, f)
        except Exception as e:
            print("Error:", e)


    def get_file_from_bucket(self, blob_name):
        try:
            print(f'Downloading {blob_name}...')
            blob = self.bucket.blob(blob_name)
        except Exception as e:
            print("Error:", e)

        return blob



    def download_files_from_bucket(self, blobs_names, destiny_dir):
        pathlib.Path(destiny_dir).mkdir(parents=True, exist_ok=True)
        
        for blob_name in blobs_names:
            try:
                print(f'Downloading {blob_name}...')
                blob = self.bucket.blob(blob_name)
                with open(os.path.join(destiny_dir, blob_name), 'wb') as f:
                    self.storage_client.download_blob_to_file(blob, f)
            except Exception as e:
                print(e)

if __name__=='__main__':

    CREDENTIALS_PATH = ""
    BUCKET_FILES_PATH = ""
    BUCKET_NAME = ""

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = CREDENTIALS_PATH


    PREFIX = ""
    SALES_TEMPLATE = ""
    DELTA_DAYS = 1

    storage_conn = GoogleBucketConnection(BUCKET_NAME)

    timestamp = storage_conn.make_timestamp(delta_days=DELTA_DAYS)
    year_month = ''.join(timestamp.split('-')[:2])
    filename_gcs = SALES_TEMPLATE.format(year_month)
  
    timestamp = storage_conn.make_timestamp(delta_days=DELTA_DAYS)
    file = storage_conn.list_bucket_files(prefix=f'{PREFIX}/{filename_gcs}')
    # blob = storage_conn.get_file_from_bucket(blob_name=file)
    blob = storage_conn.download_files_from_bucket(blobs_names=file, destiny_dir='')

