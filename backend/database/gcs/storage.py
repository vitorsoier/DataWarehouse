import logging
from google.cloud import storage

class ManipulateGCSBucket():

    def __init__(self, client, name):
        self.__client = client
        self.__name = name
        self.__bucket = self.__client.bucket(name)

        logging.basicConfig(
            filename="backend\logs.log",
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%m/%d/%Y %I:%M:%S %p",
        )

        self.logger = logging.getLogger(__class__.__name__)

    def start(self, file_obj, blob_name, storage_class = "STANDARD", location='us-central1'):
        try:
            if self.__name in self.list_buckets():
                self.upload_blob(file_obj, blob_name)
            else:
                self.create_bucket_class_location(storage_class, location)
                self.upload_blob(file_obj, blob_name)
            return True
        except:
            self.logger.warning(f"Processo falhou")
            return False

    def create_bucket_class_location(self, storage_class = "STANDARD", location='us-central1'):

        self.__bucket.storage_class = storage_class
        new_bucket = self.__client.create_bucket(self.__bucket, location=location)
        self.logger.info(f"Bucket {self.__bucket} criado")
        return new_bucket
    
    def upload_blob(self, source_file_name, destination_blob_name):

        blob = self.__bucket.blob(destination_blob_name)
        blob.upload_from_file(file_obj=source_file_name, rewind=True)
        self.logger.info(f"Blob {destination_blob_name} criado no bucket {self.__bucket}")

    def list_blobs(self):

        blobs =  self.__client.list_blobs(self.__name)
        blob_names = []
        for blob in blobs:
            blob_names.append(blob.name)
        return blob_names
        
    
    def list_buckets(self):

        buckets = self.__client.list_buckets()
        buckets_name = []
        for bucket in buckets:
            buckets_name.append(bucket.name) 
        return buckets_name




