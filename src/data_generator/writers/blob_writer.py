# writers/blob_writer.py

from azure.storage.blob import BlobServiceClient
from config.settings import settings
import os


class BlobWriter:

    def __init__(self):
        if not settings.ADLS_CONNECTION_STR:
            raise ValueError("Missing Azure Blob connection string")

        self.client = BlobServiceClient.from_connection_string(
            settings.ADLS_CONNECTION_STR
        )

    def upload_file(self, local_path, table_name: str):

        # CASE 1: multiple files
        if isinstance(local_path, list):
            for path in local_path:
                self._upload_single_file(path, table_name)
        else:
            # CASE 2: single file
            self._upload_single_file(local_path, table_name)


    def _upload_single_file(self, local_path: str, table_name: str):

        blob_name = f"{table_name}/{os.path.basename(local_path)}"

        blob_client = self.client.get_blob_client(
            container=settings.BLOB_CONTAINER,
            blob=blob_name
        )

        with open(local_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)

        print(f"☁️ Uploaded to Blob: {blob_name}")