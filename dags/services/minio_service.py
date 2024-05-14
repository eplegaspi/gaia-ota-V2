from minio import Minio
import urllib3
import io
import os

class MinioConnector:
    def __init__(self, endpoint, access_key, secret_key, secure=False):
        self.minio_client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure
        )

    def fetch_file_from_url(self, url):
        http = urllib3.PoolManager()
        response = http.request('GET', url)
        if response.status == 200:
            return io.BytesIO(response.data)
        else:
            raise Exception(f"Failed to fetch file from URL: {url}, Status Code: {response.status}")

    def store_file(self, bucket_name, object_name, file_stream, file_size):
        try:
            self.minio_client.put_object(bucket_name, object_name, file_stream, file_size)
            print("File stored in MinIO successfully.")
        except Exception as e:
            print(f"Failed to store the file in MinIO: {e}")
            raise

    def download_file(self, bucket_name, object_name, file_path):
        try:
            self.minio_client.fget_object(bucket_name, object_name, file_path)
            print(f"File downloaded from MinIO successfully to {file_path}.")
        except Exception as e:
            print(f"Failed to download the file from MinIO: {e}")
            raise