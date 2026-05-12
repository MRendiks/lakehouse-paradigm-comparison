class StorageService:
    def __init__(self, project_id: str):
        self.project_id = project_id

    def upload_blob(self, bucket_name: str, destination_blob_name: str, source_file_name: str):
        # Logic to upload file to GCS
        pass

    def list_blobs(self, bucket_name: str):
        # Logic to list objects in GCS
        return []
