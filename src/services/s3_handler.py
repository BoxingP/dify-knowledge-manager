from pathlib import Path

import boto3


class S3Handler(object):
    def __init__(self, aws_access_key_id, aws_secret_access_key, region_name, bucket_name):
        self.bucket_name = bucket_name
        session = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
        )
        self.s3 = session.client('s3')

    def download_file(self, key, local_dir):
        try:
            local_dir_path = Path(local_dir)
            local_dir_path.mkdir(parents=True, exist_ok=True)
            self.s3.download_file(self.bucket_name, key, Path(local_dir) / Path(key).name)
            print(f'download file "{key}" to "{local_dir}" successfully')
        except Exception as e:
            print(f'download file "{key}" to "{local_dir}" failed: {e}')

    def list_files(self, prefix):
        response = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
        if 'Contents' not in response:
            print(f'no files with "{prefix}" in "{self.bucket_name}"')
            return []
        return [file['Key'] for file in response.get('Contents', [])]

    def find_and_download_file(self, file, local_dir, skip_if_exists=False) -> bool:
        files = self.list_files(file)
        if file in files:
            local_file_path = Path(local_dir) / Path(file).name
            if skip_if_exists and local_file_path.exists():
                return True
            else:
                self.download_file(file, local_dir)
                return True
        return False
