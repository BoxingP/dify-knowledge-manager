from abc import ABC, abstractmethod
from pathlib import Path

from src.services.s3_handler import S3Handler


class FileDownloadStrategy(ABC):
    @abstractmethod
    def download_file(self, source_path, destination_dir, skip_if_exists: bool):
        pass


class S3DownloadStrategy(FileDownloadStrategy):
    def __init__(self, s3_handler: S3Handler):
        self.s3_handler = s3_handler

    def download_file(self, source_paths, destination_dir, skip_if_exists: bool):
        return self.s3_handler.find_and_download_file(source_paths, destination_dir, skip_if_exists)


def download_files(file_paths: dict[str: Path], target_dir: Path, strategy: FileDownloadStrategy,
                   skip_if_exists: bool = False) -> dict[str: Path]:
    mapping = {}

    for file_uuid, file_path in file_paths.items():
        destination_path = target_dir / file_path.name
        if skip_if_exists and destination_path.exists():
            print(f'skip downloading file as it already exists: {file_path.name}')
            mapping[file_uuid] = destination_path
            continue

        if strategy.download_file(str(file_path.as_posix()), target_dir, skip_if_exists):
            mapping[file_uuid] = destination_path
    return mapping
