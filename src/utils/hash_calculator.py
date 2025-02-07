import hashlib
from pathlib import Path


class HashCalculator(object):
    def __init__(self, algorithm: str = 'sha256'):
        self.algorithm = algorithm.lower()
        if self.algorithm not in hashlib.algorithms_available:
            raise ValueError(f'Unsupported algorithm: {algorithm}'
                             f'\nAvailable algorithms: {hashlib.algorithms_available}')

    def calculate_file_hash(self, file_path: [str, Path], chunk_size: int = 8192) -> str:
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f'File not found: {file_path}')

        try:
            hash_object = hashlib.new(self.algorithm)
        except ValueError as e:
            raise ValueError(f'Failed to create hash object: {e}')

        try:
            with open(file_path, 'rb') as f:
                while chunk := f.read(chunk_size):
                    hash_object.update(chunk)
        except FileNotFoundError:
            raise
        except PermissionError:
            raise PermissionError(f'Permission denied when trying to read: {file_path}')
        except Exception as e:
            raise Exception(f'An error occurred while reading {file_path}: {e}')

        return hash_object.hexdigest()

    def calculate_text_hash(self, text: str) -> str:
        try:
            hash_object = hashlib.new(self.algorithm)
        except ValueError as e:
            raise ValueError(f'Failed to create hash object: {e}')

        try:
            hash_object.update(text.encode('utf-8'))
        except Exception as e:
            raise Exception(f'An error occurred while hashing text: {e}')

        return hash_object.hexdigest()
