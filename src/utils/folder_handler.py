import os
import re
import stat
from pathlib import Path
from typing import Optional, Union, Literal

import smbclient


class FolderHandler(object):
    def __init__(self, path: Union[str, Path], username: Optional[str] = None, password: Optional[str] = None):
        self.path = Path(path)
        self.username = username
        self.password = password

        self.is_network_path = self._detect_network_path()

        if self.is_network_path:
            self._register_network_session()

    def _detect_network_path(self) -> bool:
        return '\\\\' in str(self.path) and self.username is not None and self.password is not None

    def _register_network_session(self):
        match = re.search(r'\\\\(.*?)\\', str(self.path))
        if not match:
            raise ValueError(f'Invalid network path: {self.path}')
        server = match.group(1)
        smbclient.register_session(server=server, username=self.username, password=self.password)

    def is_dir(self, path: Union[str, Path]) -> bool:
        if self.is_network_path:
            return stat.S_ISDIR(smbclient.stat(path).st_mode)
        return Path(path).is_dir()

    def list_dir(self, path: Union[str, Path]) -> list[str]:
        if self.is_network_path:
            return smbclient.listdir(path)
        return os.listdir(path)

    def stat(self, path: Union[str, Path]) -> os.stat_result:
        if self.is_network_path:
            return smbclient.stat(path)
        return os.stat(path)

    def get_files(
            self,
            path: Optional[Union[str, Path]] = None,
            include_sub_dirs=False,
            file_type: Optional[str] = None,
            include_files: Optional[list[str]] = None,
            exclude_files: Optional[list[str]] = None,
            sort_by_ctime: Optional[Literal['asc', 'desc']] = None,
            sort_alphabetical: Optional[Literal['asc', 'desc']] = None
    ) -> list[Path]:

        if path is None:
            path = self.path
        if exclude_files is None:
            exclude_files = []

        files = [
            Path(path) / file
            for file in self.list_dir(path)
            if file not in exclude_files
        ]

        if include_sub_dirs:
            all_files = []
            for file_path in files:
                if self.is_dir(file_path):
                    all_files.extend(
                        self.get_files(
                            path=file_path,
                            include_sub_dirs=include_sub_dirs,
                            file_type=file_type,
                            include_files=include_files,
                            exclude_files=exclude_files,
                            sort_by_ctime=sort_by_ctime,
                            sort_alphabetical=sort_alphabetical
                        )
                    )
                else:
                    all_files.append(file_path)
            files = all_files
        files = [
            file
            for file in files
            if not self.is_dir(file) and (file_type in file.suffix if file_type else True)
        ]
        if include_files is not None:
            files = [file for file in files if file.name in include_files]
        if sort_by_ctime:
            ctimes = {file: self.stat(file).st_ctime for file in files}
            reverse = sort_by_ctime.lower() == 'desc'
            files = sorted(files, key=ctimes.get, reverse=reverse)
        elif sort_alphabetical:
            reverse = sort_alphabetical.lower() == 'desc'
            files = sorted(files, key=lambda x: Path(x).name, reverse=reverse)

        return files
