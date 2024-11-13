import stat
from typing import List

import smbclient


class WindowsShareFolder(object):
    def __init__(self, path, username, password):
        self.path = path
        self.username = username
        self.password = password
        smbclient.register_session(server=str(self.path).split('\\')[2], username=self.username, password=self.password)

    def is_directory(self, path):
        return stat.S_ISDIR(smbclient.stat(path).st_mode)

    def get_files_list(self, path=None, include_subfolders=False, file_type: str = None,
                       include_files: List[str] = None, exclude_files: List[str] = None,
                       sort_by_ctime=False, sort_alphabetical: str = None) -> List[str]:
        if path is None:
            path = self.path
        if exclude_files is None:
            exclude_files = []

        files = [path / file for file in smbclient.listdir(path) if file not in exclude_files]
        if include_subfolders:
            all_files = []
            for file_path in files:
                if self.is_directory(file_path):
                    all_files.extend(
                        self.get_files_list(
                            path=file_path,
                            include_subfolders=include_subfolders,
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
        files = [file for file in files if
                 not self.is_directory(file) and (file_type in file.suffix if file_type else True)]
        if include_files is not None:
            files = [file for file in files if file.name in include_files]
        if sort_by_ctime:
            ctimes = {file: smbclient.stat(file).st_ctime for file in files}
            files = sorted(files, key=ctimes.get, reverse=True)
        elif sort_alphabetical is not None:
            if sort_alphabetical.lower() == 'asc':
                files = sorted(files, key=lambda x: x.name)
            elif sort_alphabetical.lower() == 'desc':
                files = sorted(files, key=lambda x: x.name, reverse=True)

        return files
