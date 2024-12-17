from typing import Any, Optional


class Response(object):

    def __init__(self, status_code: Optional[int], data: Any):
        self.status_code = status_code
        self.data = data
