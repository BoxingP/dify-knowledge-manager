import datetime
from functools import wraps


def timing(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = datetime.datetime.now()
        result = func(*args, **kwargs)
        end = datetime.datetime.now()
        duration = (end - start).total_seconds()
        minutes = int(duration // 60)
        seconds = int(duration % 60)
        print(f"Function '{func.__name__}' took {minutes} min {seconds} sec")
        return result

    return wrapper
