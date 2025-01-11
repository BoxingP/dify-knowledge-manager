import os
import random

from src.utils.config import config


def random_browser():
    selected_browser = random.choice(config.browser_types)
    print(f"Using {selected_browser}")
    os.environ['BROWSER'] = selected_browser
