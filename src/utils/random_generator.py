import os
import random

from faker import Faker

from src.utils.config import config


def random_browser():
    selected_browser = random.choice(config.browser_types)
    print(f"Using {selected_browser}")
    os.environ['BROWSER'] = selected_browser


def random_name() -> str:
    fake = Faker(['en_US'])
    return fake.first_name().lower()
