import os
import re
import subprocess

from colorama import init, Fore, Style

from src.database.record_database import RecordDatabase
from src.utils.config import config
from src.utils.random_generator import random_browser


def print_out(output, default_color, label):
    if output:
        print(f'{default_color}{Style.BRIGHT}{label}:{Style.RESET_ALL}')
        for line in output.splitlines():
            if re.match(r'^E\s+', line):
                print(f'{Fore.RED}{line}{Style.RESET_ALL}')
            else:
                print(f'{default_color}{line}{Style.RESET_ALL}')


def scrape_web_page_content(url):
    random_browser()
    os.environ['URL_TO_SCRAPE'] = url
    try:
        with subprocess.Popen(
                ['pytest', f"{config.jobs_dir}", '--cache-clear', '-s'],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                bufsize=1,
                universal_newlines=True
        ) as process:
            stdout, stderr = process.communicate()
            init(autoreset=True)
            print_out(stdout, Fore.GREEN, 'Standard Output')
            print_out(stderr, Fore.RED, 'Standard Error')
            if process.returncode == 0:
                summary, details = RecordDatabase('record').get_news(url)
                return summary, details
            else:
                print(f'{Fore.RED}Scraping {url} failed with errors{Style.RESET_ALL}')
                return '', ''
    except subprocess.CalledProcessError as e:
        print(f'{Fore.RED}Command failed with return code {e.returncode}{Style.RESET_ALL}')
        print(f'Error output: {e.stderr}')
        return '', ''
