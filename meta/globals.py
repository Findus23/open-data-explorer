"""
a few global objects I need everywhere and will therefore define here for now
"""
from datetime import timedelta
from pathlib import Path

import requests_cache

from very_simple_task_queue import Queue

root_dir = Path(__file__).parent.parent

s = requests_cache.CachedSession('ds/requests_cache', expire_after=timedelta(hours=1))
