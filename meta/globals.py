"""
a few global objects I need everywhere and will therefore define here for now
"""
from datetime import timedelta
from pathlib import Path

import requests_cache

root_dir = Path(__file__).parent.parent

s = requests_cache.CachedSession(root_dir / 'ds/requests_cache', expire_after=timedelta(hours=1))
