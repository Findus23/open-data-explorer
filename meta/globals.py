"""
a few global objects I need everywhere and will therefore define here for now
"""
from datetime import timedelta
from pathlib import Path

import requests_cache

root_dir = Path(__file__).parent.parent
ds_dir = root_dir / "ds"
s = requests_cache.CachedSession(ds_dir / 'requests_cache', expire_after=timedelta(hours=1),allowable_methods=('GET', 'POST'))
s.cache.delete(expired=True)
