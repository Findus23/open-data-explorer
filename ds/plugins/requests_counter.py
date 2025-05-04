"""
This is a really ugly hack to get a rough statistic about which databases are accessed most.

While it won't be precise, it should not slow down anything and still give a good estimate.
"""

import sqlite3
from collections import defaultdict
from pathlib import Path

from datasette import hookimpl

access_stats = defaultdict(int)
save_counter = 0


@hookimpl
def permission_allowed(action, resource):
    if action != "execute-sql":
        return
    access_stats[resource] += 1
    print(access_stats)

    global save_counter
    save_counter += 1
    if save_counter % 10 == 0:
        store_counter()
        save_counter = 0


def store_counter():
    ds_dir = Path(__file__).resolve().parent.parent
    conn = sqlite3.connect(ds_dir / "meta_db.db", autocommit=False)
    for res, count in access_stats.items():
        conn.execute("UPDATE records SET num_queries = num_queries + ? WHERE id = ?",
                     [count, res])

    access_stats.clear()

    conn.commit()
    conn.close()
