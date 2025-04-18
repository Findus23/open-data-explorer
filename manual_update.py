from pathlib import Path

from sqlite_utils import Database
from sqlite_utils.db import NotFoundError

from meta.meta_db import meta_db
from meta.processes import run_datasette_inspect

for file in Path("ds").glob("*.db"):
    if "meta" in file.name:
        continue
    print(file)
    try:
        meta=meta_db.db["records"].get(file.stem)
    except NotFoundError:
        print(file, "not found")
        input("delete")
        file.unlink()
        file.with_suffix(".db-shm").unlink(missing_ok=True)
        file.with_suffix(".db-wal").unlink(missing_ok=True)
        continue
    db = Database(file)

    db.disable_wal()
    db.vacuum()
    db.disable_wal()
    db.vacuum()
    # https://til.simonwillison.net/sqlite/database-file-size
    curr = db.execute("select page_size * page_count from pragma_page_count(), pragma_page_size()")
    db_size = curr.fetchone()[0]
    db.close()

    inspect_data = run_datasette_inspect(file.name)
    meta_db.db["records"].update(file.stem,{
        "db_size":db_size,
        "inspect_data":inspect_data,
    })


