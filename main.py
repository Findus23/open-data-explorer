import csv
import tempfile
from datetime import timedelta, datetime

import requests_cache
from sqlite_utils import Database
from sqlite_utils.utils import file_progress, TypeTracker

from ds.datagv import get_metadata
from meta import MetaDatabase, create_ds_metadata
from meta.meta_db import Record, Resource

s = requests_cache.CachedSession('ds/requests_cache', expire_after=timedelta(hours=1))
id = "148bfd64-40c2-3201-831c-fa2ab8766dbb"
id = "c6fc4688-1aca-4363-bd1a-8d14ea08d2f8"

meta_db = MetaDatabase(Database(f"ds/meta.db"))

datagv_meta = get_metadata(id, s)
db = Database(f"ds/{id}.db", recreate=True)
db.enable_wal()


meta_obj = Record(
    id=id,
    title=datagv_meta["title"],
    publisher=datagv_meta["publisher"],
    notes=datagv_meta["notes"],

    license_citation=datagv_meta["license_citation"],
    license_id=datagv_meta["license_id"],
    license_title=datagv_meta["license_title"],
    license_url=datagv_meta["license_url"],

    maintainer=datagv_meta["maintainer"],
    metadata_created=datagv_meta["metadata_created"],
    metadata_modified=datagv_meta["metadata_modified"],
    metadata_linkage=datagv_meta["metadata_linkage"],

    attribute_description=datagv_meta["attribute_description"],
    geographic_toponym=datagv_meta["geographic_toponym"],
    tags=[p["display_name"] for p in datagv_meta["tags"]],
    api_data=datagv_meta,
)
meta_db.upsert_record(id, meta_obj)

for res in datagv_meta["resources"]:
    format = res["format"]
    name = res["name"]
    if format != "CSV":
        print(f"skipping {name} ({format})")
        continue

    url = res["url"]

    r = s.get(url)
    r.raise_for_status()
    with tempfile.TemporaryFile("w+") as f:
        encoding = "utf-8"
        f.write(r.content.decode(encoding, "ignore"))
        print(f.tell())

        f.seek(0)
        start = f.read(2048)  # .decode(encoding, "ignore")
        print(start)
        dialect = csv.Sniffer().sniff(start)
        has_header = csv.Sniffer().has_header(start)
        print(has_header)
        f.seek(0)
        with file_progress(f) as f_prog:
            reader = csv.reader(f_prog, dialect)
            first_row = next(reader)


            def clean_row(row):
                # print(row)
                row = list(map(lambda x: x.replace(",", "."), row))
                # print(row)
                # exit()
                return row


            docs = (dict(zip(first_row, clean_row(row))) for row in reader)
            tracker = TypeTracker()
            docs = tracker.wrap(docs)

            db[name].insert_all(docs)
            print(tracker.types)
            db[name].transform(types=tracker.types)
            for col in db[name].columns:
                coldet = db[name].analyze_column(col.name, most_common=False, least_common=False)
                print(coldet)
                if coldet.num_distinct < 50 < coldet.total_rows:
                    db[name].create_index([col.name])

    meta_res = Resource(
        id=res["id"],
        record=meta_obj,
        format=format,
        name=name,
        url=url,
        mimetype=res["mimetype"],
        position=res["position"],
        last_fetched=datetime.now(),
    )
    meta_db.upsert_resource(res["id"], meta_res)

# db.index_foreign_keys()

db.analyze()
db.vacuum()
create_ds_metadata(meta_db)
exit()
