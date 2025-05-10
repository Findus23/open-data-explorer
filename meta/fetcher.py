import csv
import subprocess
import sys
import tempfile
from datetime import datetime
from io import BytesIO
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
from chardet import UniversalDetector
from requests import Response
from sqlite_utils import Database
from sqlite_utils.utils import file_progress, TypeTracker

from meta import create_ds_metadata
from meta.hardcoded_fixes import fix_url, format_normalizer
from meta.parlament import import_parlament
from .datagv import get_metadata
from .globals import s, ds_dir
from .meta_db import Record, Resource, meta_db
from .processes import run_datasette_inspect, restart_datasette_process
from .progress_logger import RecordLogger


def allowed_fetch_url(url: str) -> bool:
    host = urlparse(url).hostname
    allowed_hosts = [
        ".ac.at", ".gv.at",
        "offenerhaushalt.at",
        "arbeitsmarktdatenbank.at"
    ]
    for tld in allowed_hosts:
        if host.endswith(tld):
            return True

    return False


def zstd_file_size(file: Path):
    proc = subprocess.Popen([
        "zstd", "-c", str(file),
    ], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    total = 0
    assert proc.stdout is not None

    while True:
        chunk = proc.stdout.read(8192)
        if not chunk:
            break
        total += len(chunk)
    ret = proc.wait()
    if ret != 0:
        raise RuntimeError(f"zstd returned {ret}")
    return total


def import_csv(db: Database, r: Response, logger: RecordLogger, name: str, i: int, num_res: int):
    with tempfile.TemporaryFile("w+") as f:
        logger.set_status(f"guessing encoding {i}/{num_res}")
        detector = UniversalDetector()
        for line in r.content.splitlines():
            detector.feed(line)
            if detector.done:
                break
        detector.close()
        print(detector.result)
        encoding = detector.result["encoding"]
        f.write(r.content.decode(encoding, "ignore"))

        f.seek(0)
        logger.set_status(f"guessing CSV type {i}/{num_res}")

        start = f.read(4048)  # .decode(encoding, "ignore")
        dialect = csv.Sniffer().sniff(start)
        print(dialect)
        print(dialect.__dict__)
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

            logger.set_status(f"reading CSV file {i}/{num_res}")

            docs = (dict(zip(first_row, clean_row(row))) for row in reader)
            logger.set_status(f"detecting types {i}/{num_res}")

            tracker = TypeTracker()
            docs = tracker.wrap(docs)

            db[name].insert_all(docs)
            db[name].transform(types=tracker.types)
    return encoding


def import_xlsx(db: Database, r: Response, logger: RecordLogger, name: str, i: int, num_res: int):
    logger.set_status(f"reading Excel file {i}/{num_res}")
    excel_data = BytesIO(r.content)
    dfs = pd.read_excel(excel_data, sheet_name=None)
    for table, df in dfs.items():
        print(df.head)
        df.to_sql(name + "_" + table, db.conn)




def fetch_dataset(id: str, task_id: str):
    logger = RecordLogger(id, task_id)
    logger.set_status("fetching dataset")
    datagv_meta = get_metadata(id)
    db_file = ds_dir / f"{id}.db"
    db = Database(db_file, recreate=True)
    db.enable_wal()

    meta_obj = Record(
        id=id,
        title=datagv_meta["title"],
        publisher=datagv_meta["publisher"],
        notes=datagv_meta["notes"],

        license_citation=datagv_meta["license_citation"] if "license_citation" in datagv_meta else None,
        license_id=datagv_meta["license_id"],
        license_title=datagv_meta["license_title"],
        license_url=datagv_meta["license_url"],

        maintainer=datagv_meta["maintainer"],
        metadata_created=datagv_meta["metadata_created"],
        metadata_modified=datagv_meta["metadata_modified"],
        metadata_linkage=datagv_meta["metadata_linkage"] if "metadata_linkage" in datagv_meta else None,

        attribute_description=datagv_meta["attribute_description"] if "attribute_description" in datagv_meta else None,
        geographic_toponym=datagv_meta["geographic_toponym"] if "geographic_toponym" in datagv_meta else None,
        tags=[p["display_name"] for p in datagv_meta["tags"]],
        api_data=datagv_meta,
    )
    num_res = len(datagv_meta["resources"])
    for i, res in enumerate(datagv_meta["resources"]):
        logger.set_status(f"fetching resource {i}/{num_res}")

        format = res["format"]
        format = format_normalizer(format)
        name = res["name"]
        if format not in ["CSV", "XLSX", "XLS", "JSON"]:
            logger.set_status(f"skipping {name} ({format})")
            continue
        url = res["url"]
        url = fix_url(url)
        if format == "JSON":
            if "www.parlament.gv.at" not in url:
                continue

        if not allowed_fetch_url(url):
            logger.set_status(f"skipping resource {i}/{num_res}")

        if format == "JSON":
            encoding = ""
            import_parlament(db, id, logger, name, i, num_res)
        else:
            r = s.get(url)
            r.raise_for_status()
            logger.set_status(f"importing resource {i}/{num_res}")
            if format == "CSV":
                encoding = import_csv(db, r, logger, name, i, num_res)
            elif format in ["XLSX", "XLS"]:
                encoding = format
                import_xlsx(db, r, logger, name, i, num_res)
            else:
                raise RuntimeError(f"unsupported format {format}")

        meta_res = Resource(
            id=res["id"],
            record=meta_obj,
            format=format,
            name=name,
            url=url,
            mimetype=res["mimetype"],
            position=res["position"],
            encoding=encoding,
            last_fetched=datetime.now(),
        )
        meta_db.upsert_resource(res["id"], meta_res)

    logger.set_status(f"adding indices {i}/{num_res}")
    for tab in db.tables:
        for col in tab.columns:
            coldet = tab.analyze_column(col.name, most_common=False, least_common=False)
            if coldet.num_distinct < 50 < coldet.total_rows:
                tab.create_index([col.name])
        # print("enable FTS")
        # tab.enable_fts(["PFAD"])

    # db.index_foreign_keys()
    logger.set_status("optimizing database")

    db.analyze()
    db.disable_wal()
    db.vacuum()
    # https://til.simonwillison.net/sqlite/database-file-size
    curr = db.execute("select page_size * page_count from pragma_page_count(), pragma_page_size()")
    db_size = curr.fetchone()[0]
    db.close()

    inspect_data = run_datasette_inspect(f"{id}.db")
    meta_obj.db_size = db_size
    meta_obj.compressed_size = zstd_file_size(db_file)
    meta_obj.inspect_data = inspect_data
    meta_db.upsert_record(id, meta_obj)
    logger.set_status("done")


def delete_dataset(id: str):
    assert "." not in id
    with meta_db.conn:
        dataset_file = ds_dir / f"{id}.db"
        assert dataset_file.exists()
        dataset_file.unlink()

        meta_db.conn.execute("DELETE FROM records WHERE id = ?", (id,))
        restart_datasette_process()


if __name__ == '__main__':
    id = sys.argv[1]
    try:
        delete_dataset(id)
    except Exception as e:
        print(e)
    fetch_dataset(id, "no_task")
    create_ds_metadata()
    restart_datasette_process()
