import csv
import subprocess
import sys
import tempfile
from datetime import datetime
from io import BytesIO
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import yaml
from chardet import UniversalDetector
from requests import Response
from sqlite_utils import Database
from sqlite_utils.utils import file_progress, TypeTracker

from meta import create_ds_metadata
from meta.ds_metadata import Tweaks, TableTweaks, ResourceTweaks, CSVDialectTweak
from meta.hardcoded_fixes import fix_url, format_normalizer
from meta.offenerhaushalt import fetch_offenerhaushalt
from meta.parlament import import_parlament
from .datagv import get_metadata
from .globals import s, ds_dir, tweaks_dir
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


def create_csv_dialect(t_dialect: CSVDialectTweak) -> type(csv.Dialect):
    class CustomDialect(csv.Dialect):
        ...

    CustomDialect.delimiter = t_dialect.delimiter
    CustomDialect.doublequote = t_dialect.doublequote
    CustomDialect.escapechar = t_dialect.escapechar
    CustomDialect.lineterminator = t_dialect.lineterminator
    CustomDialect.quotechar = t_dialect.quotechar
    CustomDialect.quoting = t_dialect.quoting
    assert t_dialect.quoting == csv.QUOTE_MINIMAL
    CustomDialect.skipinitialspace = t_dialect.skipinitialspace
    CustomDialect.strict = t_dialect.strict
    return CustomDialect


def import_csv(db: Database, r: Response, logger: RecordLogger, name: str, tweaks: ResourceTweaks):
    with tempfile.TemporaryFile("w+") as f:
        if tweaks.encoding is not None:
            logger
            encoding = tweaks.encoding
        else:
            logger.set_status(f"guessing encoding")
            detector = UniversalDetector()
            for line in r.content.splitlines()[:1000]:
                detector.feed(line)
                if detector.done:
                    break
            detector.close()
            logger.set_status(f"guess result: {detector.result}")

            encoding = detector.result["encoding"]
        f.write(r.content.decode(encoding, "ignore"))

        if tweaks.csv_dialect is not None:
            dialect = create_csv_dialect(tweaks.csv_dialect)
        else:
            f.seek(0)
            logger.set_status(f"guessing CSV type")
            start = f.read(4048)
            dialect = csv.Sniffer().sniff(start)
            has_header = csv.Sniffer().has_header(start)
            print(has_header)

        logger.set_status(f"using CSV with delimiter {dialect.delimiter}")

        print(dialect)
        print(dialect.__dict__)
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

            logger.set_status(f"reading CSV file")

            docs = (dict(zip(first_row, clean_row(row))) for row in reader)
            logger.set_status(f"detecting types")

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

    tweaks_file = tweaks_dir / f"{id}.yaml"
    if tweaks_file.exists():
        with tweaks_file.open() as f:
            tweaks = Tweaks(**yaml.load(f, Loader=yaml.SafeLoader))
    else:
        tweaks = Tweaks()

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

        try:
            resource_tweaks = tweaks.resources[name]
        except KeyError:
            resource_tweaks = ResourceTweaks()
        print("resource tweaks:", resource_tweaks)

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
            if tweaks.custom_user_agent:
                print(tweaks.custom_user_agent)
                r = s.get(url, headers={"User-Agent": tweaks.custom_user_agent})
            else:
                r = s.get(url)
            r.raise_for_status()
            if url.startswith("https://offenerhaushalt.at/"):
                r = fetch_offenerhaushalt(r.content)

            if r.content.startswith(b"PK\x03\x04"):
                logger.set_status(f"detected ZIP file, skipping resource {i}/{num_res}")
                continue
            logger.set_status(f"importing resource {i}/{num_res}")
            if format == "CSV":
                encoding = import_csv(db, r, logger, name, resource_tweaks)
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

        print("tweaks")
        print(tweaks)
        try:
            table_tweaks = tweaks.tables[tab.name]
        except KeyError as e:
            print(e)
            table_tweaks = TableTweaks()
            continue
        logger.set_status(f"applying table tweaks")
        if table_tweaks.additional_indices:
            for add_idx in table_tweaks.additional_indices:
                logger.set_status(f"adding additional indices to {add_idx}")
                tab.create_index(add_idx)
        if table_tweaks.fts_indices:
            for fts_idx in table_tweaks.fts_indices:
                logger.set_status(f"adding full-text-search to columns {fts_idx}")
                tab.enable_fts(fts_idx)

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
