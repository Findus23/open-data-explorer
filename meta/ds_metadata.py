import json
from typing import Optional

import yaml
from pydantic import BaseModel

from .globals import root_dir
from .meta_db import meta_db
from .utils import merge_models, Url

metadata_dir = root_dir / "metadata"
schema_file = metadata_dir / "schema.json"
metadata_output_file = root_dir / "ds" / "metadata.yaml"
datasette_conf_output_file = root_dir / "ds" / "datasette.yaml"
inspect_output_file = root_dir / "ds" / "inspect-data.json"


class CannedQuery(BaseModel):
    sql: str
    title: Optional[str] = None
    parameters: Optional[list[str]] = None


class TableMeta(BaseModel):
    source: Optional[str] = None
    source_url: Optional[Url] = None
    license: Optional[str] = None
    license_url: Optional[Url] = None
    about: Optional[str] = None
    about_url: Optional[Url] = None
    description: Optional[str] = None  # undocumented
    hidden: Optional[bool] = None
    sort: Optional[str] = None
    sort_desc: Optional[str] = None
    size: Optional[int] = None
    sortable_columns: Optional[list[str]] = None
    label_column: Optional[str] = None
    facets: Optional[list[str]] = None  # technically more formats supported
    columns: Optional[dict[str, str]] = None


class DatabaseMeta(BaseModel):
    source: Optional[str] = None
    source_url: Optional[Url] = None
    license: Optional[str] = None
    license_url: Optional[Url] = None
    about: Optional[str] = None
    about_url: Optional[Url] = None
    title: Optional[str] = None  # undocumented
    description: Optional[str] = None  # undocumented
    tables: dict[str, TableMeta] = {}
    queries: Optional[dict[str, CannedQuery]] = None


class MetaData(BaseModel):
    title: Optional[str] = None
    description: Optional[str] = None
    description_url: Optional[Url] = None
    license: Optional[str] = None
    license_url: Optional[Url] = None
    source: Optional[str] = None
    source_url: Optional[Url] = None
    databases: dict[str, DatabaseMeta] = {}


def create_schema_file():
    with schema_file.open("w") as f:
        print(type(DatabaseMeta.model_json_schema()))
        json.dump(DatabaseMeta.model_json_schema(), f, indent=2, ensure_ascii=False)


def create_ds_metadata():
    create_schema_file()
    metadata = MetaData()
    with (metadata_dir / "datasette.yaml").open() as f:
        datasette_conf = yaml.safe_load(f)
        datasette_conf["databases"] = {}
    inspect_data = {}

    for record in meta_db.get_records():
        inspect_data[record.id] = record.inspect_data
        db_meta = DatabaseMeta(
            source=record.publisher,
            source_url=record.datagvurl,
            license=record.license_title,
            license_url=record.license_url,
            about=record.maintainer,
            about_url=record.metadata_linkage,
            title=record.title,
            description=record.notes,  # add more in the future
        )
        for resource in meta_db.get_resources(record):
            res_meta = TableMeta(
                source=record.publisher,
                source_url=record.datagvurl,
                license=record.license_title,
                license_url=record.license_url,
                about=record.maintainer,
                about_url=record.metadata_linkage,
            )
            db_meta.tables[resource.name] = res_meta
        override_file = metadata_dir / f"{record.id}.yaml"
        if override_file.exists():
            with override_file.open() as f:
                data = yaml.safe_load(f)

                metadata_from_file = DatabaseMeta(**data)
                print("overriding from file")
                print(metadata_from_file)
                db_meta = merge_models(db_meta, metadata_from_file)
        metadata.databases[record.id] = db_meta

    with (metadata_dir / "meta_db.yaml").open() as f:
        data = yaml.safe_load(f)
        metadata_from_file = DatabaseMeta(**data)
        metadata.databases["meta_db"] = metadata_from_file

    for db_id, db_meta in metadata.databases.items():
        if db_meta.queries is not None:
            datasette_conf["databases"][db_id] = {
                "queries": {k: v.model_dump(exclude_none=True) for k, v in db_meta.queries.items()}
            }
            metadata.databases[db_id].queries = None

    with metadata_output_file.open("w") as f:
        yaml.dump(metadata.model_dump(exclude_none=True), f, sort_keys=False)
    with datasette_conf_output_file.open("w") as f:
        yaml.dump(datasette_conf, f, sort_keys=False)

    inspect_dict = {}
    for id, insp_str in inspect_data.items():
        insp_data = json.loads(insp_str)
        inspect_dict[id] = insp_data

    with inspect_output_file.open("w") as f:
        json.dump(inspect_dict, f, indent=2, ensure_ascii=False)


if __name__ == '__main__':
    create_ds_metadata()
