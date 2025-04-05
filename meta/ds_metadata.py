import json
from pathlib import Path
from typing import Optional

import yaml
from pydantic import BaseModel

from .meta_db import MetaDatabase
from .utils import Url, merge_models

root_dir = Path(__file__).parent.parent
metadata_dir = Path(__file__).parent.parent / "metadata"
schema_file = metadata_dir / "schema.json"
metadata_output_file = root_dir / "ds" / "metadata.yaml"


class TableMeta(BaseModel):
    source: Optional[str] = None
    source_url: Optional[Url] = None
    license: Optional[str] = None
    license_url: Optional[Url] = None
    about: Optional[str] = None
    about_url: Optional[Url] = None
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
    tables: dict[str, TableMeta] = {}


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


def create_ds_metadata(db: MetaDatabase):
    create_schema_file()
    metadata = MetaData()

    for record in db.get_records():
        db_meta = DatabaseMeta(
            source=record.publisher,
            source_url=record.datagvurl,
            license=record.license_title,
            license_url=record.license_url,
            about=record.maintainer,
            about_url=record.metadata_linkage,
        )
        for resource in db.get_resources(record):
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

    print(metadata)

    with metadata_output_file.open("w") as f:
        yaml.dump(metadata.model_dump(exclude_none=True), f, sort_keys=False)
