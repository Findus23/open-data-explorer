import json
from datetime import datetime
from sqlite3 import Connection
from typing import Optional

from pydantic import BaseModel
from sqlite_utils import Database

from .utils import Url


class Record(BaseModel):
    id: str
    title: str
    publisher: str
    notes: str
    license_citation: str
    license_id: str
    license_title: str
    license_url: Url
    maintainer: str
    metadata_linkage: Url
    metadata_created: str
    metadata_modified: str
    attribute_description: str
    geographic_toponym: str
    tags: list[str]
    api_data: dict

    @property
    def datagvurl(self):
        return "https://www.data.gv.at/katalog/dataset/" + self.id


class Resource(BaseModel):
    id: str
    record: Record|str
    format: str
    name: str
    url: Url
    mimetype: Optional[str]
    position: int
    last_fetched: datetime


class MetaDatabase:
    def __init__(self, db: Database):
        self.db = db
        self.conn: Connection = db.conn
        self.records = self.db["records"]
        self.resources = self.db["resources"]

    def upsert_record(self, id: str, data: Record):
        assert id == data.id
        as_dict = Record.model_dump(data)
        return self.records.upsert(as_dict, pk="id")

    def upsert_resource(self, id, data: Resource):
        assert id == data.id
        as_dict = Resource.model_dump(data)
        as_dict["record"] = data.record.id
        return self.resources.upsert(as_dict, pk="id", foreign_keys=["record"])

    def get_resources(self, record: Record) -> list[Resource]:
        return [Resource(**row) for row in self.db.query("SELECT * FROM resources where record= ?", [record.id])]

    def self_rec_row_to_record(self, row: dict) -> Record:
        row["tags"] = json.loads(row["tags"])
        row["api_data"] = json.loads(row["api_data"])
        return Record(**row)

    def get_records(self) -> list[Record]:
        return [self.self_rec_row_to_record(row) for row in self.records.rows]
