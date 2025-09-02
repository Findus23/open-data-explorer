import json
from datetime import datetime
from sqlite3 import Connection
from typing import Optional

from pydantic import BaseModel
from sqlite_utils import Database

from very_simple_task_queue import Job
from .globals import root_dir
from .utils import Url


class Record(BaseModel):
    id: str
    title: str
    publisher: str
    notes: str
    license_citation: Optional[str] = None
    license_id: Optional[str] = None
    license_title: Optional[str] = None
    license_url: Url
    maintainer: str
    metadata_linkage: Optional[Url]
    metadata_created: str
    metadata_modified: str
    attribute_description: Optional[str] = None
    geographic_toponym: Optional[str] = None
    tags: list[str]
    api_data: dict
    inspect_data: Optional[str] = None

    db_size: Optional[int] = None
    compressed_size: Optional[int] = None
    num_queries: Optional[int] = 0

    @property
    def datagvurl(self):
        return "https://www.data.gv.at/katalog/dataset/" + self.id

    @property
    def datasetteurl(self):
        return "/" + self.id


class Resource(BaseModel):
    id: str
    record: Record | str
    format: str
    name: str
    url: Url
    mimetype: Optional[str]
    position: Optional[int] = None
    encoding: Optional[str] = None
    last_fetched: datetime


class MetaDatabase:
    def __init__(self, db: Database):
        self.db = db
        self.conn: Connection = db.conn
        self.records = self.db["records"]
        self.resources = self.db["resources"]
        self.status = self.db["status"]
        self.db.enable_wal()

    def upsert_record(self, id: str, data: Record):
        assert id == data.id
        as_dict = Record.model_dump(data)
        return self.records.upsert(as_dict, pk="id", defaults={"num_queries": 0})

    def upsert_resource(self, id, data: Resource):
        assert id == data.id
        as_dict = Resource.model_dump(data)
        as_dict["record"] = data.record.id
        return self.resources.upsert(as_dict, pk="id", foreign_keys=["record"])

    def get_resources(self, record: Record) -> list[Resource]:
        return [Resource(**row) for row in self.db.query("SELECT * FROM resources where record= ?", [record.id])]

    def get_resource(self, id) -> Optional[Resource]:
        try:
            row = list(self.db.query("SELECT * FROM resources where id= ?", [id]))[0]
        except IndexError:
            return None
        return Resource(**row)

    def get_record(self, id) -> Optional[Record]:
        try:
            row = list(self.db.query("SELECT * FROM records where id= ?", [id]))[0]
        except IndexError:
            return None
        return self.self_rec_row_to_record(row)

    def self_rec_row_to_record(self, row: dict) -> Record:
        row["tags"] = json.loads(row["tags"])
        row["api_data"] = json.loads(row["api_data"])
        return Record(**row)

    def get_records(self) -> list[Record]:
        return [self.self_rec_row_to_record(row) for row in self.records.rows]

    def total_storage(self) -> tuple[int, int]:  #
        return (
            self.conn.execute("SELECT SUM(db_size) FROM records").fetchone()[0],
            self.conn.execute("SELECT SUM(compressed_size) FROM records").fetchone()[0]
        )

    def get_tasks_for_record(self, record: Record) -> list[Job]:
        conn = self.conn.execute("SELECT job_id,status,data,in_time FROM queue WHERE record= :record_id ORDER BY in_time DESC",
                                 {"record_id": record.id})

        return [Job(job_id, status, record.id, json.loads(data), in_time) for job_id, status, data, in_time in conn]


meta_sqlite_conn = Connection(root_dir / "ds/meta_db.db")

# have just one singleton object
meta_db = MetaDatabase(Database(meta_sqlite_conn))
