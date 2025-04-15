"""
This is mainly based on https://github.com/litements/litequeue

It for sure is massivly limited (especially if multiple workers would be used),
but should work for now and only uses SQLite

"""
import json
import sqlite3
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from enum import IntEnum
from pathlib import Path
from typing import Optional

from pydantic import UUID4


@dataclass(frozen=True)
class Job:
    id: str
    status: int
    data: dict
    in_time: float


class JobStatus(IntEnum):
    PENDING = 0
    IN_PROGRESS = 1
    DONE = 2


class Queue:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.init_tables()

    def init_tables(self):
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS queue (
            job_id BLOB PRIMARY KEY,
            status INTEGER NOT NULL,
            data JSON NOT NULL,
            in_time INTEGER NOT NULL,
            lock_time INTEGER
        ) WITHOUT ROWID """)

        self.conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS queue_idx ON queue (job_id)")

        self.conn.execute("CREATE INDEX IF NOT EXISTS status_idx ON queue (status)")
        self.conn.execute("PRAGMA journal_mode = WAL;")
        self.conn.execute("PRAGMA temp_store = MEMORY;")
        self.conn.execute("PRAGMA synchronous = NORMAL;")

    def put(self, data) -> None:
        job_id = uuid.uuid4()
        now = datetime.now().timestamp()
        with self.conn:
            self.conn.execute(
                """INSERT INTO queue (data, job_id, status, in_time)
    VALUES (:data, :job_id, :status, :now)""",
                {"data": json.dumps(data), "job_id": job_id.hex, "status": 0, "now": now})

    def get_next_job(self) -> Optional[Job]:
        now = datetime.now().timestamp()
        with self.conn:
            resp = self.conn.execute(
                "UPDATE queue SET status=1, lock_time = :now WHERE job_id = (SELECT job_id FROM queue WHERE status=0 ORDER BY in_time LIMIT 1) RETURNING job_id,status,data,in_time",
                {"now": now}).fetchone()
        if resp is None:
            return None
        job_id, status, data, in_time = resp
        return Job(job_id, status, json.loads(data), in_time)

    def set_job_done(self,job: Job) -> None:
        with self.conn:
            self.conn.execute("UPDATE queue SET status=2 WHERE job_id= :job_id", {"job_id": job.id})

if __name__ == '__main__':
    queue = Queue(Path('queue'))

    mode = sys.argv[1]
    if mode == "write":
        while True:
            job = input("job: ")
            queue.put(job)
    elif mode == "read":
        while True:
            job = queue.get_next_job()
            if job is None:
                time.sleep(.25)
                continue
            print(job)
