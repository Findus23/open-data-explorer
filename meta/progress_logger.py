from datetime import datetime

from .meta_db import meta_db


class RecordLogger():
    def __init__(self, record_id: str, task_id: str = None):
        self.record_id = record_id
        self.task_id = task_id
        self.log = meta_db.db["logging"]

    def set_status(self, status: str):
        self.log.insert({
            "record_id": self.record_id,
            "task_id": self.task_id,
            "status": status,
            "timestamp": datetime.now(),
        })

    @staticmethod
    def get_latest_status_by_task_id(task_id):
        curr = meta_db.db.execute(
            "SELECT status FROM logging where task_id = ? ORDER BY timestamp DESC LIMIT 1;",
            [task_id]
        )
        return curr.fetchone()[0]

    def get_latest_status(self):
        self.get_latest_status_by_id(self.id)
