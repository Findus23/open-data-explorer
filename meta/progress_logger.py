from datetime import datetime

from .meta_db import meta_db


class RecordLogger():
    def __init__(self, id: str):
        self.id = id
        self.log = meta_db.db["logging"]

    def set_status(self, status: str):
        self.log.insert({
            "id": self.id,
            "status": status,
            "timestamp": datetime.now(),
        })

    @staticmethod
    def get_latest_status_by_id(id):
        curr = meta_db.db.execute(
            "SELECT status FROM logging where id = ? ORDER BY timestamp DESC LIMIT 1;",
            [id]
        )
        return curr.fetchone()[0]

    def get_latest_status(self):
        self.get_latest_status_by_id(self.id)
