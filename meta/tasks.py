import time

from meta import fetch_dataset, create_ds_metadata
from meta.meta_db import meta_sqlite_conn, meta_db
from meta.processes import restart_datasette_process
from meta.utils import sd_notify
from very_simple_task_queue import Queue

q = Queue(meta_sqlite_conn)


def add_fetch_task(id: str) -> str:
    if meta_db.total_storage()[1] / 1024 / 1024 > 500:
        raise Exception("out of disk space")
    if meta_db.get_resource(id) is not None:
        raise Exception("resource already exists")
    return q.put({
        "task_type": "fetch_task",
        "properties": {}
    }, record=id)


def start_task_runner():
    sd_notify('READY=1')
    while True:
        job = q.get_next_job()
        if job is None:
            time.sleep(.25)
            continue
        print(job)
        if job.data["task_type"] == "fetch_task":
            fetch_dataset(job.record, task_id=job.id)
            create_ds_metadata()
            restart_datasette_process()
            q.set_job_done(job)


if __name__ == '__main__':
    start_task_runner()
