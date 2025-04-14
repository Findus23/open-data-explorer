import time

from meta import fetch_dataset
from meta.utils import sd_notify
from very_simple_task_queue import Queue
from .globals import root_dir

q = Queue(root_dir / "ds" / "queue.db")


def add_fetch_task(id: str):
    q.put({
        "task_type": "fetch_task",
        "properties": {"id": id}
    })


def start_task_runner():
    sd_notify('READY=1')
    while True:
        job = q.get_next_job()
        if job is None:
            time.sleep(.25)
            continue
        print(job)
        if job.data["task_type"] == "fetch_task":
            fetch_dataset(job.data["properties"]["id"])
            q.set_job_done(job)


if __name__ == '__main__':
    start_task_runner()
