from starlette.applications import Starlette
from starlette.responses import JSONResponse, RedirectResponse
from starlette.routing import Route, Mount
from starlette.staticfiles import StaticFiles
from starlette.templating import Jinja2Templates

from meta.meta_db import meta_db
from meta.progress_logger import RecordLogger
from meta.tasks import add_fetch_task, q
from meta.utils import pretty_byte_size

templates = Jinja2Templates(directory='templates')
templates.env.filters['pretty_byte_size'] = pretty_byte_size


async def home(request):
    records = meta_db.get_records()
    total_storage = meta_db.total_storage()
    print(total_storage)
    return templates.TemplateResponse(request, 'index.html', context={
        "records": records,
        "total_storage": total_storage[0] / 1024 / 1024,
        "total_storage_comp": total_storage[1] / 1024 / 1024,
    })


async def show(request):
    id = request.path_params['id']
    resource = meta_db.get_record(id=id)

    if resource is None:
        return templates.TemplateResponse(request, 'confirm_fetch.html', context={'id': id})

    record = meta_db.get_record(id=id)
    resources = meta_db.get_resources(record=record)
    tasks = meta_db.get_tasks_for_record(record=record)
    task_sets=[]
    for task in tasks:
        logging = RecordLogger.get_all_status_by_task_id(task.id)
        task_sets.append((task,logging))


    return templates.TemplateResponse(
        request, 'detail.html',
        context={
            'id': id, "record": record,
            "resources": resources, "logging": logging,
            "tasks": task_sets
        })

    # return RedirectResponse(request.url_for('fetch_start', id=id))

    # status = RecordLogger.get_latest_status_by_id(id)
    return RedirectResponse(f"/{id}/")
    # return Response(resource.model_dump_json(),media_type= "application/json")


async def fetch(request):
    id = request.path_params['id']
    # TODO: add a check to only fetch if the
    task_id = add_fetch_task(id)
    # need 303 to make POST a GET request
    return RedirectResponse(request.url_for("task_page", task_id=task_id), status_code=303)


async def task_page(request):
    task_id = request.path_params['task_id']
    return templates.TemplateResponse(request, 'fetch.html', context={'task_id': task_id})


async def task_status(request):
    task_id = request.path_params['task_id']
    try:
        task = q.get_job_status(task_id)
        status = RecordLogger.get_latest_status_by_task_id(task_id)
    except TypeError:
        task = None
        status = "unknown"

    if task and task.status == 2:
        redirect_url = request.url_for("show", id=task.record).path
    else:
        redirect_url = None
    return JSONResponse(
        {'status': status, "task_status": task.status, "task_data": task.data, "redirect_url": redirect_url})


app = Starlette(debug=True, routes=[
    Route('/', home, name='home'),
    Route('/meta/{id}', show, name='show'),
    Route('/meta/{id}/fetch', fetch, name='fetch_start', methods=["POST"]),
    Route('/meta/task/{task_id}', task_page, name='task_page'),
    Route('/meta/task/{task_id}/status', task_status, name='task_status'),
    Mount('/meta/static', app=StaticFiles(directory='static'), name="static"),
])
