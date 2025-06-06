from starlette.applications import Starlette
from starlette.responses import JSONResponse, RedirectResponse
from starlette.routing import Route
from starlette.templating import Jinja2Templates

from meta.meta_db import meta_db
from meta.progress_logger import RecordLogger
from meta.tasks import add_fetch_task, q

templates = Jinja2Templates(directory='templates')


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
        task_status, task_data = q.get_job_status(task_id)
        status = RecordLogger.get_latest_status_by_task_id(task_id)
    except TypeError:
        task_status, task_data = None, None
        status = "unknown"

    if task_status == 2:
        redirect_url = request.url_for("show", id=task_data["properties"]["id"]).path
    else:
        redirect_url = None
    return JSONResponse(
        {'status': status, "task_status": task_status, "task_data": task_data, "redirect_url": redirect_url})


app = Starlette(debug=True, routes=[
    Route('/', home, name='home'),
    Route('/meta/{id}', show, name='show'),
    Route('/meta/{id}/fetch', fetch, name='fetch_start', methods=["POST"]),
    Route('/meta/task/{task_id}', task_page, name='task_page'),
    Route('/meta/task/{task_id}/status', task_status, name='task_status'),
])
