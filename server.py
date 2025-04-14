from starlette.applications import Starlette
from starlette.responses import JSONResponse, RedirectResponse, Response
from starlette.routing import Route
from starlette.templating import Jinja2Templates

from meta import fetch_dataset
from meta.meta_db import meta_db
from meta.progress_logger import RecordLogger
from meta.tasks import add_fetch_task

templates = Jinja2Templates(directory='templates')

async def status(request):
    id = request.path_params['id']
    status = RecordLogger.get_latest_status_by_id(id)
    return JSONResponse({'status': status})
async def show(request):
    id = request.path_params['id']
    resource=meta_db.get_record(id=id)


    if resource is None:
        ...
        # return RedirectResponse(request.url_for('fetch',id=id))

    # status = RecordLogger.get_latest_status_by_id(id)
    return Response(resource.model_dump_json(),media_type= "application/json")

async def fetch(request):
    id = request.path_params['id']
    add_fetch_task(id)
    return templates.TemplateResponse(request, 'index.html',context={'id':id})


app = Starlette(debug=True, routes=[
    Route('/meta/{id}/status', status),
    Route('/meta/{id}/fetch', fetch,name='fetch'),
    Route('/meta/{id}', show),
])
