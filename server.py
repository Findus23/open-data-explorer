from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route

from meta.progress_logger import RecordLogger


async def status(request):
    id = request.path_params['id']
    status = RecordLogger.get_latest_status_by_id(id)
    return JSONResponse({'status': status})
async def fetch(request):
    id = request.path_params['id']
    status = RecordLogger.get_latest_status_by_id(id)
    return JSONResponse({'status': status})


app = Starlette(debug=True, routes=[
    Route('/meta/{id}/status', status),
    Route('/meta/{id}/fetch', fetch),
])
