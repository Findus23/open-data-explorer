import os
import socket
from typing import TypeVar

from pydantic import HttpUrl, TypeAdapter, BaseModel

http_url_adapter = TypeAdapter(HttpUrl)
# Url = Annotated[str, BeforeValidator(lambda value: str(http_url_adapter.validate_python(value)))]
Url = str

T = TypeVar('T', bound=BaseModel)


def merge_models(base: T, override: T) -> T:
    assert type(base) == type(override)
    merged_data = {}
    for field, field_info in base.__class__.model_fields.items():
        base_val = getattr(base, field)
        override_val = getattr(override, field)
        if isinstance(base_val, dict) and isinstance(override_val, dict):
            merged = {}
            for k in list(base_val.keys()) + list(override_val.keys()):
                if k in merged:
                    # already handled before
                    continue
                if k not in base_val:
                    merged[k] = override_val[k]
                    continue
                base_v = base_val[k]
                if k not in override_val:
                    merged[k] = base_v
                    continue
                override_v = override_val[k]
                if isinstance(base_v, BaseModel):
                    merged[k] = merge_models(base_v, override_v)
                else:
                    # merged[k] = {**base_v, **override_v} # untested
                    raise NotImplementedError()
            merged_data[field] = merged
        elif isinstance(base_val, BaseModel) and isinstance(override_val, BaseModel):
            # Recursively merge nested models
            merged_data[field] = merge_models(base_val, override_val)
        elif override_val is not None:
            merged_data[field] = override_val
        else:
            merged_data[field] = base_val
    return base.__class__(**merged_data)


def sd_notify(message: str):
    notify_socket = os.getenv("NOTIFY_SOCKET")
    if not notify_socket:
        return  # Not running under systemd

    # Abstract namespace socket if starts with "@"
    if notify_socket[0] == "@":
        notify_socket = "\0" + notify_socket[1:]

    sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    try:
        sock.connect(notify_socket)
        sock.sendall(message.encode())
    finally:
        sock.close()

def pretty_byte_size(nbytes: int):
    for unit in ("", "Ki", "Mi", "Gi", "Ti"):
        if abs(nbytes) < 1024.0:
            return f"{nbytes:3.1f} {unit}B"
        nbytes /= 1024.0
