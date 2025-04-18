import json
import subprocess
from pathlib import Path

from meta.globals import root_dir


def restart_datasette_process() -> None:
    subprocess.run([
        "systemctl", "--user", "restart", "ode-datasette"
    ], check=True)


def run_datasette_inspect(file_name:str) -> str:
    out = subprocess.run([
        "datasette", "inspect", file_name,
    ], cwd=root_dir / "ds", check=True, capture_output=True)

    lines = out.stdout.decode().splitlines()
    offset = 0
    for line in lines:
        if line.startswith("{"):
            break
        offset += 1
    json_string = "\n".join(lines[offset:])
    data = json.loads(json_string)
    actual_data = next(iter(data.values()))
    return json.dumps(actual_data)


