import subprocess


def restart_datasette_process() -> None:
    subprocess.run([
        "systemctl", "--user", "restart", "ode-datasette"
    ], check=True)
