import subprocess
from pathlib import Path

from .globals import root_dir

extension_dir = root_dir / "extension"

bookmarklet_file = extension_dir / "script.js"
bookmarklet_redir_file = extension_dir / "bookmarklet_redir.js"


def js_file_to_bookmarklet(js_file: Path):
    out = subprocess.run([
        "esbuild",
        "--minify",
        "--format=iife",
        js_file
    ], check=True, capture_output=True)
    bookmarklet = out.stdout.decode("utf-8").strip()
    return bookmarklet


bookmarklet = js_file_to_bookmarklet(bookmarklet_file)
bookmarklet_redir = js_file_to_bookmarklet(bookmarklet_redir_file)

print(bookmarklet_redir)
