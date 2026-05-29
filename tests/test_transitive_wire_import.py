"""Pin: importing sqlalchemydqlite eagerly imports dqlitewire, so the wire-layer
free-threading guard is inherited; lazifying the import would silently drop it here."""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path


def test_dqlitewire_loaded_after_sqlalchemydqlite_import() -> None:
    repo_src = Path(__file__).resolve().parent.parent / "src"
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(
        [str(repo_src)] + ([env["PYTHONPATH"]] if env.get("PYTHONPATH") else [])
    )
    snippet = """
        import sys
        assert "dqlitewire" not in sys.modules
        import sqlalchemydqlite  # noqa: F401
        if "dqlitewire" not in sys.modules:
            print("FAIL: sqlalchemydqlite did not transitively load dqlitewire", flush=True)
            sys.exit(1)
        print("OK", flush=True)
    """
    result = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(snippet)],
        capture_output=True,
        text=True,
        env=env,
    )
    assert result.returncode == 0, (
        f"transitive-wire-import pin failed:\nstdout={result.stdout}\nstderr={result.stderr}"
    )
    assert "OK" in result.stdout
