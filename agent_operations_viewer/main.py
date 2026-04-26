from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEPENDENCY_ROOT = PROJECT_ROOT / ".deps"
if DEPENDENCY_ROOT.exists():
    sys.path.insert(0, str(DEPENDENCY_ROOT))

from .web.app import create_app


app = create_app()
