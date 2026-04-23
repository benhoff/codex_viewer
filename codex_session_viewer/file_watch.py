from __future__ import annotations

import sys

from agent_daemon import file_watch as _impl


sys.modules[__name__] = _impl
