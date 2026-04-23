from __future__ import annotations

import sys

from agent_daemon import remote_sync as _impl


sys.modules[__name__] = _impl
