from __future__ import annotations

import importlib
import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass

__all__ = ["reload"]


def reload() -> None:
    if __package__ is None:
        return

    package = __package__.split(".", 1)[0]
    for name in tuple(sys.modules):
        if not name.startswith(package):
            continue

        module = sys.modules[name]
        importlib.reload(module)
