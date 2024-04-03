from __future__ import annotations

from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    from os import PathLike
    from pathlib import Path

    from _typeshed import StrOrBytesPath

    PathType = Union[str, bytes, PathLike[str], PathLike[bytes], Path, StrOrBytesPath]

__all__ = ["PathType"]
