"""Storage backends for sigil."""

from sigil.storage.base import StorageBackend
from sigil.storage.local import LocalStorage

__all__ = ["StorageBackend", "LocalStorage"]
