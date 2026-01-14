"""Infrastructure abstraction ports."""

from .system import IClock, IConfigPathResolver, IFileSystem  # noqa: F401

__all__ = [
    "IClock",
    "IFileSystem",
    "IConfigPathResolver",
]
