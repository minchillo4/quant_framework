"""System infrastructure port definitions."""

from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path


class IClock(ABC):
    """Abstract interface for clock operations."""

    @abstractmethod
    def now(self) -> datetime:
        """Get current local time."""
        ...

    @abstractmethod
    def utcnow(self) -> datetime:
        """Get current UTC time."""
        ...


class IFileSystem(ABC):
    """Abstract interface for file system operations."""

    @abstractmethod
    def read_text(self, path: Path) -> str:
        """Read file as text."""
        ...

    @abstractmethod
    def write_text(self, path: Path, content: str) -> None:
        """Write text to file."""
        ...

    @abstractmethod
    def exists(self, path: Path) -> bool:
        """Check if path exists."""
        ...

    @abstractmethod
    def mkdir(self, path: Path, parents: bool = True) -> None:
        """Create directory."""
        ...


class IConfigPathResolver(ABC):
    """Abstract interface for resolving configuration file paths."""

    @abstractmethod
    def resolve_project_root(self) -> Path:
        """Get project root directory."""
        ...

    @abstractmethod
    def resolve_config_file(self, relative_path: str) -> Path:
        """Resolve config file path relative to config directory."""
        ...

    @abstractmethod
    def resolve_env_config_file(self, env: str) -> Path:
        """Resolve environment-specific config file."""
        ...
