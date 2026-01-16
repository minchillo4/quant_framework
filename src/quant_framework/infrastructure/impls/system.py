"""Default implementations of infrastructure abstractions."""

from datetime import datetime
from pathlib import Path

from quant_framework.infrastructure.ports.system import (
    IClock,
    IConfigPathResolver,
    IFileSystem,
)


class SystemClock(IClock):
    """Default implementation using system time."""

    def now(self) -> datetime:
        """Get current local time."""
        return datetime.now()

    def utcnow(self) -> datetime:
        """Get current UTC time."""
        return datetime.utcnow()


class OsFileSystem(IFileSystem):
    """Default implementation using OS file system."""

    def read_text(self, path: Path) -> str:
        """Read file as text."""
        return path.read_text(encoding="utf-8")

    def write_text(self, path: Path, content: str) -> None:
        """Write text to file, creating parent directories."""
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")

    def exists(self, path: Path) -> bool:
        """Check if path exists."""
        return path.exists()

    def mkdir(self, path: Path, parents: bool = True) -> None:
        """Create directory."""
        path.mkdir(parents=parents, exist_ok=True)


class ProjectConfigPathResolver(IConfigPathResolver):
    """Resolve config paths relative to project root."""

    def __init__(self, project_root: Path | None = None):
        """Initialize resolver.

        Args:
            project_root: Path to project root. If None, derives from file location.
        """
        if project_root is None:
            # Navigate from this file's location to project root
            # src/quant_framework/infrastructure/impls/system.py -> project root
            current_file = Path(__file__)
            project_root = current_file.parent.parent.parent.parent.parent.parent
        self._project_root = project_root

    def resolve_project_root(self) -> Path:
        """Get project root directory."""
        return self._project_root

    def resolve_config_file(self, relative_path: str) -> Path:
        """Resolve config file path relative to config directory."""
        return self._project_root / "config" / relative_path

    def resolve_env_config_file(self, env: str) -> Path:
        """Resolve environment-specific config file."""
        return self._project_root / "config" / "env" / f"{env}.yaml"
