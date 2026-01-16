"""
Settings bridge for quant_framework.

This module re-exports the global settings from the main config module,
allowing quant_framework components to import settings without circular dependencies.

Usage:
    from quant_framework.infrastructure.config.settings import settings
"""

from config.settings import config_loader, config_registry, settings

__all__ = ["settings", "config_loader", "config_registry"]
