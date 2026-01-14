"""
Minimal conftest for big-bang replacement.
Legacy exchange/DB/Kafka fixtures removed.
"""

import logging
import sys
from pathlib import Path

# Ensure src on path
sys.path.insert(0, str(Path(__file__).parent / "src"))

logger = logging.getLogger(__name__)

# Add new fixtures here as needed for the new pipeline.
