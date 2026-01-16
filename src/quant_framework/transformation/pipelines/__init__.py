"""Transformation pipelines for end-to-end data processing.

This subdirectory contains orchestration pipelines:
- ohlcv.py: OHLCVTransformationPipeline
  - Fetch OHLCV from API → Normalize → Validate → Store
- open_interest.py: OpenInterestPipeline
  - Fetch OI from API → Normalize → Validate → Store
- trades.py: TradeTransformationPipeline
  - Fetch trades from API → Normalize → Validate → Store
- base.py: BaseTransformationPipeline (abstract orchestration template)

Each pipeline handles:
- Error handling and retry logic
- Batch operation optimization (1000+ records)
- Progress logging and monitoring
- Transaction management
"""

from quant_framework.transformation.pipelines.base import (
    BaseTransformationPipeline,
    PipelineError,
)
from quant_framework.transformation.pipelines.ohlcv import OHLCVTransformationPipeline
from quant_framework.transformation.pipelines.open_interest import OpenInterestPipeline
from quant_framework.transformation.pipelines.trades import TradeTransformationPipeline

__all__ = [
    "BaseTransformationPipeline",
    "PipelineError",
    "OHLCVTransformationPipeline",
    "OpenInterestPipeline",
    "TradeTransformationPipeline",
]
