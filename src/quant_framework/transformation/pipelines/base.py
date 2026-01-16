"""Base transformation pipeline for orchestrating data processing workflows.

Provides:
- BaseTransformationPipeline: Abstract template for fetch → normalize → validate → store
- Error handling and retry logic
- Progress tracking and logging
- Transaction management
"""

import logging
import time
from typing import Any, Protocol

from quant_framework.transformation.ports import (
    IDataNormalizer,
    IDataValidator,
)

logger = logging.getLogger(__name__)


class IRepository(Protocol):
    """Repository protocol for batch saves."""

    async def save_batch(self, records: list[Any]) -> int:
        """Save batch of records to storage.

        Args:
            records: List of records to save

        Returns:
            Number of records successfully saved
        """
        ...


class PipelineError(Exception):
    """Raised when pipeline execution fails."""

    pass


class BaseTransformationPipeline:
    """Base pipeline for end-to-end data transformation.

    Orchestrates: Normalize → Validate → Store

    Subclasses override:
    - data_type property: Type of data (ohlcv, open_interest, trade)
    """

    def __init__(
        self,
        normalizer: IDataNormalizer,
        validator: IDataValidator,
        repository: IRepository,
        discard_invalid: bool = False,
    ):
        """Initialize transformation pipeline.

        Args:
            normalizer: Data normalizer (converts API response → schema object)
            validator: Data validator (checks quality before persistence)
            repository: Storage repository (persists validated records)
            discard_invalid: If True, discard invalid records. If False, raise error on validation failure.
        """
        self.normalizer = normalizer
        self.validator = validator
        self.repository = repository
        self.discard_invalid = discard_invalid

    @property
    def data_type(self) -> str:
        """Type of data being processed (ohlcv, open_interest, trade)."""
        raise NotImplementedError(
            f"{self.__class__.__name__} must define data_type property"
        )

    async def transform_and_store(
        self,
        raw_data: Any,
    ) -> dict[str, Any]:
        """Execute complete transformation pipeline.

        Args:
            raw_data: Raw API response (list of dicts/arrays)

        Returns:
            {
                "success": bool,
                "records_processed": int,
                "records_normalized": int,
                "records_valid": int,
                "records_stored": int,
                "errors": list[str],
                "duration_seconds": float,
            }

        Raises:
            PipelineError: On fatal pipeline failures
        """
        start_time = time.time()
        errors = []

        try:
            # Step 1: Normalize raw data
            logger.info(
                f"Starting {self.data_type} normalization for {len(raw_data) if isinstance(raw_data, list) else 1} records"
            )
            normalized_records = await self._normalize(raw_data)
            records_normalized = len(normalized_records)

            if not normalized_records:
                logger.warning(f"No {self.data_type} records normalized")
                return {
                    "success": False,
                    "records_processed": len(raw_data)
                    if isinstance(raw_data, list)
                    else 1,
                    "records_normalized": 0,
                    "records_valid": 0,
                    "records_stored": 0,
                    "errors": ["No records normalized"],
                    "duration_seconds": time.time() - start_time,
                }

            # Step 2: Validate normalized records
            logger.info(f"Validating {records_normalized} {self.data_type} records")
            valid_records, validation_errors = await self._validate(normalized_records)
            records_valid = len(valid_records)

            if validation_errors:
                logger.warning(
                    f"{len(validation_errors)} {self.data_type} validation errors: {validation_errors[:5]}"
                )
                errors.extend(validation_errors)

            if not valid_records:
                logger.error(f"No valid {self.data_type} records after validation")
                if not self.discard_invalid:
                    raise PipelineError(
                        f"All {records_normalized} records failed validation"
                    )
                return {
                    "success": False,
                    "records_processed": len(raw_data)
                    if isinstance(raw_data, list)
                    else 1,
                    "records_normalized": records_normalized,
                    "records_valid": 0,
                    "records_stored": 0,
                    "errors": errors,
                    "duration_seconds": time.time() - start_time,
                }

            # Step 3: Store valid records
            logger.info(f"Storing {records_valid} valid {self.data_type} records")
            records_stored = await self._store(valid_records)

            duration = time.time() - start_time
            logger.info(
                f"Pipeline complete: {records_stored}/{records_normalized} {self.data_type} records stored in {duration:.2f}s"
            )

            return {
                "success": True,
                "records_processed": len(raw_data) if isinstance(raw_data, list) else 1,
                "records_normalized": records_normalized,
                "records_valid": records_valid,
                "records_stored": records_stored,
                "errors": errors,
                "duration_seconds": duration,
            }

        except Exception as e:
            duration = time.time() - start_time
            logger.error(
                f"{self.data_type} pipeline failed after {duration:.2f}s: {str(e)}",
                exc_info=True,
            )
            raise PipelineError(f"{self.data_type} pipeline failed: {str(e)}") from e

    async def _normalize(self, raw_data: Any) -> list[Any]:
        """Normalize raw API data to schema objects.

        Args:
            raw_data: Raw API response

        Returns:
            List of normalized schema objects

        Raises:
            PipelineError: If normalization fails
        """
        try:
            if isinstance(raw_data, list):
                return await self.normalizer.normalize_batch(raw_data)
            else:
                record = await self.normalizer.normalize_single(raw_data)
                return [record]
        except Exception as e:
            raise PipelineError(f"Normalization failed: {str(e)}") from e

    async def _validate(self, records: list[Any]) -> tuple[list[Any], list[str]]:
        """Validate normalized records.

        Args:
            records: List of normalized records

        Returns:
            (valid_records, error_messages)

        Raises:
            PipelineError: If validation process fails (not if records are invalid)
        """
        try:
            validities, error_lists = await self.validator.validate_batch(records)

            valid_records = []
            all_errors = []

            for i, (record, is_valid, errors) in enumerate(
                zip(records, validities, error_lists, strict=True)
            ):
                if is_valid:
                    valid_records.append(record)
                else:
                    error_msg = f"Record {i}: {'; '.join(errors)}"
                    all_errors.append(error_msg)

            return (valid_records, all_errors)
        except Exception as e:
            raise PipelineError(f"Validation failed: {str(e)}") from e

    async def _store(self, records: list[Any]) -> int:
        """Store valid records to repository.

        Args:
            records: List of valid records

        Returns:
            Number of records stored

        Raises:
            PipelineError: If storage fails
        """
        try:
            records_stored = await self.repository.save_batch(records)
            return records_stored
        except Exception as e:
            raise PipelineError(f"Storage failed: {str(e)}") from e
