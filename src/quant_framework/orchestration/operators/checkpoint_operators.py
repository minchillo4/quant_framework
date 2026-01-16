"""
Checkpoint Operators - State Management
=======================================

Operators for reading, writing, and managing checkpoint state.
"""

import logging
from datetime import datetime
from typing import Any

from quant_framework.orchestration.operators.base import (
    BaseDataOperator,
)
from quant_framework.orchestration.ports import ICheckpointStore

logger = logging.getLogger(__name__)


class CheckpointReaderOperator(BaseDataOperator):
    """
    Operator for reading checkpoint state.

    Loads checkpoint for a data stream to determine resume point.
    """

    def __init__(
        self,
        task_id: str,
        checkpoint_store: ICheckpointStore,
        symbol: str,
        timeframe: str,
        data_type: str,
        source: str,
        **kwargs,
    ):
        """
        Initialize checkpoint reader operator.

        Args:
            task_id: Task identifier
            checkpoint_store: Checkpoint storage
            symbol: Symbol identifier
            timeframe: Timeframe
            data_type: Type of data
            source: Data source
            **kwargs: Additional args for BaseDataOperator
        """
        super().__init__(task_id, **kwargs)
        self.checkpoint_store = checkpoint_store
        self.symbol = symbol
        self.timeframe = timeframe
        self.data_type = data_type
        self.source = source

    async def execute(self, context: dict[str, Any] | None = None) -> dict[str, Any]:
        """
        Execute checkpoint read.

        Args:
            context: Airflow context dictionary

        Returns:
            Result dictionary with checkpoint data
        """
        start_time = datetime.utcnow()

        self.log_info(
            f"Loading checkpoint for {self.symbol}/{self.timeframe}/{self.data_type}"
        )

        try:
            checkpoint_data = await self.checkpoint_store.load_checkpoint(
                symbol=self.symbol,
                timeframe=self.timeframe,
                data_type=self.data_type,
                source=self.source,
            )

            duration = (datetime.utcnow() - start_time).total_seconds()

            if checkpoint_data:
                last_ts = checkpoint_data.get("last_timestamp")
                last_date = (
                    datetime.utcfromtimestamp(last_ts / 1000) if last_ts else None
                )

                self.log_info(f"Checkpoint found: last_timestamp={last_date}")
            else:
                self.log_info("No checkpoint found (cold start)")

            return self.create_result(
                success=True,
                duration=duration,
                checkpoint_data=checkpoint_data,
                has_checkpoint=checkpoint_data is not None,
            ).to_dict()

        except Exception as e:
            duration = (datetime.utcnow() - start_time).total_seconds()
            error_msg = f"Checkpoint read failed: {str(e)}"
            self.log_error(error_msg)

            return self.create_result(
                success=False,
                duration=duration,
                error=error_msg,
            ).to_dict()


class CheckpointWriterOperator(BaseDataOperator):
    """
    Operator for writing checkpoint state.

    Saves checkpoint after successful data processing.
    """

    def __init__(
        self,
        task_id: str,
        checkpoint_store: ICheckpointStore,
        symbol: str,
        timeframe: str,
        data_type: str,
        source: str,
        **kwargs,
    ):
        """
        Initialize checkpoint writer operator.

        Args:
            task_id: Task identifier
            checkpoint_store: Checkpoint storage
            symbol: Symbol identifier
            timeframe: Timeframe
            data_type: Type of data
            source: Data source
            **kwargs: Additional args for BaseDataOperator
        """
        super().__init__(task_id, **kwargs)
        self.checkpoint_store = checkpoint_store
        self.symbol = symbol
        self.timeframe = timeframe
        self.data_type = data_type
        self.source = source

    async def execute(
        self,
        context: dict[str, Any] | None = None,
        last_timestamp: int | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Execute checkpoint write.

        Args:
            context: Airflow context dictionary
            last_timestamp: Last processed timestamp (Unix ms)
            metadata: Additional checkpoint metadata

        Returns:
            Result dictionary
        """
        start_time = datetime.utcnow()

        # Get last_timestamp from XCom if not provided
        if last_timestamp is None and context:
            ti = context.get("ti")
            if ti:
                upstream_result = ti.xcom_pull(task_ids=None)
                if isinstance(upstream_result, dict):
                    last_timestamp = upstream_result.get("last_timestamp")
                    metadata = upstream_result.get("metadata", metadata)

        if last_timestamp is None:
            error_msg = "No last_timestamp provided for checkpoint"
            self.log_error(error_msg)
            return self.create_result(
                success=False,
                error=error_msg,
            ).to_dict()

        last_date = datetime.utcfromtimestamp(last_timestamp / 1000)
        self.log_info(
            f"Saving checkpoint for {self.symbol}/{self.timeframe}/{self.data_type} "
            f"at {last_date}"
        )

        try:
            success = await self.checkpoint_store.save_checkpoint(
                symbol=self.symbol,
                timeframe=self.timeframe,
                data_type=self.data_type,
                source=self.source,
                last_timestamp=last_timestamp,
                metadata=metadata,
            )

            duration = (datetime.utcnow() - start_time).total_seconds()

            if success:
                self.log_info(f"Checkpoint saved successfully in {duration:.2f}s")
            else:
                self.log_warning("Checkpoint save returned False")

            return self.create_result(
                success=success,
                duration=duration,
                last_timestamp=last_timestamp,
            ).to_dict()

        except Exception as e:
            duration = (datetime.utcnow() - start_time).total_seconds()
            error_msg = f"Checkpoint write failed: {str(e)}"
            self.log_error(error_msg)

            return self.create_result(
                success=False,
                duration=duration,
                error=error_msg,
            ).to_dict()


class CheckpointDeleteOperator(BaseDataOperator):
    """
    Operator for deleting checkpoint state.

    Useful for forcing cold start or cleanup.
    """

    def __init__(
        self,
        task_id: str,
        checkpoint_store: ICheckpointStore,
        symbol: str,
        timeframe: str,
        data_type: str,
        source: str,
        **kwargs,
    ):
        """
        Initialize checkpoint delete operator.

        Args:
            task_id: Task identifier
            checkpoint_store: Checkpoint storage
            symbol: Symbol identifier
            timeframe: Timeframe
            data_type: Type of data
            source: Data source
            **kwargs: Additional args for BaseDataOperator
        """
        super().__init__(task_id, **kwargs)
        self.checkpoint_store = checkpoint_store
        self.symbol = symbol
        self.timeframe = timeframe
        self.data_type = data_type
        self.source = source

    async def execute(self, context: dict[str, Any] | None = None) -> dict[str, Any]:
        """
        Execute checkpoint delete.

        Args:
            context: Airflow context dictionary

        Returns:
            Result dictionary
        """
        start_time = datetime.utcnow()

        self.log_info(
            f"Deleting checkpoint for {self.symbol}/{self.timeframe}/{self.data_type}"
        )

        try:
            success = await self.checkpoint_store.delete_checkpoint(
                symbol=self.symbol,
                timeframe=self.timeframe,
                data_type=self.data_type,
                source=self.source,
            )

            duration = (datetime.utcnow() - start_time).total_seconds()

            if success:
                self.log_info(f"Checkpoint deleted successfully in {duration:.2f}s")
            else:
                self.log_warning("Checkpoint delete returned False (may not exist)")

            return self.create_result(
                success=True,  # Not finding checkpoint is OK
                duration=duration,
            ).to_dict()

        except Exception as e:
            duration = (datetime.utcnow() - start_time).total_seconds()
            error_msg = f"Checkpoint delete failed: {str(e)}"
            self.log_error(error_msg)

            return self.create_result(
                success=False,
                duration=duration,
                error=error_msg,
            ).to_dict()
