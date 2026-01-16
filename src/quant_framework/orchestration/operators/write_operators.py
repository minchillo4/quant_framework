"""
Write Operators - Data Persistence
==================================

Operators for writing data to bronze and silver layers.
"""

import logging
from datetime import datetime
from typing import Any

from quant_framework.orchestration.operators.base import (
    BaseDataOperator,
)

logger = logging.getLogger(__name__)


class WriteBronzeOperator(BaseDataOperator):
    """
    Operator for writing data to bronze layer (MinIO/S3).

    Writes raw data to Parquet files with Pydantic validation.
    """

    def __init__(
        self,
        task_id: str,
        writer: Any,  # BronzeWriter instance
        symbol: str,
        timeframe: str,
        data_type: str,
        exchange: str,
        date: datetime | None = None,
        **kwargs,
    ):
        """
        Initialize bronze write operator.

        Args:
            task_id: Task identifier
            writer: BronzeWriter instance
            symbol: Symbol identifier
            timeframe: Timeframe
            data_type: Type of data ("ohlcv", "open_interest")
            exchange: Exchange identifier
            date: Date for file naming (default: today)
            **kwargs: Additional args for BaseDataOperator
        """
        super().__init__(task_id, **kwargs)
        self.writer = writer
        self.symbol = symbol
        self.timeframe = timeframe
        self.data_type = data_type
        self.exchange = exchange
        self.date = date or datetime.utcnow()

    async def execute(
        self,
        context: dict[str, Any] | None = None,
        data: list[dict] | None = None,
    ) -> dict[str, Any]:
        """
        Execute bronze write.

        Args:
            context: Airflow context dictionary
            data: Data to write (or pulled from XCom if None)

        Returns:
            Result dictionary for XCom
        """
        start_time = datetime.utcnow()

        # Pull data from XCom if not provided
        if data is None and context:
            ti = context.get("ti")
            if ti:
                # Try to pull from previous task
                upstream_result = ti.xcom_pull(task_ids=None)
                if isinstance(upstream_result, dict):
                    data = upstream_result.get("data")

        if not data:
            error_msg = "No data provided or available in XCom"
            self.log_warning(error_msg)
            return self.create_result(
                success=True,  # Not an error, just no data
                records_processed=0,
                duration=0.0,
                error=None,
            ).to_dict()

        self.log_info(
            f"Writing {len(data)} records to bronze: "
            f"{self.symbol}/{self.timeframe}/{self.data_type}"
        )

        try:
            # Write with retry
            result = await self.execute_with_retry(
                self.writer.write_daily_batch,
                data=data,
                symbol=self.symbol,
                timeframe=self.timeframe,
                data_type=self.data_type,
                exchange=self.exchange,
                date=self.date,
            )

            duration = (datetime.utcnow() - start_time).total_seconds()

            if result.get("success"):
                records_written = result.get("records_written", 0)
                file_path = result.get("file_path")

                self.log_info(
                    f"Successfully wrote {records_written} records to {file_path} "
                    f"in {duration:.2f}s"
                )

                return self.create_result(
                    success=True,
                    records_processed=records_written,
                    duration=duration,
                    file_path=file_path,
                ).to_dict()
            else:
                error_msg = result.get("error", "Unknown write error")
                self.log_error(f"Bronze write failed: {error_msg}")

                return self.create_result(
                    success=False,
                    duration=duration,
                    error=error_msg,
                ).to_dict()

        except Exception as e:
            duration = (datetime.utcnow() - start_time).total_seconds()
            error_msg = f"Bronze write exception: {str(e)}"
            self.log_error(error_msg)

            return self.create_result(
                success=False,
                duration=duration,
                error=error_msg,
            ).to_dict()


class WriteSilverOperator(BaseDataOperator):
    """
    Operator for writing data to silver layer (TimescaleDB).

    Writes normalized, validated data to database repositories.
    """

    def __init__(
        self,
        task_id: str,
        repository: Any,  # Repository instance (OHLCVRepository, etc.)
        **kwargs,
    ):
        """
        Initialize silver write operator.

        Args:
            task_id: Task identifier
            repository: Repository instance
            **kwargs: Additional args for BaseDataOperator
        """
        super().__init__(task_id, **kwargs)
        self.repository = repository

    async def execute(
        self,
        context: dict[str, Any] | None = None,
        records: list[Any] | None = None,
    ) -> dict[str, Any]:
        """
        Execute silver write.

        Args:
            context: Airflow context dictionary
            records: Records to write (Pydantic models)

        Returns:
            Result dictionary for XCom
        """
        start_time = datetime.utcnow()

        # Pull records from XCom if not provided
        if records is None and context:
            ti = context.get("ti")
            if ti:
                upstream_result = ti.xcom_pull(task_ids=None)
                if isinstance(upstream_result, dict):
                    records = upstream_result.get("records")

        if not records:
            error_msg = "No records provided or available in XCom"
            self.log_warning(error_msg)
            return self.create_result(
                success=True,
                records_processed=0,
                duration=0.0,
            ).to_dict()

        self.log_info(f"Writing {len(records)} records to silver layer")

        try:
            # Write with retry
            result = await self.execute_with_retry(
                self.repository.save_batch,
                records=records,
            )

            duration = (datetime.utcnow() - start_time).total_seconds()

            # Result format varies by repository
            records_written = result if isinstance(result, int) else len(records)

            self.log_info(
                f"Successfully wrote {records_written} records to silver "
                f"in {duration:.2f}s"
            )

            return self.create_result(
                success=True,
                records_processed=records_written,
                duration=duration,
            ).to_dict()

        except Exception as e:
            duration = (datetime.utcnow() - start_time).total_seconds()
            error_msg = f"Silver write exception: {str(e)}"
            self.log_error(error_msg)

            return self.create_result(
                success=False,
                duration=duration,
                error=error_msg,
            ).to_dict()
