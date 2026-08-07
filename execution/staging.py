"""
execution/staging.py
Writes compiled Spark job output to a Delta staging path.
"""
from __future__ import annotations

import os
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


class StagingWriter:
    """
    Writes a Spark DataFrame to a Delta staging location.
    The staging path is separate from production and can be discarded on rejection.
    """

    def __init__(self, base_staging_path: str | None = None) -> None:
        self._base = base_staging_path or os.environ.get(
            "STAGING_DELTA_PATH", "/tmp/migrate_io/staging"
        )

    def staging_path(self, run_id: str) -> str:
        return f"{self._base}/{run_id}"

    def write(
        self,
        spark: Any,
        df: Any,
        run_id: str,
        partition_by: list[str] | None = None,
        options: dict[str, Any] | None = None,
    ) -> str:
        """
        Write `df` to a Delta staging table for this run.
        Returns the staging path.
        """
        path = self.staging_path(run_id)
        opts = options or {}
        try:
            writer = df.write.format("delta").mode("overwrite")
            if partition_by:
                writer = writer.partitionBy(*partition_by)
            for k, v in opts.items():
                writer = writer.option(k, v)
            writer.save(path)
            row_count = df.count()
            logger.info("staging.written", run_id=run_id, path=path, rows=row_count)
            return path
        except Exception as exc:
            logger.error("staging.write_failed", run_id=run_id, error=str(exc))
            raise

    def discard(self, spark: Any, run_id: str) -> None:
        """Remove staging data for this run_id."""
        path = self.staging_path(run_id)
        try:
            dbutils = spark._jvm.com.databricks.dbutils_v1.DBUtilsHolder.dbutils()
            dbutils.fs.rm(path, recurse=True)
            logger.info("staging.discarded", run_id=run_id)
        except Exception:
            # Fallback: use shutil for local runs
            import shutil
            from pathlib import Path
            local_path = Path(path)
            if local_path.exists():
                shutil.rmtree(local_path)
            logger.info("staging.discarded_local", run_id=run_id)

    def read_preview(self, spark: Any, run_id: str, limit: int = 100) -> Any:
        """Read a preview slice from the staging Delta table."""
        path = self.staging_path(run_id)
        return spark.read.format("delta").load(path).limit(limit)
