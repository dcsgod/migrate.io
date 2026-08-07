"""
execution/commit.py
Atomic Delta MERGE/swap from staging → production destination.
"""
from __future__ import annotations

from typing import Any

import structlog

logger = structlog.get_logger(__name__)


class CommitManager:
    """
    Commits staged Delta data to the production destination.

    Two commit strategies:
    - overwrite: atomic swap (staging → production path)
    - merge: Delta MERGE INTO with configurable merge keys

    On rejection: staging is discarded, production is untouched.
    Delta's transaction log provides automatic time-travel rollback.
    """

    def approve(
        self,
        spark: Any,
        run_id: str,
        staging_path: str,
        destination_path: str,
        mode: str = "overwrite",
        merge_keys: list[str] | None = None,
        options: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Commit staging → production.

        Returns:
            { "committed": True, "rows_written": int, "destination": str }
        """
        opts = options or {}
        logger.info("commit.approving", run_id=run_id, mode=mode, dest=destination_path)

        staging_df = spark.read.format("delta").load(staging_path)
        row_count = staging_df.count()

        if mode == "overwrite":
            (
                staging_df.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", opts.get("overwrite_schema", "false"))
                .save(destination_path)
            )

        elif mode == "append":
            staging_df.write.format("delta").mode("append").save(destination_path)

        elif mode == "merge":
            if not merge_keys:
                raise ValueError("merge_keys required for merge mode")
            from delta.tables import DeltaTable
            if not DeltaTable.isDeltaTable(spark, destination_path):
                # First run — create the table
                staging_df.write.format("delta").mode("overwrite").save(destination_path)
            else:
                delta_tbl = DeltaTable.forPath(spark, destination_path)
                match_condition = " AND ".join(
                    f"target.`{k}` = source.`{k}`" for k in merge_keys
                )
                # Build update map for all non-key columns
                all_cols = staging_df.columns
                update_map = {c: f"source.`{c}`" for c in all_cols}
                insert_map = {f"`{c}`": f"source.`{c}`" for c in all_cols}

                (
                    delta_tbl.alias("target")
                    .merge(staging_df.alias("source"), match_condition)
                    .whenMatchedUpdate(set=update_map)
                    .whenNotMatchedInsert(values=insert_map)
                    .execute()
                )

        else:
            raise ValueError(f"Unknown commit mode: {mode!r}")

        logger.info("commit.done", run_id=run_id, rows=row_count, dest=destination_path)
        return {
            "committed": True,
            "run_id": run_id,
            "rows_written": row_count,
            "destination": destination_path,
            "mode": mode,
        }

    def reject(self, spark: Any, run_id: str, staging_path: str) -> None:
        """Discard staging — production is untouched."""
        logger.info("commit.rejected", run_id=run_id)
        from execution.staging import StagingWriter
        writer = StagingWriter()
        writer.discard(spark, run_id)

    def rollback(self, spark: Any, destination_path: str, version: int) -> None:
        """
        Roll back the destination table to a previous Delta version.
        Uses Delta time travel.
        """
        logger.info("commit.rollback", dest=destination_path, version=version)
        df = spark.read.format("delta").option("versionAsOf", version).load(destination_path)
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(destination_path)
        logger.info("commit.rollback_done", dest=destination_path, restored_to_version=version)

    def get_history(self, spark: Any, destination_path: str) -> Any:
        """Return the Delta transaction history as a DataFrame."""
        from delta.tables import DeltaTable
        return DeltaTable.forPath(spark, destination_path).history()
