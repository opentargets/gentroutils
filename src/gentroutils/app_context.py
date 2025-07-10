"""CLI application context."""

import os
import sys
import time
from functools import cached_property
from tempfile import mkstemp
from typing import TextIO

from loguru import logger

from gentroutils.gcs import CloudPath, TranserableObject


class AppContext:
    """Application context for the CLI."""

    VERBOSITY_MAP = {0: "INFO", 1: "SUCCESS", 2: "DEBUG"}

    def __init__(self, dry_run: bool, log_file: str | None, log_level: int):
        logger.info("Initializing application context...")
        self.start = time.perf_counter()
        self.dry_run = dry_run
        self.log_level = self.VERBOSITY_MAP.get(log_level, "DEBUG")
        self.log_file = log_file
        logger.info(f"Log level set to {self.log_level}.")

    def set_logger(self) -> None:
        """Set the logger with the specified sink and log level.

        If there is a log_file, set the sink to that file or a temporary file before uploading it to GCS.
        """
        logger.remove()
        sink = self.sink.local if isinstance(self.sink, TranserableObject) else self.sink
        logger.add(sink=sink, level=self.log_level)
        if sink != sys.stdout:
            logger.add(sink=sys.stdout, level=self.log_level)
        logger.info(f"Logger initialized with sink {sink}")

    @cached_property
    def sink(self) -> str | TextIO | TranserableObject:
        """Get the local log file path."""
        match self.log_file:
            case str():
                if self.log_file.startswith("gs://"):
                    fd, tmp_log_file = mkstemp(suffix=".log", prefix="gentroutils_")
                    os.close(fd)
                    logger.debug(f"Logging to temporary file: {tmp_log_file}")
                    return TranserableObject(local=tmp_log_file, remote=CloudPath(self.log_file).uri)
                else:
                    logger.debug(f"Logging to local file: {self.log_file}")
                    return self.log_file
            case _:
                logger.debug("No log file specified, logging to stdout.")
                return sys.stdout

    def sink_logs_to_gcs(self) -> None:
        """Sink the logs to the GCS blob if transferable object is configured."""
        self.end = time.perf_counter()
        logger.success(f"Application finished in {self.end - self.start:.2f} seconds")
        if not isinstance(self.sink, TranserableObject):
            logger.debug("No remote log file configured, skipping upload.")
            return
        if self.dry_run:
            logger.debug("Running in dry run mode. Logs will not be uploaded to GCS.")
            return  # noop
        remote_log_file = self.sink.remote
        client = self.sink.client
        bucket = client.bucket(bucket_name=remote_log_file.bucket)
        blob = bucket.blob(blob_name=remote_log_file.object)
        try:
            logger.debug(f"Uploading logs to GCS from: {self.sink.local}, to: {self.sink.remote}")
            blob.upload_from_filename(self.sink.local)

        except Exception as e:
            logger.error(f"Failed to upload logs to GCS: {e}")
            return
        finally:
            logger.success(f"Logs successfully uploaded to {self.sink.remote}")
