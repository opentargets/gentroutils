"""Ütility functions for the CLI."""

import asyncio
import logging
import sys
import time
from functools import wraps
from pathlib import Path
from tempfile import NamedTemporaryFile
from urllib.parse import urlparse

import click
from google.cloud import storage

logger = logging.getLogger("gentroutils")


def set_log_file(ctx: click.Context, param: click.Option, log_file: str) -> str:
    """Set logging file based on provided `log-file` flag.

    This is a callback function called by the click.Option [--log-file] flag.
    In case of the `log_file` being path to the GCP bucket the returned value
    will be the local temporary file path. both log file paths (remote and local)
    will be stored in the click context object for further reference at the end of the CLI run.


    Args:
        ctx (click.Context): click context
        param (click.Option): click option
        log_file (str): log file path

    Raises:
        click.BadParameter: If the log file is a directory or the URI scheme is not GCS.

    Returns:
        str: log file path
    """
    ctx.ensure_object(dict)
    if not log_file:
        return ""
    logger.info("Extracting log file from the %s", param)
    upload_to_gcp = False

    if "://" in log_file:
        upload_to_gcp = True
    ctx.obj["upload_to_gcp"] = upload_to_gcp

    if upload_to_gcp:
        parsed_uri = urlparse(log_file)
        if parsed_uri.scheme != "gs":
            raise click.BadParameter("Only GCS is supported for logging upload")
        tmp_file = NamedTemporaryFile(delete=False)
        logger.info("Logging to temporary file %s", tmp_file.name)
        handler = logging.FileHandler(tmp_file.name)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        handler.setLevel(logging.DEBUG)
        logger.addHandler(handler)
        ctx.obj["local_log_file"] = tmp_file.name
        ctx.obj["local_log_file_obj"] = tmp_file
        ctx.obj["gcp_log_file"] = log_file
        return tmp_file.name

    else:
        local_file = Path(log_file)
        if local_file.exists() and local_file.is_dir():
            raise click.BadParameter("Log file is a directory")
        if local_file.exists() and local_file.is_file():
            local_file.unlink()
        if not local_file.exists():
            local_file.parent.mkdir(parents=True, exist_ok=True)
            local_file.touch()
        logger.info("Logging to %s", local_file)
        handler = logging.FileHandler(local_file)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        handler.setLevel(logging.DEBUG)
        logger.addHandler(handler)
        return str(local_file)


def teardown_cli(ctx: click.Context) -> None:
    """Teardown the gentroutils cli.

    This function is used to as a teardown function for the CLI.
    This will upload the log file to the GCP bucket if the `upload_to_gcp` flag is set in the context object.

    Args:
        ctx (click.Context): click context
    """
    if "upload_to_gcp" in ctx.obj and ctx.obj["upload_to_gcp"]:
        gcp_file = ctx.obj["gcp_log_file"]
        local_file = ctx.obj["local_log_file"]
        with open(local_file, "r") as f:
            content = f.read()
        try:
            client = storage.Client()
            bucket_name = urlparse(gcp_file).netloc
            bucket = client.bucket(bucket_name=bucket_name)
            file_name = urlparse(gcp_file).path.lstrip("/")
            blob = bucket.blob(file_name)
            logger.info("Uploading %s to %s", local_file, gcp_file)
            if ctx.obj["dry_run"]:
                logger.info("Dry run, skipping the upload of the log file")
            else:
                blob.upload_from_string(content)
                ctx.obj["local_log_file_obj"].close()
        except Exception as e:
            msg = f"Failed to upload log file to GCP {e}"
            logger.error(click.style(msg, fg="red"))
    logger.info("Finished, elapsed time %s seconds", time.time() - ctx.obj["execution_start"])


def set_log_lvl(_: click.Context, param: click.Option, value: int) -> int:
    """Set logging level based on the number of provided `v` flags.

    This is a callback function called by the click.Option [-v] flag.
    For example
    `-vv` - DEBUG
    `-v`  - INFO
    `no flag - ERROR

    Args:
        param (click.Option): click option
        value (int): logging level

    Returns:
        int: logging level
    """
    logger.info("Extracting log level from the %s", param)
    log_lvls = {0: logging.ERROR, 1: logging.INFO, 2: logging.DEBUG}
    log_lvl = log_lvls.get(value, logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    handler.setLevel(log_lvl)
    logger.addHandler(handler)
    return log_lvl


def coro(f):
    """Corutine wrapper for synchronous functions."""

    @wraps(f)
    def wrapper(*args, **kwargs):
        """Wrapper around the synchronous function."""
        return asyncio.run(f(*args, **kwargs))

    return wrapper


__all__ = ["set_log_file", "set_log_lvl", "coro", "logger", "teardown_cli"]
