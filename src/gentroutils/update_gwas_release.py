"""Update gwas catalog metadata."""

from __future__ import annotations

import asyncio
import sys

import aiohttp
import tqdm
from loguru import logger

from gentroutils.gcs import FTPtoGCPTranserableObject

MAX_CONCURRENT_CONNECTIONS = 10


async def update_gwas_release(files_to_transfer: list[tuple[str, str]], release_info_url: str) -> None:
    """Update GWAS Catalog metadata directly to cloud bucket."""
    # we always want to have the logs from this command uploaded to the target bucket
    async with aiohttp.ClientSession() as session:
        transferable_objects = [FTPtoGCPTranserableObject(x[0], x[1]) for x in files_to_transfer]
        transfer_tasks = [asyncio.create_task(x.sync_from_ftp_to_gcp()) for x in transferable_objects]
        # capture latest release metadata
        async with session.get(release_info_url) as response:
            if not response.ok:
                logger.error("Failed to fetch release info.")
                sys.exit(1)
            release_info = await response.json()
            for key, value in release_info.items():
                logger.debug(f"{key}: {value}")

            efo_version = release_info.get("efoversion")
            logger.info(f"Diseases were mapped to {efo_version} EFO release.")
            logger.info(f"EFO version: {efo_version}")
            ensembl_build = release_info.get("ensemblbuild")
            logger.info(f"Genes were mapped to v{ensembl_build} Ensembl release.")

        for f in tqdm.tqdm(asyncio.as_completed(transfer_tasks), total=len(transfer_tasks), desc="Downloading"):
            await f
        logger.info("gwas_curation_update step completed.")


__all__ = ["update_gwas_release"]
