"""Update gwas catalog metadata."""

from __future__ import annotations

import asyncio
import io
import logging
import re
import sys
from urllib.parse import ParseResult, urlparse

import aioftp
import aiohttp
import click
from google.cloud import storage

from gentroutils.commands.utils import coro

logger = logging.getLogger("gentroutils")
MAX_CONCURRENT_CONNECTIONS = 10
CURATED_INPUTS = (
    (
        "ftp://ftp.ebi.ac.uk/pub/databases/gwas/releases/latest/gwas-catalog-associations_ontology-annotated.tsv",
        "gs://gwas_catalog_inputs/gwas_catalog_associations_ontology_annotated.tsv",
    ),
    (
        "ftp://ftp.ebi.ac.uk/pub/databases/gwas/releases/latest/gwas-catalog-download-studies-v1.0.3.1.txt",
        "gs://gwas_catalog_inputs/gwas_catalog_download_studies.tsv",
    ),
    (
        "ftp://ftp.ebi.ac.uk/pub/databases/gwas/releases/latest/gwas-catalog-download-ancestries-v1.0.3.1.txt",
        "gs://gwas_catalog_inputs/gwas_catalog_download_ancestries.tsv",
    ),
)


@click.command(name="update-gwas-curation-metadata")
@click.option(
    "--file-to-transfer",
    "-f",
    metavar="<ftp_file|http(s) file> <gcp_file>",
    type=(str, str),
    multiple=True,
    default=CURATED_INPUTS,
)
@click.option(
    "--gwas-catalog-release-info-url",
    "-g",
    metavar="<url>",
    default="https://www.ebi.ac.uk/gwas/api/search/stats",
    type=click.STRING,
)
@click.pass_context
@coro
async def update_gwas_curation_metadata_command(
    ctx: click.Context,
    file_to_transfer: list[tuple[str, str]],
    gwas_catalog_release_info_url: str,
) -> None:
    """Update GWAS Catalog metadata directly to cloud bucket.

    \b
    This is the script to fetch the latest GWAS Catalog data files that include:
    - [x] gwas-catalog-associations_ontology-annotated.tsv - list of associations with ontology annotations by GWAS Catalog
    - [x] gwas-catalog-download-studies-v1.0.3.1.txt - list of published studies by GWAS Catalog
    - [x] gwas-catalog-download-ancestries-v1.0.3.1.txt - list of published studies by GWAS Catalog

    \b
    By default all GWAS Catalog data files are uploaded from GWAS Catalog FTP server to Open Targets GCP bucket.
    The script also captures the latest release metadata from GWAS Catalog release info url.
    One can overwrite this script to sync data files from FTP or HTTP(s) to GCP bucket. The example usage is as follows:

    \b
    gentroutils --log-file gs://gwas_catalog_data/gwas_catalog_inputs/20250609/log.txt update-gwas-curation-metadata \\
    -f ftp://ftp.ebi.ac.uk/pub/databases/gwas/releases/latest/gwas-catalog-associations_ontology-annotated.tsv gs://gwas_catalog_inputs/gwas_catalog_associations_ontology_annotated.tsv \\
    -f ftp://ftp.ebi.ac.uk/pub/databases/gwas/releases/latest/gwas-catalog-download-studies-v1.0.3.1.txt gs://gwas_catalog_inputs/gwas_catalog_download_studies.tsv \\
    -f ftp://ftp.ebi.ac.uk/pub/databases/gwas/releases/latest/gwas-catalog-download-ancestries-v1.0.3.1.txt gs://gwas_catalog_inputs/gwas_catalog_download_ancestries.tsv \\
    -g https://www.ebi.ac.uk/gwas/api/search/stats


    To preserve the logs from this command, you can specify the log file path using `--log-file` option. The log file can point to local or GCP path.
    Currently only FTP and HTTP(s) protocols are supported for input and GCP protocol is supported for output.
    """  # noqa: D301
    # we always want to have the logs from this command uploaded to the target bucket
    logger.debug("Running gwas_curation_update step.")
    dry_run = ctx.obj["dry_run"]
    if len(file_to_transfer) > MAX_CONCURRENT_CONNECTIONS:
        logger.error(
            "File transfer limit exceeded! Max %s connections allowed.",
            MAX_CONCURRENT_CONNECTIONS,
        )
        sys.exit(1)
    async with aiohttp.ClientSession() as session:
        uri_map = [
            {"input": urlparse(ftp_file), "output": urlparse(gcp_file)} for ftp_file, gcp_file in file_to_transfer
        ]
        transfer_tasks = generate_transfer_tasks(session, uri_map, dry_run)

        # capture latest release metadata
        async with session.get(gwas_catalog_release_info_url) as response:
            if not response.ok:
                logger.error("Failed to fetch release info.")
                sys.exit(1)
            release_info = await response.json()
            for key, value in release_info.items():
                logger.debug("%s: %s", key, value)

            efo_version = release_info.get("efoversion")
            logger.info("Diseases were mapped to %s EFO release.", efo_version)
            logger.info("EFO version: %s", efo_version)
            ensembl_build = release_info.get("ensemblbuild")
            logger.info("Genes were mapped to v%s Ensembl release.", ensembl_build)

        results = await asyncio.gather(*transfer_tasks)
        if not dry_run:
            logger.info("Transferred %s files.", len(results))
        logger.info("gwas_curation_update step completed.")


def generate_transfer_tasks(
    session: aiohttp.ClientSession, uri_map: list[dict[str, ParseResult]], dry_run: bool
) -> list[asyncio.Task[None]]:
    """Generate transfer tasks.

    Args:
        session (aiohttp.ClientSession): Client Session.
        uri_map (list[dict[str, ParseResult]]): list of transferable tasks, each should have `input` and `output` keys.
        dry_run (bool): dry run flag.

    Returns:
        list[asyncio.Task[None]]: list of asyncio tasks.
    """
    ftp_transfer_list = []
    http_transfer_list = []
    for uri in uri_map:
        if uri["input"].scheme != "ftp" and not uri["input"].scheme.startswith("http"):
            logger.error("Only FTP and HTTP(s) protocols is supported at input.")
            sys.exit(1)
        if uri["output"].scheme != "gs":
            logger.error("Only GCP protocol is supported at output.")
            sys.exit(1)
        in_server = uri["input"].netloc
        out_server = uri["output"].netloc
        in_prefix = "/".join(uri["input"].path.strip("/").split("/")[:-1])
        in_file = uri["input"].path.strip("/").split("/")[-1]
        out_prefix = "/".join(uri["output"].path[1:-1].split("/")[:-1])
        out_bucket = uri["output"].path.split("/")[-1]
        if uri["input"].scheme == "ftp":
            ftp_transfer_list.append({
                "ftp_server": in_server,
                "ftp_prefix": in_prefix,
                "ftp_filename": in_file,
                "gcp_bucket": out_server,
                "gcp_prefix": out_prefix,
                "gcp_filename": out_bucket,
            })
        if uri["input"].scheme.startswith("http"):
            http_transfer_list.append({
                "http_url": uri["input"].geturl(),
                "gcp_bucket": out_server,
                "gcp_prefix": out_prefix,
                "gcp_filename": out_bucket,
            })
    transfer_tasks = [
        asyncio.create_task(
            sync_from_ftp_to_gcp(
                session,
                transfer_obj["ftp_server"],
                transfer_obj["ftp_prefix"],
                transfer_obj["ftp_filename"],
                transfer_obj["gcp_bucket"],
                transfer_obj["gcp_prefix"],
                transfer_obj["gcp_filename"],
                dry_run=dry_run,
            )
        )
        for transfer_obj in ftp_transfer_list
    ]

    transfer_tasks.extend(
        asyncio.create_task(
            sync_from_http_to_gcp(
                session,
                transfer_obj["http_url"],
                transfer_obj["gcp_bucket"],
                transfer_obj["gcp_prefix"],
                transfer_obj["gcp_filename"],
                dry_run=dry_run,
            )
        )
        for transfer_obj in http_transfer_list
    )

    return transfer_tasks


async def sync_from_http_to_gcp(
    session: aiohttp.ClientSession, url: str, gcp_bucket: str, gcp_prefix: str, gcp_file: str, *, dry_run: bool = True
) -> None:
    """Sync file from HTTP and upload to GCP.

    This function fetches the data from the provided HTTP URL and uploads the content
    directly to provided GCP bucket blob.

    Args:
        session (aiohttp.ClientSession): Client session.
        url (str): HTTP URL to fetch the data.
        gcp_bucket (str): GCP bucket name.
        gcp_prefix (str): GCP prefix.
        gcp_file (str): GCP file name.
        dry_run (bool, optional): Dry run flag. Defaults to True.
    """
    if dry_run:
        logger.info(
            "Attempting to transfer data from %s to gs://%s/%s/%s.",
            url,
            gcp_bucket,
            gcp_prefix,
            gcp_file,
        )
        return
    logger.info("Retriving data from: %s.", url)
    async with session.get(url) as response:
        if not response.ok:
            logger.error("Failed to fetch data from %s.", url)
            return

        content = response.content
        bucket = storage.Client().bucket(gcp_bucket)
        gcp_path = f"{gcp_prefix}/{gcp_file}" if gcp_prefix else gcp_file

        blob = bucket.blob(gcp_path)
        logger.info("Uploading the data to: gs://%s/%s.", gcp_bucket, gcp_path)
        text = await content.read()
        blob.upload_from_string(text)


async def sync_from_ftp_to_gcp(
    session: aiohttp.ClientSession,
    ftp_server: str,
    ftp_prefix: str,
    ftp_file: str,
    gcp_bucket: str,
    gcp_prefix: str,
    gcp_file: str,
    *,
    dry_run: bool = True,
) -> None:
    """Fetch files from FTP and upload to GCP.

    This function fetches the data from the provided FTP server and uploads the content directly
    to the provided GCP bucket blob.

    Args:
        session (aiohttp.ClientSession): Client session.
        ftp_server (str): FTP server.
        ftp_prefix (str): FTP prefix.
        ftp_file (str): FTP file name.
        gcp_bucket (str): GCP bucket name.
        gcp_prefix (str): GCP prefix.
        gcp_file (str): GCP file name.
        dry_run (bool, optional): Dry run flag. Defaults to True.

    """
    if dry_run:
        logger.info(
            "Attempting to transfer data from ftp://%s/%s/%s to gs://%s/%s/%s.",
            ftp_server,
            ftp_prefix,
            ftp_file,
            gcp_bucket,
            gcp_prefix,
            gcp_file,
        )
        return

    async with aioftp.Client.context(ftp_server, user="anonymous", password="anonymous") as ftp:  # noqa: S106
        bucket = storage.Client().bucket(gcp_bucket)
        gcp_path = f"{gcp_prefix}/{gcp_file}" if gcp_prefix else gcp_file
        blob = bucket.blob(gcp_path)
        logger.info("Changing directory to %s.", ftp_prefix)
        await ftp.change_directory(ftp_prefix)
        pwd = await ftp.get_current_directory()
        dir_match = re.match(r"^.*(?P<release_date>\d{4}\/\d{2}\/\d{2}){1}$", str(pwd))
        if dir_match:
            logger.info("Found release date!: %s", dir_match.group("release_date"))
        buffer = io.BytesIO()
        logger.info("Retrieving data from: ftp://%s/%s/%s.", ftp_server, ftp_prefix, ftp_file)
        stream = await ftp.download_stream(ftp_file)
        async with stream:
            async for block in stream.iter_by_block():
                buffer.write(block)
        buffer.seek(0)
        content = buffer.getvalue().decode("utf-8")
        buffer.close()
        logger.info("Uploading data to: gs://%s/%s.", gcp_bucket, gcp_path)
        blob.upload_from_string("".join(content))


__all__ = ["update_gwas_curation_metadata_command"]
