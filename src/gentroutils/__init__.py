"""Cli for gentroutils."""

from __future__ import annotations

import sys
from typing import Annotated

import typer
from loguru import logger

from gentroutils.app_context import AppContext

cli = typer.Typer(
    help="Gentroutils Command Line Interface",
    no_args_is_help=True,
    rich_markup_mode="rich",
)

import asyncio
from functools import wraps


def coro(f):
    """Corutine wrapper for synchronous functions."""

    @wraps(f)
    def wrapper(*args, **kwargs):
        """Wrapper around the synchronous function."""
        return asyncio.run(f(*args, **kwargs))

    return wrapper


CURATED_INPUTS = [
    (
        "ftp://ftp.ebi.ac.uk/pub/databases/gwas/releases/latest/gwas-catalog-associations_ontology-annotated.tsv",
        "gs://gwas_catalog_inputs/gentroutils/20250708/gwas_catalog_associations_ontology_annotated.tsv",
    ),
    (
        "ftp://ftp.ebi.ac.uk/pub/databases/gwas/releases/latest/gwas-catalog-download-ancestries-v1.0.3.1.txt",
        "gs://gwas_catalog_inputs/gentroutils/20250708/gwas_catalog_download_ancestries.tsv",
    ),
    (
        "ftp://ftp.ebi.ac.uk/pub/databases/gwas/releases/latest/gwas-catalog-download-studies-v1.0.3.1.txt",
        "gs://gwas_catalog_inputs/gentroutils/20250708/gwas_catalog_download_studies.tsv",
    ),
]


@cli.callback()
def main(
    ctx: typer.Context,
    log_level: Annotated[int, typer.Option("-v", count=True, help="Use -v, -vv, -vvv for more verbosity")] = 0,
    dry_run: Annotated[bool, typer.Option("--dry-run", is_flag=True, help="Run in dry run mode")] = False,
    log_file: Annotated[
        str | None, typer.Option("--log-file", help="Path to the log file, defaults to stdout.")
    ] = None,
) -> None:
    """Gentroutils Command Line Interface."""
    import pyfiglet

    ascii_art = pyfiglet.Figlet(font="serifcap").renderText("Gentroutils")
    typer.echo(typer.style(ascii_art, fg=typer.colors.BRIGHT_GREEN))

    # Pass the application context to the CLI commands
    app_ctx = AppContext(dry_run=dry_run, log_level=log_level, log_file=log_file)
    app_ctx.set_logger()
    ctx.obj = app_ctx
    ctx.call_on_close(app_ctx.sink_logs_to_gcs)


@cli.command(name="update-gwas-release")
@coro
async def update_gwas_release_command(
    ctx: typer.Context,
    associations_file: Annotated[
        tuple[str, str],
        typer.Option("--associations-file", "-o", help="Ontology annotated associations file (ftp_file, gcp_file)"),
    ] = CURATED_INPUTS[0],
    ancestry_file: Annotated[
        tuple[str, str], typer.Option("--ancestry-file", "-a", help="Ancestry file (ftp_file, gcp_file)")
    ] = CURATED_INPUTS[1],
    study_file: Annotated[
        tuple[str, str], typer.Option("--study-file", "-s", help="Files to transfer (ftp_file, gcp_file)")
    ] = CURATED_INPUTS[2],
    release_info_url: Annotated[
        str, typer.Option("--release-info-url", "-g", help="GWAS Catalog release info URL")
    ] = "https://www.ebi.ac.uk/gwas/api/search/stats",
) -> None:
    """Update GWAS Catalog metadata directly to cloud bucket.

    \b
    This is the script to fetch the latest GWAS Catalog data files that include:
    - [x] gwas-catalog-associations_ontology-annotated.tsv - list of associations with ontology annotations by GWAS Catalog
    - [x] gwas-catalog-download-studies-v1.0.3.1.txt - list of published studies by GWAS Catalog
    - [x] gwas-catalog-download-ancestries-v1.0.3.1.txt - list of published ancestries by GWAS Catalog

    \b
    By default all GWAS Catalog data files are uploaded from GWAS Catalog FTP server to Open Targets GCP bucket.
    The script also captures the latest release metadata from GWAS Catalog release info url.
    One can overwrite this script to sync data files from FTP or HTTP(s) to GCP bucket. The example usage is as follows:

    \b
    gentroutils --log-file gs://gwas_catalog_data/gwas_catalog_inputs/gentroutils/20250708/log.txt -vvv update-gwas-release \\
    -o ftp://ftp.ebi.ac.uk/pub/databases/gwas/releases/latest/gwas-catalog-associations_ontology-annotated.tsv gs://gwas_catalog_inputs/gentroutils/20250708/gwas_catalog_associations_ontology_annotated.tsv \\
    -a ftp://ftp.ebi.ac.uk/pub/databases/gwas/releases/latest/gwas-catalog-download-studies-v1.0.3.1.txt gs://gwas_catalog_inputs/gentroutils/20250708/gwas_catalog_download_studies.tsv \\
    -s ftp://ftp.ebi.ac.uk/pub/databases/gwas/releases/latest/gwas-catalog-download-ancestries-v1.0.3.1.txt gs://gwas_catalog_inputs/gentroutils/20250708/gwas_catalog_download_ancestries.tsv \\
    -g https://www.ebi.ac.uk/gwas/api/search/stats

    To preserve the logs from this command, you can specify the log file path using `--log-file` option. The log file can point to local or GCP path.
    Currently only FTP and HTTP(s) protocols are supported for input and GCP protocol is supported for output.
    """  # noqa: D301
    logger.info("Updating GWAS release...")
    app_context: AppContext = ctx.obj
    if app_context.dry_run:
        logger.warning("Running in dry run mode. No changes will be made.")
        sys.exit(0)
    files_to_transfer = [associations_file, ancestry_file, study_file]
    from gentroutils.update_gwas_release import update_gwas_release

    await update_gwas_release(files_to_transfer, release_info_url)


@cli.command(name="prepare-gwas-curation")
def prepare_gwas_curation_command(
    ctx: typer.Context,
    study_list: Annotated[
        str, typer.Option("--study-file", "-s", help="Path to gwas catalog file study file.")
    ] = CURATED_INPUTS[2][1],
    raw_sumstat_bucket: Annotated[
        str, typer.Option("--raw-sumstat-bucket", "-b", help="Path to bucket with raw summary statistics.")
    ] = "gs://gwas_catalog_inputs/raw_summary_statistics",
    previous_curation_file: Annotated[
        str, typer.Option("--previous-curation-file", "-p", help="Path to previous curation file.")
    ] = "gs://gwas_catalog_inputs/curation/202507/GWAS_Catalog_study_curation.tsv",
    output_file: Annotated[
        str, typer.Option("--output-file", "-o", help="Path to output curation file.")
    ] = "curation.tsv",
) -> None:
    """Prepare GWAS curation table."""
    logger.info("Preparing GWAS curation table...")
    app_context: AppContext = ctx.obj
    if app_context.dry_run:
        logger.warning("Running in dry run mode. No changes will be made.")
        sys.exit(0)
    from gentroutils.prepare_gwas_curation import prepare_gwas_curation

    prepare_gwas_curation(
        study_list=study_list,
        raw_sumstat_bucket=raw_sumstat_bucket,
        previous_curation_file=previous_curation_file,
        output_file=output_file,
    )


@cli.command(name="validate-gwas-curation")
def validate_gwas_curation_command(ctx: typer.Context) -> None:
    """Validate GWAS curation table after manual curation."""
    logger.info("Validating GWAS curation...")
    app_context: AppContext = ctx.obj
    if app_context.dry_run:
        logger.warning("Running in dry run mode. No changes will be made.")
        sys.exit(0)


__all__ = ["cli"]
