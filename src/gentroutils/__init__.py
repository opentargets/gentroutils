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
def update_gwas_release_command(ctx: typer.Context) -> None:
    """Update GWAS Catalog release in the bucket."""
    logger.info("Updating GWAS curation metadata...")
    app_context: AppContext = ctx.obj
    if app_context.dry_run:
        logger.warning("Running in dry run mode. No changes will be made.")
        sys.exit(0)


@cli.command(name="prepare-gwas-curation")
def prepare_gwas_curation_command(ctx: typer.Context) -> None:
    """Prepare GWAS curation table."""
    logger.info("Preparing GWAS curation table...")
    app_context: AppContext = ctx.obj
    if app_context.dry_run:
        logger.warning("Running in dry run mode. No changes will be made.")
        sys.exit(0)


@cli.command(name="validate-gwas-curation")
def validate_gwas_curation_command(ctx: typer.Context) -> None:
    """Validate GWAS curation table after manual curation."""
    logger.info("Validating GWAS curation...")
    app_context: AppContext = ctx.obj
    if app_context.dry_run:
        logger.warning("Running in dry run mode. No changes will be made.")
        sys.exit(0)


# cli.add_command(update_gwas_curation_metadata_command)
# cli.add_command(validate_gwas_curation_command)
# cli.add_command(prepare_curation_table_command)

__all__ = ["cli"]
