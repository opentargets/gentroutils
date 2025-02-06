"""Cli for gentroutils."""

from __future__ import annotations

import logging
import time

import click
import pyfiglet

from gentroutils.commands import (
    update_gwas_curation_metadata_command,
    validate_gwas_curation,
)
from gentroutils.commands.utils import set_log_file, set_log_lvl, teardown_cli

logger = logging.getLogger("gentroutils")
logger.setLevel(logging.DEBUG)


@click.group()
@click.option("-d", "--dry-run", is_flag=True, default=False)
@click.option(
    "-v",
    count=True,
    default=0,
    callback=set_log_lvl,
    help="Increase verbosity of the logging. Can be used multiple times. The default log level is ERROR, -v is INFO, -vv is DEBUG",
)
@click.option("-q", "--log-file", callback=set_log_file, required=False)
@click.pass_context
def cli(ctx: click.Context, **kwargs: dict[str, str]) -> None:
    r"""Gentroutils Command Line Interface."""
    ascii_art = pyfiglet.Figlet(font="serifcap").renderText("Gentroutils")
    click.echo(click.style(ascii_art, fg="blue"))
    ctx.max_content_width = 200
    ctx.ensure_object(dict)
    ctx.obj["dry_run"] = kwargs["dry_run"]
    ctx.obj["execution_start"] = time.time()
    ctx.call_on_close(lambda: teardown_cli(ctx))


cli.add_command(update_gwas_curation_metadata_command)
cli.add_command(validate_gwas_curation)

__all__ = ["cli"]
