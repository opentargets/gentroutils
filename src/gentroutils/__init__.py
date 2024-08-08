"""Cli for gentroutils."""

from __future__ import annotations

import time

import click

from gentroutils.cli.utils import set_log_file, set_log_lvl, teardown_cli

ascii_art = "GENTROUTILS"


@click.group()
@click.option("-d", "--dry-run", is_flag=True, default=False)
@click.option("-v", "--verbose", count=True, default=0, callback=set_log_lvl)
@click.option("-q", "--log-file", callback=set_log_file, required=False)
@click.pass_context
def cli(ctx: click.Context, **kwargs) -> None:
    r"""Gentroutils Command Line Interface."""
    click.echo(click.style(ascii_art, fg="blue"))
    ctx.max_content_width = 200
    ctx.ensure_object(dict)
    ctx.obj["dry_run"] = kwargs["dry_run"]
    ctx.obj["execution_start"] = time.time()
    ctx.call_on_close(lambda: teardown_cli(ctx))


__all__ = ["cli"]
