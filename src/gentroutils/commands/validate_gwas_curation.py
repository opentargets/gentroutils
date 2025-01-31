"""Validate gwas catalog manual curation file."""

from __future__ import annotations
import click
import great_expectations as gx


@click.command(name="validate-gwas-curation")
@click.argument("input_file", type=click.Path(exists=True, dir_okay=False))
def validate_gwas_curation(input_file: str) -> None:
    """Validate GWAS catalog manual curation file."""

    context = gx.get_context(mode="ephemeral")
    print(context)
