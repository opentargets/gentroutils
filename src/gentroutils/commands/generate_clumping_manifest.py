"""Generate clumping manifest from the qc passed studies."""

import click


@click.command("generate-clumping-manifest")
@click.pass_context
def generate_clumping_manifest(ctx: click.Context) -> None:
    """Generate clumping manifest from the qc passed studies."""
    pass
