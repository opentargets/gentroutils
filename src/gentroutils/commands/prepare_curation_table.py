"""Prepare curation table for GWAS Catalog curation."""

from __future__ import annotations

import logging
import re

import click
import pandas as pd
from google.cloud import storage

from gentroutils.cloud import CloudPath
from gentroutils.manifests import SyncedRawSummaryStatisticsManifest

logger = logging.getLogger("gentroutils")


@click.command(name="prepare-curation-table")
@click.option(
    "--catalog-study-file",
    type=click.STRING,
    help="Path to the catalog study file.",
    required=True,
)
@click.option(
    "--output-file",
    type=click.STRING,
    help="Path to the curation file.",
    required=True,
)
@click.option(
    "--sumstat-glob",
    type=click.STRING,
    help="Path to the bucket where the summary statistics are stored.",
    required=True,
    default="gs://gwas_catalog_inputs/raw_summary_statistics/**h.tsv.gz",
)
def prepare_curation_table(catalog_study_file: str, output_file: str, sumstat_glob: str) -> None:
    """Prepare curation table for GWAS Catalog curation process."""
    study_table = pd.read_csv(catalog_study_file, sep="\t", header=0, dtype=str)
    harmonisation_table = SyncedRawSummaryStatisticsManifest(sumstat_glob).create()
