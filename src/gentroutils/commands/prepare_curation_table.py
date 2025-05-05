"""Prepare curation table for GWAS Catalog curation."""

from __future__ import annotations

import logging
import sys

import click
import pandas as pd
from click_option_group import RequiredMutuallyExclusiveOptionGroup, optgroup

from gentroutils.github import curation_latest_tag
from gentroutils.manifests import (
    CuratedStudiesManifestBuilder,
    PublishedStudiesManifestBuilder,
    SummaryStatisticsManifestBuilder,
    SyncedStudiesManifestBuilder,
)

logger = logging.getLogger("gentroutils")


@click.command(name="prepare-curation-table")
@click.option(
    "--catalog-study-file",
    type=click.STRING,
    help="Path to the catalog study file.",
    required=True,
)
@click.option(
    "--sumstat-glob",
    type=click.STRING,
    help="Path to the bucket where the summary statistics are stored.",
    required=True,
    default="gs://gwas_catalog_inputs/raw_summary_statistics/**h.tsv.gz",
)
@optgroup.group(
    "Previous curation",
    help="The reference for previous curation.",
    cls=RequiredMutuallyExclusiveOptionGroup,
)
@optgroup.option(
    "--previous-curation-tag",
    type=click.STRING,
    help="opentargets/curation repository tag like 25.03",
)
@optgroup.option(
    "--previous-curation-file",
    type=click.STRING,
    help="Previous curation tsv file.",
)
@click.option(
    "--output-file",
    type=click.STRING,
    help="Path to the curation file.",
    required=True,
)
@click.pass_context
def prepare_curation_table_command(ctx: click.Context, **kwargs) -> None:
    """Prepare curation table for GWAS Catalog curation process."""
    if ctx.obj.get("dry_run"):
        logger.info("Running in --dry-run mode, exitting.")
        sys.exit(0)
    logger.info(kwargs)
    logger.info("Reading published studies...")
    # published_studies = PublishedStudiesManifestBuilder(catalog_study_file).create()
    logger.info("Listing synchornised studies to the Open Targets google cloud storage bucket...")
    # synced_studies = SyncedStudiesManifestBuilder(sumstat_glob).create()
    logger.info("Reading curated studies from ")
    # curated_studies = CuratedStudiesManifestBuilder(previous_curation_file).create()
    # sumstat_manifest = SummaryStatisticsManifestBuilder(published_studies, synced_studies, curated_studies).create()
    # sumstat_manifest.to_csv(output_file, sep="\t", header=False)
