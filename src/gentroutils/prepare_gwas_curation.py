"""Prepare GWAS curation table for manual curation."""

from __future__ import annotations

import re
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING

import polars as pl
from google.cloud import storage
from loguru import logger

from gentroutils.gcs import CloudPath

if TYPE_CHECKING:
    from google.api_core.page_iterator import HTTPIterator


def prepare_gwas_curation(study_list_file: str, raw_sumstat_bucket: str, previous_curation_file: str, output_file: str):
    """Prepare GWAS curation table."""
    synced = SyncedStudiesManifest(raw_sumstat_bucket).construct()
    logger.info(f"Synced studies manifest constructed with {synced.shape[0]} entries.")
    logger.info(SyncedStudiesManifest.synced_statistics(synced))

    curated = CuratedStudiesManifest(previous_curation_file).construct()

    published = PublisedStudiesManifest(study_list_file).construct()

    # Merging strategy:
    # 1. Start with previously curated studies.
    # 2. Outer join published studies to add new entries.
    # 3. Outer join synced studies to see which studies were removed


class PublisedStudiesManifest:
    """Class to manage published studies manifest."""

    def __init__(self, study_list_file: str) -> None:
        """Initialize the PublisedStudiesManifest."""
        self.study_list_file = study_list_file

    def construct(self) -> pl.DataFrame:
        """Construct the DataFrame of published studies."""
        return pl.read_csv(self.study_list_file, separator="\t", has_header=True)


class CuratedStudiesManifest:
    """Class to manage curated studies manifest."""

    def __init__(self, prepare_curation_file: str) -> None:
        """Initialize the CuratedStudiesManifest."""
        self.prepare_curation_file = prepare_curation_file

    def construct(self) -> pl.DataFrame:
        """Construct the DataFrame of curated studies."""
        return pl.read_csv(self.prepare_curation_file, separator="\t", has_header=True)


class SyncedStudiesManifest:
    """Class to manage synced studies manifest."""

    def __init__(self, raw_sumstat_bucket: str) -> None:
        """Initialize the SyncedStudiesManifest."""
        self.cloud_path = CloudPath(raw_sumstat_bucket)

    def list_sumstats(self) -> HTTPIterator:
        client = storage.Client()
        bucket = client.bucket(self.cloud_path.bucket)
        return bucket.list_blobs(match_glob="**h.tsv.gz", prefix=self.cloud_path.object)

    def construct(self) -> pl.DataFrame:
        """Construct the DataFrame of synced studies."""
        iterator = self.list_sumstats()
        with ThreadPoolExecutor(max_workers=4) as executor:
            results = list(executor.map(lambda x: (extract_study_id(x.name), x.name), iterator))

        df = pl.DataFrame(results, schema=[("studyId", pl.Utf8), ("rawSumstatPath", pl.Utf8)], orient="row")
        return (
            df.group_by("studyId")
            .agg(pl.col("rawSumstatPath").alias("rawSumstatPaths"), pl.count("rawSumstatPath").alias("nSumstat"))
            .with_columns(
                pl.when(pl.col("nSumstat") != 1)
                .then(pl.lit(True))
                .otherwise(pl.lit(False))
                .alias("hasMultipleSumstatFiles")
            )
        )

    @staticmethod
    def synced_statistics(synced_df: pl.DataFrame) -> pl.DataFrame:
        return synced_df.group_by("hasMultipleSumstatFiles").len().sort("len")


def extract_study_id(blob_name: str) -> str:
    """Extract study ID from the blob name."""
    # Assuming the blob name is structured as 'study_id/filename'
    pattern = re.compile(r"\/(GCST\d+)\/")
    match = pattern.search(blob_name)
    return match.group(1) if match else ""
