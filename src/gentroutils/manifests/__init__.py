"""Module providing study manifest classes."""

import re

import pandas as pd
from google.cloud import storage

from gentroutils.cloud import CloudPath


class SyncedRawSummaryStatisticsManifest:
    """Custom class that holds the information about the currently synced raw summary statistics from the GWAS Catalog FTP."""

    def __init__(self, sumstat_glob: str, suffix: str = "h.tsv.gz") -> None:
        self.sumstat_glob = CloudPath(sumstat_glob)
        self.suffix = suffix

    def create(self) -> pd.DataFrame:
        """Create harmonised table for GWAS Catalog curation process.

        Args:
            sumstat_glob (str): Path to the bucket where the summary statistics are stored.
            suffix (str): Suffix of the files to be included in the table.
                Default is "h.tsv.gz".

        Returns:
            pd.DataFrame: DataFrame with study id and file path.

        The function returns a dataframe with two columns (study and file_paths where
        the file_paths are lists of paths to the summary statistics files for each study.

        Make sure the glob pattern matches the expected harmonised file names.
        """
        # Create a CloudPath object from the sumstat_glob
        # Get the bucket, prefix and match glob.
        bucket = self.sumstat_glob.bucket
        object_path = self.sumstat_glob.object
        prefix = object_path.split("/")[0] + "/"
        match_glob = object_path[len(prefix) :]
        # List all files in the bucket
        client = storage.Client()
        blobs = client.list_blobs(bucket, prefix=prefix, match_glob=match_glob)
        # Create a list to store the file paths
        data = [(extract_study_id_from_path(blob.name), blob.name) for blob in blobs if blob.name.endswith(self.suffix)]
        # Create a DataFrame from the file paths
        df = pd.DataFrame(data, columns=["studyId", "rawSumstatPath"])
        # group duplicated studies
        df = (
            df.groupby("studyId")
            .agg({"rawSumstatPath": lambda x: list(x)})
            .reset_index()
            .rename(columns={"rawSumstatPath": "rawSumstatPaths"})
        )
        # Add the information about the sync performed on raw study
        df["hasSyncedRawSumstatPaths"] = df["rawSumstatPaths"].apply(lambda x: bool(len(x)))
        # Add the information about the number of raw sumstat files for the study
        df["rawSumstatPathsCount"] = df["rawSumstatPaths"].apply(lambda x: len(x))
        return df


def extract_study_id_from_path(path: str) -> str:
    """Extract study id from path.

    Args:
        path (str): path to extract study id from.

    Returns:
        str: study id.

    Raises:
        ValueError: when identifier is not found.
    """
    raw_pattern = r"\/(GCST\d+)(\.parquet)?\/"
    pattern = re.compile(raw_pattern)
    result = pattern.search(path)
    if not result:
        raise ValueError("Gwas Catalog identifier was not found in %s", path)
    return result.group(1)
