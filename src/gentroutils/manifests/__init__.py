"""Module providing study manifest classes."""

import re
from functools import cached_property

import pandas as pd
from google.cloud import storage

from gentroutils.cloud import CloudPath


@pd.api.extensions.register_dataframe_accessor("sumstat")
class SummaryStatisticsManifestMethods:
    """Methods to invoke on the summary statistics manifest object."""

    def __init__(self, pandas_obj: pd.DataFrame):
        self.expected_cols = {"studyId", "rawSumstatPaths", "hasSyncedRawSumstatPaths"}
        self._validate(pandas_obj)
        self._df = pandas_obj

    def _validate(self, obj: pd.DataFrame):
        """Verify that correct fields are passed in the object."""
        if not self.expected_cols.issubset(set(obj.columns)):
            raise AttributeError("Passed object does not match the sumstat manifest dataframe")

    @property
    def synced(self) -> int:
        """Get the nuber of synced studies.

        Examples:
        >>> data = [('S1', ['some/path/S1'], True), ('S2', ['some/path/S2'], False)]
        >>> columns = ['studyId', 'rawSumstatPaths', 'hasSyncedRawSumstatPaths']
        >>> sm = SummaryStatisticsManifest(data=data, columns=columns)
        >>> sm
          studyId rawSumstatPaths  hasSyncedRawSumstatPaths
        0      S1  [some/path/S1]                      True
        1      S2  [some/path/S2]                     False

        >>> sm.sumstat.synced
        1
        """
        return len(self._df[self._df["hasSyncedRawSumstatPaths"]])

    @property
    def ambiguous(self) -> pd.DataFrame:
        """Get the ambiguous studies.

        Get the copy of the summary statistics slice with the studies that
        have ambigiuous number of raw summary statistics. (n studies != 1)

        Examples:
        >>> data = [('S1', ['some/path/S1', 'other/path/S1'], True), ('S2', ['some/path/S2'], False)]
        >>> columns = ['studyId', 'rawSumstatPaths', 'hasSyncedRawSumstatPaths']
        >>> sm = SummaryStatisticsManifest(data=data, columns=columns)
        >>> sm
          studyId                rawSumstatPaths  hasSyncedRawSumstatPaths
        0      S1  [some/path/S1, other/path/S1]                      True
        1      S2                 [some/path/S2]                     False

        >>> sm.sumstat.ambiguous
          studyId                rawSumstatPaths  hasSyncedRawSumstatPaths
        0      S1  [some/path/S1, other/path/S1]                      True
        """
        mask = self._df["rawSumstatPaths"].apply(lambda x: len(x))
        return self._df[mask != 1].copy()


class SummaryStatisticsManifest(pd.DataFrame):
    """Summary Statistics Manifest.

    Examples:
    >>> data = [('S1', ['some/path/S1', 'other/path/S1'], True), ('S2', ['some/path/S2'], False)]
    >>> columns = ['studyId', 'rawSumstatPaths', 'hasSyncedRawSumstatPaths']
    >>> sm = SummaryStatisticsManifest(data=data, columns=columns)
    >>> sm
      studyId                rawSumstatPaths  hasSyncedRawSumstatPaths
    0      S1  [some/path/S1, other/path/S1]                      True
    1      S2                 [some/path/S2]                     False

    ### Check the type of manifest

    >>> isinstance(sm, pd.DataFrame)
    True

    >>> isinstance(sm, SummaryStatisticsManifest)
    True
    """

    _required_columns = ["studyId", "rawSumstatPaths", "hasSyncedRawSumstatPaths"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        missing = set(self._required_columns) - set(self.columns)
        if missing:
            raise AttributeError(f"Missing following fields {missing}")

    @property
    def _constructor(self):
        return SummaryStatisticsManifest


class SummaryStatisticsManifestBuilder:
    """Custom class that holds the information about the currently synced raw summary statistics from the GWAS Catalog FTP.

    Examples:
        ### Initialize the manifest builder

        >>> sumstat_glob = "gs://gentroutils_test_gwas_catalog_inputs/test/**/*h.tsv.gz"
        >>> builder = SummaryStatisticsManifestBuilder(sumstat_glob, "h.tsv.gz")
        >>> builder.suffix
        'h.tsv.gz'

        >>> builder.bucket
        'gentroutils_test_gwas_catalog_inputs'

        >>> builder.prefix
        'test/'

        >>> builder.match_glob
        '**/*h.tsv.gz'

        ### Create the manifest from the summary statistics in existing bucket.

        List existing studies to see what should be in the manifest

        >>> blobs = builder.client.list_blobs(builder.bucket)
        >>> len(list(blobs))
        5

        >>> manifest = builder.create()

        >>> isinstance(manifest, SummaryStatisticsManifest)
        True

        >>> list(manifest.columns)
        ['studyId', 'rawSumstatPaths', 'hasSyncedRawSumstatPaths']

        >>> manifest.studyId
        0    GCST01
        1    GCST02
        2    GCST03
        3    GCST04
        4    GCST05
        Name: studyId, dtype: object
    """

    def __init__(self, sumstat_glob: str, suffix: str = "h.tsv.gz", *args) -> None:
        glob = CloudPath(sumstat_glob)
        self.suffix = suffix
        self.bucket = glob.bucket
        object_path = glob.object
        self.prefix = object_path.split("/")[0] + "/"
        self.match_glob = object_path[len(self.prefix) :]

    @cached_property
    def client(self) -> storage.Client:
        """Get the client for GCS connection."""
        return storage.Client()

    def create(self) -> SummaryStatisticsManifest:
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
        # List all summary statistics in the bucket.
        blobs = self.client.list_blobs(self.bucket, prefix=self.prefix, match_glob=self.match_glob)
        # Create a list to store the file paths
        data = [
            (extract_gwas_catalog_study_id_from_path(blob.name), blob.name)
            for blob in blobs
            if blob.name.endswith(self.suffix)
        ]
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
        return SummaryStatisticsManifest(df[["studyId", "rawSumstatPaths", "hasSyncedRawSumstatPaths"]])


def extract_gwas_catalog_study_id_from_path(path: str) -> str:
    """Extract GWAS Catalog study id from path.

    Args:
        path (str): path to extract study id from.

    Returns:
        str: study id.

    Raises:
        ValueError: when identifier is not found.

    Examples:
    >>> path = 'gs://some_bucket/GCST01-GCST10/GCST02/some_path'
    >>> extract_gwas_catalog_study_id_from_path(path)
    'GCST02'
    """
    raw_pattern = r"\/(GCST\d+)\/"
    pattern = re.compile(raw_pattern)
    result = pattern.search(path)
    if not result:
        raise ValueError("Gwas Catalog identifier was not found in %s", path)
    return result.group(1)


class PublishedStudiesManifestBuilder:
    def __init__(self, published_studies: str) -> None: ...
    def create(self) -> pd.DataFrame: ...


class SyncedStudiesManifestBuilder:
    def __init__(self, sumstat_glob: str) -> None: ...
    def create(self) -> pd.DataFrame: ...


class CuratedStudiesManifestBuilder:
    def __init__(self, previous_curation_file: str) -> None: ...
    def create(self) -> pd.DataFrame: ...
