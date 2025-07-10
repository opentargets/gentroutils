"""Manifest for the curated summary statistics."""

from __future__ import annotations

import pandas as pd


class CuratedStudiesManifestBuilder:
    def __init__(self, published_studies_path: str) -> None:
        self.published_studies_path = published_studies_path

    @classmethod
    def from_tag(cls, tag: str) -> CuratedStudiesManifestBuilder:
        return cls(cls.get_curation_file(tag))

    @staticmethod
    def get_curation_file(
        tag: str,
        file: str = "genetics/GWAS_Catalog_study_curation.tsv",
        repo: str = "opentargets/curation",
    ) -> str:
        """Get the curation file from the tag."""
        return f"https://raw.githubusercontent.com/{repo}/refs/tags/{tag}/{file.removeprefix('/')}"

    def create(self) -> CuratedStudiesManifest:
        df = pd.read_csv(self.published_studies_path, sep="\t")
        return CuratedStudiesManifest(df)


class CuratedStudiesManifest(pd.DataFrame):
    """Manifest for curated studies.

    Examples:
    >>> d1 = ('S1', None, None, None, True, 17463246, 'Publication title 1', 'Trait 1')
    >>> d2 = ('S2', None, None, None, True, 17463247, 'Publication title 2', 'Trait 2')
    >>> columns = ['studyId', 'studyType', 'analysisFlag', 'qualityControl', 'isCurated', 'pubmedId', 'publicationTitle', 'traitFromSource']
    >>> pm = CuratedStudiesManifest(data=[d1, d2], columns=columns)
    >>> pm
      studyId studyType analysisFlag qualityControl  isCurated  pubmedId     publicationTitle traitFromSource
    0      S1      None         None           None       True  17463246  Publication title 1         Trait 1
    1      S2      None         None           None       True  17463247  Publication title 2         Trait 2

    ### Check the type of manifest

    >>> isinstance(pm, pd.DataFrame)
    True

    >>> isinstance(pm, CuratedStudiesManifest)
    True
    """

    _required_columns = [
        "studyId",
        "studyType",
        "analysisFlag",
        "qualityControl",
        "isCurated",
        "pubmedId",
        "publicationTitle",
        "traitFromSource",
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        missing = set(self._required_columns) - set(self.columns)
        if missing:
            raise AttributeError(f"Missing following fields {missing}")

    @property
    def _constructor(self):
        return CuratedStudiesManifest
