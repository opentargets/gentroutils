"""Tests for curation."""

from pathlib import Path

import polars as pl
import pytest

from gentroutils.errors import GentroutilsError
from gentroutils.parsers.curation import CurationSchema, DownloadStudiesSchema, GWASCatalogCuration


@pytest.fixture
def curation_data() -> pl.DataFrame:
    """Fixture defining previous curation file data."""
    return pl.DataFrame(
        [
            ("GCST000001", None, None, None, True, "12345678", "Publication Title A", "Trait A"),
            ("GCST000002", None, None, None, True, "23456789", "Publication Title B", "Trait B"),
            ("GCST000003", None, None, None, True, "34567890", "Publication Title C", "Trait C"),
            ("GCST000004", None, None, None, True, "45678901", "Publication Title D", "Trait D"),
        ],
        schema=CurationSchema.columns(),
        orient="row",
    )


@pytest.fixture
def prev_curation_file(tmp_path: Path, curation_data: pl.DataFrame) -> str:
    """Fixture for previous curation file."""
    file_path = tmp_path / "previous_curation.tsv"
    curation_data.write_csv(file_path, separator="\t")
    return file_path.as_posix()


def test_curation_schema(curation_data: pl.DataFrame) -> None:
    """Test the CurationSchema columns."""
    assert curation_data.columns == CurationSchema.columns(), "CurationSchema columns do not match expected values."
    assert curation_data.shape == (4, len(CurationSchema.columns())), "Curation data shape is incorrect."


@pytest.fixture
def studies_data() -> pl.DataFrame:
    """Fixture defining studies data."""
    return pl.DataFrame(
        [
            ("GCST000001", "Trait A", "12345678", "Publication Title A"),
            ("GCST000002", "Trait B", "23456789", "Publication Title B"),
            ("GCST000003", "Trait C", "34567890", "Publication Title C"),
            ("GCST000005", "Trait E", "56789012", "Publication Title E"),  # New study not in the previous curation
            # ("GCST000004", "Trait D", "45678901", "Publication Title D"),  # Excluded study
        ],
        schema=DownloadStudiesSchema.columns(),
        orient="row",
    )


@pytest.fixture
def downloaded_studies_file(tmp_path: Path, studies_data: pl.DataFrame) -> str:
    """Fixture for downloaded studies file."""
    file_path = tmp_path / "downloaded_studies.tsv"
    reverse_mapping = {v: k for k, v in DownloadStudiesSchema.mapping().items()}
    studies_data.rename(mapping=reverse_mapping).write_csv(file_path, separator="\t")
    return file_path.as_posix()


def test_download_studies_schema(studies_data: pl.DataFrame) -> None:
    """Test the DownloadStudiesSchema columns."""
    assert studies_data.columns == DownloadStudiesSchema.columns(), (
        "DownloadStudiesSchema columns do not match expected values."
    )
    assert studies_data.shape == (4, len(DownloadStudiesSchema.columns())), "Download studies data shape is incorrect."


class TestGwasCatalogCuration:
    """Tests for GWAS Catalog Curation."""

    def test_curation_initialization(self, curation_data: pl.DataFrame, studies_data: pl.DataFrame) -> None:
        """Test initialization of GWASCatalogCuration."""
        curation = GWASCatalogCuration(
            previous_curation=curation_data,
            studies=studies_data,
        )
        assert isinstance(curation.previous_curation, pl.DataFrame), "Previous curation should be a DataFrame."
        assert isinstance(curation.studies, pl.DataFrame), "Studies should be a DataFrame."

    def test_result(self, curation_data: pl.DataFrame, studies_data: pl.DataFrame) -> None:
        """Test run method."""
        result = GWASCatalogCuration(previous_curation=curation_data, studies=studies_data).result
        expected_result_columns = CurationSchema.extended_columns()
        assert result.columns == expected_result_columns, (
            "Result DataFrame columns do not match expected extended columns."
        )
        assert result.shape == (5, len(expected_result_columns)), "Result DataFrame shape is incorrect."
        assert result.filter(pl.col("status") == "removed").shape[0] == 1, "There should be one removed study."
        assert result.filter(pl.col("status") == "new").shape[0] == 1, "There should be one new study."
        assert result.filter(pl.col("status") == "curated").shape[0] == 3, "There should be three curated studies."
        assert (
            result.filter(pl.col("studyId") == "GCST000005")
            .filter(pl.col("status") == "new")
            .filter(~pl.col("isCurated"))
            .shape[0]
            == 1
        ), "New study GCST000005 should be present in the result with `new` status and `isCurated` flag set to False."

        assert result.filter(pl.col("studyId") == "GCST000004").filter(pl.col("status") == "removed").shape[0] == 1, (
            "Removed study GCST000004 should be present in the result with `removed` status."
        )
        assert result.filter(pl.col("isCurated")).shape[0] == 4, (
            "There should be three studies with `isCurated` flag set to True."
        )

    def test_constructor_from_prev_curation(self, prev_curation_file: str, downloaded_studies_file: str) -> None:
        """Test constructor from previous curation and studies."""
        curation = GWASCatalogCuration.from_prev_curation(prev_curation_file, downloaded_studies_file)
        assert isinstance(curation, GWASCatalogCuration), "Should return an instance of GWASCatalogCuration."
        assert curation.previous_curation.shape[0] == 4, "Previous curation should have 4 rows."
        assert curation.studies.shape[0] == 4, "Studies should have 4 rows."

    def test_empty_previous_curation(self, tmp_path: Path, downloaded_studies_file: str) -> None:
        """Test handling of empty previous curation."""
        empty_curation_file = tmp_path / "empty_previous_curation.tsv"
        empty_curation_file.write_text("\t".join(CurationSchema.columns()) + "\n")
        with pytest.raises(GentroutilsError, match="Previous curation data is empty"):
            GWASCatalogCuration.from_prev_curation(empty_curation_file.as_posix(), downloaded_studies_file)

    def test_empty_studies(self, prev_curation_file: str, tmp_path: Path) -> None:
        """Test handling of empty studies."""
        empty_studies_file = tmp_path / "empty_studies.tsv"
        columns = DownloadStudiesSchema.mapping().keys()
        empty_studies_file.write_text("\t".join(columns) + "\n")
        with pytest.raises(GentroutilsError, match="List of downloaded studies from GWAS Catalog release is empty"):
            GWASCatalogCuration.from_prev_curation(prev_curation_file, empty_studies_file.as_posix())


class TestDownloadedStudiesSchema:
    """Tests for DownloadedStudiesSchema."""

    def test_columns_method(self, studies_data: pl.DataFrame) -> None:
        """Test the DownloadedStudiesSchema columns."""
        assert isinstance(DownloadStudiesSchema.columns(), list), "DownloadStudiesSchema.columns should be a list."
        assert all(isinstance(col, str) for col in DownloadStudiesSchema.columns()), (
            "All DownloadStudiesSchema columns should be strings."
        )
        assert all(col in studies_data.columns for col in DownloadStudiesSchema.columns()), (
            "All DownloadStudiesSchema columns should be present in the studies DataFrame."
        )

    def test_mapping_method(self) -> None:
        """Test the DownloadedStudiesSchema mapping."""
        mapping = DownloadStudiesSchema.mapping()
        assert isinstance(mapping, dict), "DownloadStudiesSchema.mapping should return a dictionary."
        assert all(isinstance(k, str) and isinstance(v, str) for k, v in mapping.items()), (
            "All keys and values in DownloadStudiesSchema.mapping should be strings."
        )
        assert len(mapping) == len(DownloadStudiesSchema.columns()), (
            "Mapping length should match the number of columns in DownloadStudiesSchema."
        )
