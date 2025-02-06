"""Test cases for validate_gwas_curation command."""

import pytest
from click.testing import CliRunner

from gentroutils import cli


@pytest.mark.integration_test
def test_run_validate_curation_help():
    """Test command with --help flag."""
    runner = CliRunner()
    result = runner.invoke(cli, ["validate-gwas-curation", "--help"])
    assert result.exit_code == 0


@pytest.mark.parametrize(
    [
        "curation_file",
        "exit_code",
    ],
    [
        pytest.param("tests/data/manual_curation/correct_curation.tsv", 0, id="Correct curation file"),
        pytest.param("tests/data/manual_curation/missing_curation.tsv", 2, id="Local file does not exist"),
        pytest.param("tests/data/manual_curation/", 2, id="Local directory"),
        pytest.param("tests/data/manual_curation/incorrect_columns_curation.tsv", 1, id="Incorrect column names"),
        pytest.param("tests/data/manual_curation/incorrect_pubmedId_type.tsv", 1, id="Incorrect type in pubmedId"),
        pytest.param(
            "tests/data/manual_curation/incorrect_publicationTitle_type.tsv", 1, id="Incorrect type in publicationTitle"
        ),
        pytest.param("tests/data/manual_curation/incorrect_traitFromSource_type.tsv", 1, id="Incorrect type in traitFromSource"),
        pytest.param("tests/data/manual_curation/incorrect_studyId_type.tsv", 1, id="Incorrect type in studyId"),
        pytest.param("tests/data/manual_curation/incorrect_studyType_type.tsv", 1, id="Incorrect type in studyType"),
        pytest.param("tests/data/manual_curation/incorrect_analysisFlag_type.tsv", 1, id="Incorrect type in analysisFlag"),
        pytest.param("tests/data/manual_curation/incorrect_studyType_value.tsv", 1, id="Incorrect value in studyType"),
        pytest.param("tests/data/manual_curation/incorrect_analysisFlag_value.tsv", 1, id="Incorrect value in analysisFlag"),
        pytest.param("tests/data/manual_curation/incorrect_studyId_value.tsv", 1, id="Incorrect value in studyId"),
        pytest.param("tests/data/manual_curation/null_value_in_studyId.tsv", 1, id="Null value in studyId"),
        pytest.param("tests/data/manual_curation/non_unique_studyId.tsv", 1, id="Non unique studyId"),
        pytest.param("gs://gwas_catalog/correct_curation.tsv", 2, id="Non local path"),
    ],
)
@pytest.mark.intergration_test
def test_run_validate_curation(
    curation_file: str,
    exit_code: int,
) -> None:
    """Test curation command."""
    runner = CliRunner()
    result = runner.invoke(cli, ["validate-gwas-curation", curation_file])
    assert result.exit_code == exit_code


@pytest.mark.intergration_test
def test_run_validate_curation_dry_run():
    """Test curation command with --dry-run flag"""
    curation_file = "tests/data/manual_curation/correct_curation.tsv"
    runner = CliRunner()
    result = runner.invoke(cli, ["--dry-run", "validate-gwas-curation", curation_file])
    assert result.exit_code == 0
