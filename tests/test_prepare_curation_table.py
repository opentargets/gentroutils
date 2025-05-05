"""Test the prepare_curation_table module."""

from pathlib import Path

import pytest
from click.testing import CliRunner


def test_create_harmonised_table() -> None:
    """Test the create_harmonised_table function."""
    from gentroutils.manifests import SummaryStatisticsManifestBuilder

    sumstat_glob = "gs://gentroutils_test_gwas_catalog_inputs/test/**/*h.tsv.gz"

    expected_study_ids = [
        "GCST01",
        "GCST02",
        "GCST03",
        "GCST04",
        "GCST05",
    ]
    harmonisation_table = SummaryStatisticsManifestBuilder(sumstat_glob).create()
    assert harmonisation_table.shape[0] == 5
    assert all(study_id in harmonisation_table["studyId"].values for study_id in expected_study_ids)
    assert {"studyId", "rawSumstatPaths", "hasSyncedRawSumstatPaths"} == set(harmonisation_table.columns)


@pytest.mark.integration_test
def test_prepare_curation_table_command(tmp_path: Path) -> None:
    """Test the command to prepare the curation table."""
    from gentroutils import cli

    runner = CliRunner()
    catalog_study_file = str(tmp_path / "catalog_study_file.tsv")
    output_file = str(tmp_path / "curation_output.tsv")
    sumstat_glob = "gs://gentroutils_test_gwas_catalog_inputs/test/**/*.h.tsv.gz"
    result = runner.invoke(
        cli,
        [
            "prepare-curation-table",
            "--catalog-study-file",
            catalog_study_file,
            "--output-file",
            output_file,
            "--sumstat-glob",
            sumstat_glob,
        ],
    )

    assert result.exit_code == 0
