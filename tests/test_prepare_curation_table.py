"""Test the prepare_curation_table module."""

import pytest


@pytest.mark.usefixtures("google_cloud_storage", "gwas_catalog_inputs_bucket_with_data")
def test_create_harmonised_table() -> None:
    """Test the create_harmonised_table function."""
    from gentroutils.manifests import SyncedRawSummaryStatisticsManifest

    sumstat_glob = "gs://gwas_catalog_inputs/test/**/*h.tsv.gz"

    expected_study_ids = [
        "GCST01",
        "GCST02",
        "GCST03",
        "GCST04",
        "GCST05",
    ]
    harmonisation_table = SyncedRawSummaryStatisticsManifest(sumstat_glob).create()
    assert harmonisation_table.shape[0] == 5
    assert all(study_id in harmonisation_table["studyId"].values for study_id in expected_study_ids)
    assert {"studyId", "rawSumstatPaths", "hasSyncedRawSumstatPaths", "rawSumstatPathsCount"} == set(
        harmonisation_table.columns
    )
