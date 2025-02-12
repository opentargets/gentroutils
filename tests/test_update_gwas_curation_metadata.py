"""Test cases for the update_gwas_curation_metadata command."""

import logging
from ftplib import FTP
from pathlib import Path

import pytest
from click.testing import CliRunner
from google.cloud import storage

import gentroutils
from gentroutils import cli


def gwas_catalog_ftp_heartbeet() -> bool:
    """Check if GWAS Catalog FTP server is up and running."""
    try:
        ftp_server = "ftp.ebi.ac.uk"
        with FTP() as ftp:
            ftp.connect(ftp_server)
            ftp.login()
            ftp.voidcmd("NOOP")
            return True
    except Exception:
        return False


@pytest.mark.integration_test
def test_run_update_gwas_curation_metadata_help():
    """Test command with --help flag."""
    runner = CliRunner()
    result = runner.invoke(cli, ["update-gwas-curation-metadata", "--help"])
    assert result.exit_code == 0


@pytest.mark.integration_test
def test_run_update_gwas_curation_metadata_exceed_connection(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Test command with --dry-run flag when connection limit is exceeded."""
    monkeypatch.setattr(
        gentroutils.commands.update_gwas_curation_metadata,
        "MAX_CONCURRENT_CONNECTIONS",
        0,
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["--dry-run", "update-gwas-curation-metadata"])
    for record in caplog.records:
        if record.levelname == "ERROR":
            assert "File transfer limit exceeded! Max 0 connections allowed" in caplog.text
    assert result.exit_code == 1


@pytest.mark.usefixtures("google_cloud_storage", "staging_bucket", "ebi_mock_server")
@pytest.mark.integration_test
def test_run_update_gwas_curation_metadata_no_dry_run(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test command save from gwas catalog ftp to local gcs mock server."""
    caplog.set_level(logging.DEBUG)
    runner = CliRunner()
    _in = "ftp://ftp.ebi.ac.uk/pub/databases/gwas/summary_statistics/harmonised_list.txt"
    _out = "gs://staging/harmonised_list.txt"
    result = runner.invoke(cli, ["update-gwas-curation-metadata", "-f", _in, _out])
    assert result.exit_code == 0
    client = storage.Client()
    blobs = client.bucket("staging").list_blobs()
    assert next(blobs).name == "harmonised_list.txt"
    for record in caplog.records:
        if record.levelname == "INFO" and "Retrieving data" in record.message:
            assert f"Retrieving data from: {_in}." in record.message
        if record.levelname == "INFO" and "Uploading data" in record.message:
            assert f"Uploading data to: {_out}." in record.message
        if record.levelname == "INFO" and "Diseases" in record.message:
            assert "Diseases were mapped to v" in record.message
        if record.levelname == "INFO" and "EFO version" in record.message:
            assert "EFO version: v" in record.message


@pytest.mark.usefixtures("google_cloud_storage", "staging_bucket", "ebi_mock_server")
@pytest.mark.integration_test
def test_run_update_gwas_curation_metadata_fail_to_fetch_release_info(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test command to see if the release info is correctly fetched."""
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "update-gwas-curation-metadata",
            "-f",
            "ftp://ftp.ebi.ac.uk/pub/databases/gwas/summary_statistics/harmonised_list.txt",
            "gs://staging/harmonised_list.txt",
            "-ghttp://localhost:4443",  # incorrect link to the gwas catalog api stats
        ],
    )
    assert result.exit_code == 1
    for record in caplog.records:
        if record.levelname == "ERROR":
            assert "Failed to fetch release info" in record.message


@pytest.mark.usefixtures("google_cloud_storage", "staging_bucket", "ebi_mock_server")
@pytest.mark.parametrize(
    "link",
    [
        pytest.param(
            "ftp://ftp.ebi.ac.uk/pub/databases/gwas/summary_statistics/harmonised_list.txt",
            id="ftp",
        ),
        pytest.param(
            "https://raw.githubusercontent.com/opentargets/curation/master/genetics/GWAS_Catalog_study_curation.tsv",
            id="http",
        ),
    ],
)
@pytest.mark.integration_test
def test_run_update_gwas_curation_metadata_with_dry_run_does_not_produce_blob(
    link: str,
) -> None:
    """Test command to see if the release info is correctly fetched."""
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--dry-run",
            "update-gwas-curation-metadata",
            "-f",
            link,
            "gs://staging/harmonised_list.txt",
        ],
    )
    assert result.exit_code == 0
    client = storage.Client()
    blobs = client.bucket("staging").list_blobs()
    assert not list(blobs)


@pytest.mark.usefixtures("google_cloud_storage", "staging_bucket", "ebi_mock_server")
@pytest.mark.integration_test
def test_run_update_gwas_curation_metadata_transfer_from_http_to_gcp():
    """Test command to see if the release info is correctly fetched."""
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "update-gwas-curation-metadata",
            "-f",
            "https://raw.githubusercontent.com/opentargets/curation/25.01/genetics/GWAS_Catalog_study_curation.tsv",
            "gs://staging/curation-metadata.tsv",
        ],
    )
    assert result.exit_code == 0
    client = storage.Client()
    blobs = client.bucket("staging").list_blobs()
    assert next(blobs).name == "curation-metadata.tsv"


@pytest.mark.usefixtures("google_cloud_storage", "staging_bucket", "ebi_mock_server")
@pytest.mark.integration_test
def test_run_update_gwas_curation_metadata_preserve_logs_locally(
    tmp_path: Path,
) -> None:
    """Test command dumps logs to a local file."""
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--log-file",
            str(tmp_path / "gentroutils.log"),
            "update-gwas-curation-metadata",
            "-f",
            "ftp://ftp.ebi.ac.uk/pub/databases/gwas/summary_statistics/harmonised_list.txt",
            "gs://staging/harmonised_list.txt",
        ],
    )
    assert result.exit_code == 0
    client = storage.Client()
    blobs = client.bucket("staging").list_blobs()
    assert next(blobs).name == "harmonised_list.txt"
    assert (tmp_path / "gentroutils.log").exists()
    Path(tmp_path / "gentroutils.log").unlink()


@pytest.mark.usefixtures("google_cloud_storage", "staging_bucket", "ebi_mock_server")
@pytest.mark.integration_test
def test_run_update_gwas_curation_metadata_preserve_logs_in_gcs():
    """Test command dumps logs to a gcs file."""
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--log-file",
            "gs://staging/gentroutils.log",
            "update-gwas-curation-metadata",
            "-f",
            "ftp://ftp.ebi.ac.uk/pub/databases/gwas/summary_statistics/harmonised_list.txt",
            "gs://staging/harmonised_list.txt",
        ],
    )
    assert result.exit_code == 0
    client = storage.Client()
    blobs = client.bucket("staging").list_blobs()
    for blob in blobs:
        assert blob.name == "harmonised_list.txt" or blob.name == "gentroutils.log"
