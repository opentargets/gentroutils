"""Tests for the cloud module."""

import pytest


@pytest.mark.parametrize(
    ("uri", "expected_bucket", "expected_object"),
    [
        pytest.param("gs://bucket_name/directory/object", "bucket_name", "directory/object", id="object path"),
        pytest.param("gs://another_bucket/directory/", "another_bucket", "directory", id="directory path"),
        pytest.param("gs://bucket_name/*.tsv.gz", "bucket_name", "*.tsv.gz", id="glob pattern"),
    ],
)
def test_cloud_path_init(uri: str, expected_bucket: str, expected_object: str) -> None:
    """Test the initialization of the CloudPath class."""
    from gentroutils.cloud import CloudPath

    # Test with a valid GCS path

    cloud_path = CloudPath(uri)
    assert cloud_path.uri == uri
    assert cloud_path.bucket == expected_bucket
    assert cloud_path.object == expected_object


def test_cloud_path_init_invalid() -> None:
    """Test the initialization of the CloudPath class with invalid paths."""
    from gentroutils.cloud import CloudPath
    from gentroutils.exceptions import GentroutilsError, GentroutilsErrorMessage

    # Test with an unsupported URL scheme
    with pytest.raises(GentroutilsError) as excinfo:
        CloudPath("s3://bucket_name/path/to/object")

    assert str(excinfo.value) == GentroutilsErrorMessage.UNSUPPORTED_URL_SCHEME.value.format(scheme="s3")


def test_cloud_path_init_missing_bucket() -> None:
    """Test the initialization of the CloudPath class with missing bucket."""
    from gentroutils.cloud import CloudPath
    from gentroutils.exceptions import GentroutilsError, GentroutilsErrorMessage

    url = "gs:///path/to/object"

    # Test with a missing bucket name
    with pytest.raises(GentroutilsError) as excinfo:
        CloudPath(url)

    assert str(excinfo.value) == GentroutilsErrorMessage.BUCKET_NAME_MISSING.value.format(url=url)


def test_cloud_path_init_missing_object() -> None:
    """Test the initialization of the CloudPath class with missing object."""
    from gentroutils.cloud import CloudPath
    from gentroutils.exceptions import GentroutilsError, GentroutilsErrorMessage

    url = "gs://bucket_name/"

    # Test with a missing object name
    with pytest.raises(GentroutilsError) as excinfo:
        CloudPath(url)

    assert str(excinfo.value) == GentroutilsErrorMessage.FILE_NAME_MISSING.value.format(url=url)
