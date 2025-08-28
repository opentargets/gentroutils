"""Test FTP to GCS transfer."""

from contextlib import contextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from gentroutils.errors import GentroutilsError
from gentroutils.io.transfer import FTPtoGCPTransferableObject


@contextmanager
def does_not_raise():
    yield


class TestFTPtoGCPTransferableObject:
    def test_validation_success(self):
        with does_not_raise():
            obj = FTPtoGCPTransferableObject(source="ftp://example.com/file.txt", destination="gs://bucket/file.txt")
            assert obj.source == "ftp://example.com/file.txt"
            assert obj.destination == "gs://bucket/file.txt"

    @pytest.mark.parametrize(
        ("source", "destination", "expected_error"),
        [
            pytest.param("invalid_ftp_path", "gs://bucket/file.txt", "Unsupported URL scheme", id="invalid_source"),
            pytest.param(
                "ftp://example.com/file.txt", "invalid_gcs_path", "Unsupported URL scheme", id="invalid_destination"
            ),
        ],
    )
    def test_validation_failure(self, source, destination, expected_error):
        with pytest.raises(GentroutilsError, match=expected_error):
            FTPtoGCPTransferableObject(source=source, destination=destination)

    @pytest.mark.asyncio
    @patch("gentroutils.io.transfer.ftp_to_gcs.storage.Client")
    @patch("gentroutils.io.transfer.ftp_to_gcs.aioftp.Client.context")
    async def test_transfer(self, mock_ftp_context, mock_storage_client):
        # Mock FTP client and its operations
        mock_ftp_client = AsyncMock()
        mock_ftp_context.return_value.__aenter__.return_value = mock_ftp_client
        mock_ftp_context.return_value.__aexit__.return_value = None

        # Mock FTP operations
        mock_ftp_client.change_directory = AsyncMock()
        mock_ftp_client.get_current_directory = AsyncMock(return_value="/some/path/2023/12/25")

        # Mock download stream
        mock_stream = AsyncMock()
        mock_stream.__aenter__.return_value = mock_stream
        mock_stream.__aexit__.return_value = None

        # Create an async iterator for iter_by_block
        async def mock_iter_by_block():  # noqa: RUF029
            for chunk in [b"test", b"data", b"content"]:
                yield chunk

        mock_stream.iter_by_block = mock_iter_by_block
        mock_ftp_client.download_stream = AsyncMock(return_value=mock_stream)

        # Mock GCS client and operations
        mock_client = MagicMock()
        mock_storage_client.return_value = mock_client
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        # Create and execute transfer
        obj = FTPtoGCPTransferableObject(
            source="ftp://example.com/2025/12/12/file.txt", destination="gs://test-bucket/file.txt"
        )
        await obj.transfer()

        # Verify FTP operations
        mock_ftp_context.assert_called_once_with("example.com", user="anonymous", password="anonymous")  # noqa: S106
        mock_ftp_client.change_directory.assert_called()
        mock_ftp_client.download_stream.assert_called_once_with("file.txt")

        # Verify GCS operations
        mock_storage_client.assert_called_once()
        mock_client.bucket.assert_called_once_with("test-bucket")
        mock_bucket.blob.assert_called_once_with("file.txt")
        mock_blob.upload_from_string.assert_called_once_with("testdatacontent")
