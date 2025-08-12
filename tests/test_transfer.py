from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from gentroutils.errors import GentroutilsError
from gentroutils.io.transfer import FTPtoGCPTransferableObject, PolarsDataFrameToGCSTransferableObject
from gentroutils.transfer import TransferManager


class TestTransferManager:
    """Test TransferManager class."""

    @pytest.mark.asyncio
    async def test_transfer_ftp_to_gcp(self):
        """Test FTP to GCP transfer method."""
        # Create mock transferable objects
        mock_obj1 = MagicMock(spec=FTPtoGCPTransferableObject)

        # Mock the transfer method to be async
        mock_obj1.transfer = AsyncMock()

        transferable_objects = [mock_obj1]

        # Execute the transfer
        await TransferManager.transfer_ftp_to_gcp(transferable_objects)

        # Verify both objects had their transfer method called
        mock_obj1.transfer.assert_called_once()

        # Ensure the event loop was run
        mock_obj1.transfer.assert_awaited()

    @pytest.mark.asyncio
    async def test_transfer_polars_to_gcs(self):
        """Test Polars DataFrame to GCS transfer method."""
        # Create mock transferable objects
        mock_obj1 = MagicMock(spec=PolarsDataFrameToGCSTransferableObject)

        # Mock the transfer method to be async
        mock_obj1.transfer = AsyncMock()

        transferable_objects = [mock_obj1]

        # Execute the transfer
        await TransferManager.transfer_polars_to_gcs(transferable_objects)

        # Verify both objects had their transfer method called
        mock_obj1.transfer.assert_called_once()

        # Ensure the event loop was run
        mock_obj1.transfer.assert_awaited()

    def test_transfer_no_objects(self):
        """Test transfer method with no transferable objects."""
        with pytest.raises(GentroutilsError, match="Transferable objects list cannot be empty"):
            TransferManager().transfer([])

    def test_transfer_invalid_objects(self):
        """Test transfer method with invalid transferable objects."""
        mock_obj = MagicMock(spec=object)
        with pytest.raises(GentroutilsError, match="Invalid transferable objects provided"):
            TransferManager().transfer([mock_obj])

    def test_transfer_mixed_objects(self):
        """Test transfer method with mixed transferable objects."""
        mock_ftp_obj = MagicMock(spec=FTPtoGCPTransferableObject)
        mock_polars_obj = MagicMock(spec=PolarsDataFrameToGCSTransferableObject)

        with pytest.raises(GentroutilsError, match="Invalid transferable objects provided"):
            TransferManager().transfer([mock_ftp_obj, mock_polars_obj])

    @patch("gentroutils.transfer.asyncio.run")
    def test_transfer_ftp_objects(self, mock_asyncio_run):
        """Test transfer method with FTP transferable objects."""
        mock_ftp_obj = MagicMock(spec=FTPtoGCPTransferableObject)

        transfer_manager = TransferManager()
        transfer_manager.transfer([mock_ftp_obj])

        # Verify that asyncio.run was called with the correct method
        mock_asyncio_run.assert_called_once()
        assert mock_asyncio_run.called

    @patch("gentroutils.transfer.asyncio.run")
    def test_transfer_polars_objects(self, mock_asyncio_run):
        """Test transfer method with Polars transferable objects."""
        mock_polars_obj = MagicMock(spec=PolarsDataFrameToGCSTransferableObject)

        transfer_manager = TransferManager()
        transfer_manager.transfer([mock_polars_obj])

        # Verify that asyncio.run was called with the correct method
        mock_asyncio_run.assert_called_once()
        assert mock_asyncio_run.called
