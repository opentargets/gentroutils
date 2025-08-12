"""Module for handling transfer operations in gentroutils."""

from gentroutils.io.transfer.ftp_to_gcs import FTPtoGCPTransferableObject
from gentroutils.io.transfer.polars_to_gcs import PolarsDataFrameToGCSTransferableObject

__all__ = ["FTPtoGCPTransferableObject", "PolarsDataFrameToGCSTransferableObject"]
