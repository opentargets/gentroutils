"""Transfer files from FTP to Google Cloud Storage (GCS)."""

import io
import re
from typing import Annotated

import aioftp
from google.cloud import storage  # type: ignore[attr-defined]
from loguru import logger
from pydantic import AfterValidator

from gentroutils.io.path import FTPPath, GCSPath
from gentroutils.io.transfer.model import TransferableObject


class FTPtoGCPTransferableObject(TransferableObject):
    """A class to represent an object that can be transferred from FTP to GCP."""

    source: Annotated[str, AfterValidator(lambda x: str(FTPPath(x)))]
    destination: Annotated[str, AfterValidator(lambda x: str(GCSPath(x)))]

    async def transfer(self) -> None:
        """Transfer files from FTP to GCP.

        This function fetches the data for the file provided in the local FTP path, collects the
        data asynchronously to buffer, and uploads it to the provided GCP bucket blob.
        """
        logger.info(f"Attempting to transfer data from {self.source} to {self.destination}.")
        gcs_obj = GCSPath(self.destination)
        ftp_obj = FTPPath(self.source)

        async with aioftp.Client.context(ftp_obj.server, user="anonymous", password="anonymous") as ftp:  # noqa: S106
            bucket = storage.Client().bucket(gcs_obj.bucket)
            blob = bucket.blob(gcs_obj.object)
            logger.info(f"Changing directory to {ftp_obj.base_dir}.")
            await ftp.change_directory(ftp_obj.base_dir)
            pwd = await ftp.get_current_directory()
            dir_match = re.match(r"^.*(?P<release_date>\d{4}\/\d{2}\/\d{2}){1}$", str(pwd))
            if dir_match:
                logger.info(f"Found release date!: {dir_match.group('release_date')}")
            buffer = io.BytesIO()
            stream = await ftp.download_stream(ftp_obj.filename)
            async with stream:
                async for block in stream.iter_by_block():
                    buffer.write(block)
            buffer.seek(0)
            content = buffer.getvalue().decode("utf-8")
            buffer.close()
            blob.upload_from_string("".join(content))
