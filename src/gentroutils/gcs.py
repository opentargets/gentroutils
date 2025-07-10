"""Classes to represent paths in a cloud storage system."""

from __future__ import annotations

import io
import re
from urllib.parse import urlparse

import aioftp
from google.cloud import storage
from loguru import logger

from gentroutils.exceptions import GentroutilsError, GentroutilsErrorMessage


class CloudPath:
    """A class to represent a path in a cloud storage system."""

    # Supported URL schemes
    SUPPORTED_SCHEMES = ["gs"]

    def __init__(self, uri: str) -> None:
        """Initialize the CloudPath object.

        Args:
            uri (str): The path to the cloud storage object.
        """
        self.uri = uri
        # NOTE: The urlparse matches to following tuple
        # ('scheme', 'netloc', 'path', 'params', 'query', 'fragment')
        parsed_url = urlparse(uri)

        if parsed_url.scheme not in self.SUPPORTED_SCHEMES:
            raise GentroutilsError(GentroutilsErrorMessage.UNSUPPORTED_URL_SCHEME, scheme=parsed_url.scheme)

        self.bucket = parsed_url.netloc
        if not self.bucket:
            raise GentroutilsError(GentroutilsErrorMessage.BUCKET_NAME_MISSING, url=uri)

        self.object = parsed_url.path.lstrip("/").rstrip("/")
        if not self.object:
            raise GentroutilsError(GentroutilsErrorMessage.FILE_NAME_MISSING, url=uri)

    def __repr__(self) -> str:
        """Return the string representation of the CloudPath object.

        Returns:
            str: The string representation of the CloudPath object.
        """
        return self.uri


class FTPPath:
    """A class to represent a path in a cloud storage system."""

    # Supported URL schemes
    SUPPORTED_SCHEMES = ["ftp"]

    def __init__(self, uri: str) -> None:
        """Initialize the FTPPath object.

        Args:
            uri (str): The path to object in ftp server.
        """
        self.uri = uri
        # NOTE: The urlparse matches to following tuple
        # ('scheme', 'netloc', 'path', 'params', 'query', 'fragment')
        parsed_url = urlparse(uri)

        if parsed_url.scheme not in self.SUPPORTED_SCHEMES:
            raise GentroutilsError(GentroutilsErrorMessage.UNSUPPORTED_URL_SCHEME, scheme=parsed_url.scheme)

        self.server = parsed_url.netloc
        if not self.server:
            raise GentroutilsError(GentroutilsErrorMessage.FTP_SERVER_MISSING, url=uri)

        self.filename = parsed_url.path.split("/")[-1]
        if not self.filename:
            raise GentroutilsError(GentroutilsErrorMessage.FILE_NAME_MISSING, url=uri)
        self.base_dir = "/".join(parsed_url.path.split("/")[0:-1])

    def __repr__(self) -> str:
        """Return the string representation of the CloudPath object.

        Returns:
            str: The string representation of the CloudPath object.
        """
        return self.uri


class TranserableObject:
    """A class to represent an object that can be transferred."""

    def __init__(self, local: str, remote: str):
        self.local = local
        self.remote = CloudPath(remote)
        try:
            self.client = storage.Client()
        except Exception as e:
            logger.error(f"Failed to set the Google Cloud Storage client: {e}")
            raise GentroutilsError(GentroutilsErrorMessage.GCS_CLIENT_INITIALIZATION_FAILED, error=e.args[0])

    def __repr__(self) -> str:
        return f"TranserableObject(local={self.local}, remote={self.remote})"


class FTPtoGCPTranserableObject(TranserableObject):
    """A class to represent an object that can be transferred from FTP."""

    def __init__(self, local: str, remote: str):
        super().__init__(local, remote)
        self.local = FTPPath(local)

    def __repr__(self) -> str:
        return f"FTPTranserableObject(local={self.local}, remote={self.remote})"

    async def sync_from_ftp_to_gcp(self) -> None:
        """Fetch files from FTP and upload to GCP.

        This function fetches the data for the file provided in the local FTP path, collects the
        data asynchronously to buffer, and uploads it to the provided GCP bucket blob.
        """
        logger.info(f"Attempting to transfer data from {self.local} to {self.remote}.")
        gcs_obj = self.remote
        ftp_obj = self.local
        async with aioftp.Client.context(ftp_obj.server, user="anonymous", password="anonymous") as ftp:  # noqa: S106
            # Set the GCS blob
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
