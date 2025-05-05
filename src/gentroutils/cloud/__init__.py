"""Classes to represent paths in a cloud storage system."""

from __future__ import annotations

from urllib.parse import urlparse

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
