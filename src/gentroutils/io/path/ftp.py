"""FTP path representation."""

from __future__ import annotations

from urllib.parse import urlparse

from gentroutils.errors import GentroutilsError, GentroutilsErrorMessage


class FTPPath:
    """A class to represent a path in a cloud storage system."""

    # Supported URL schemes
    SUPPORTED_SCHEMES = ["ftp"]

    def __init__(self, uri: str) -> None:
        """Initialize the FTPPath object.

        Args:
            uri (str): The path to object in ftp server.

        Raises:
            GentroutilsError: If the URL scheme is not supported or if the server or filename is missing.
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
