"""Gentroutils exceptions module."""

from enum import Enum


class GentroutilsErrorMessage(Enum):
    """Base class for all exceptions in the gentroutils module."""

    UNSUPPORTED_URL_SCHEME = "Unsupported URL scheme: {scheme}"
    BUCKET_NAME_MISSING = "Bucket name is missing in the URL: {url}"
    FILE_NAME_MISSING = "File name is missing in the URL: {url}"


class GentroutilsError(Exception):
    """Base class for the gentroutils exceptions."""

    def __init__(self, message: GentroutilsErrorMessage, **kwargs: str) -> None:
        """Initialize the GentroutilsError exception.

        Args:
            message (GentroutilsErrorMessage): The error message.
            **kwargs (str): Additional arguments to format the message.
        """
        super().__init__(message.value.format(**kwargs))


__all__ = ["GentroutilsError", "GentroutilsErrorMessage"]
