"""Ütility functions for the CLI."""

import asyncio
from functools import wraps


def coro(f):
    """Corutine wrapper for synchronous functions."""

    @wraps(f)
    def wrapper(*args, **kwargs):
        """Wrapper around the synchronous function."""
        return asyncio.run(f(*args, **kwargs))

    return wrapper


__all__ = ["coro"]
