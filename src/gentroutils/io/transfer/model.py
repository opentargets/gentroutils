"""Base implementation for transferable objects in gentroutils."""

from typing import Any

from pydantic import BaseModel


class TransferableObject(BaseModel):
    """Base class for transferable objects in gentroutils.

    Each transferable object should implement the `transfer` method to define how the object is transferred.
    Also each object should have the:

        - `source`: The source location of the object.
        - `destination`: The destination location where the object will be transferred.
    """

    source: Any
    destination: Any

    def __repr__(self) -> str:
        """Return a string representation of the transferable object."""
        return f"{self.__class__.__name__}(source={self.source}, destination={self.destination})"

    def __str__(self) -> str:
        """Return a string representation of the transferable object."""
        return self.__repr__()

    def transfer(self):
        """Transfer the object to the destination."""
        raise NotImplementedError("Implement in derivative class.")

    class Config:
        """Configuration that ensures that the derivative classes can have arbitrary types."""

        arbitrary_types_allowed = True
