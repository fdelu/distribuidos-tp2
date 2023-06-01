import types

"""
Type alias for the types that can be serialized and deserialized.
"""
Translated = list["Translated"] | str | int | float | bool | None

SIMPLE_TYPES = [str, int, float, types.NoneType, bool]


class SerdeError(Exception):
    """
    Raised when a serialization or deserialization error occurs.
    """

    pass
