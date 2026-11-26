"""Compatibility module name for receiving archive volumes."""
from .volumes import *  # noqa: F401,F403
from . import volumes as _volumes


def __getattr__(name):
    return getattr(_volumes, name)
