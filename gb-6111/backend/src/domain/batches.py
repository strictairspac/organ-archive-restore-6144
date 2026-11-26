"""Compatibility name for batch planning."""
from .batch import *  # noqa: F401,F403
from . import volumes as _volumes


def __getattr__(name):
    return getattr(_volumes, name)
