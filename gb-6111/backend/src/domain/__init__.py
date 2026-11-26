"""Project-specific inputs, operations, and acceptance evidence."""

import importlib


def __getattr__(name):
    # 让 ``from src.domain import pack_volumes, VolumeReceiver`` 等入口可用，
    # 同时避免导入 domain 包时强制加载 SQLAlchemy/PostgreSQL 相关模块。
    if name in {"volumes"}:
        return importlib.import_module(".volumes", __name__)
    volumes = importlib.import_module(".volumes", __name__)
    if hasattr(volumes, name):
        return getattr(volumes, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
