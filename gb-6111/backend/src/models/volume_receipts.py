from sqlalchemy import JSON, BigInteger, Column, Integer, String, Table, UniqueConstraint

from .base import metadata

# 分卷收卷回执。(batch_id, volume_number) 唯一，保证重试同一卷不会重复入账。
volume_receipts = Table(
    "volume_receipts",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("batch_id", String(120), nullable=False, index=True),
    Column("volume_number", Integer, nullable=False),
    Column("volume_id", String(180), nullable=True),
    Column("fingerprint", String(96), nullable=False),
    Column("status", String(24), nullable=False),
    Column("actor", String(48), nullable=True),
    Column("receipt", JSON(none_as_null=True), nullable=False),
    Column("envelope", JSON(none_as_null=True), nullable=True),
    Column("created_at", BigInteger, nullable=False),
    UniqueConstraint("batch_id", "volume_number", name="uq_volume_receipt_batch_number"),
)
