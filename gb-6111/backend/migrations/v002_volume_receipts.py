"""Version 2: durable idempotent receipts for split archive volumes."""

from sqlalchemy import inspect, text

from src.models import audit, cases, reviews, runs, users, volume_receipts  # noqa: F401
from src.models.base import metadata


def upgrade(connection):
    # Additive create_all: fresh databases get all tables; existing databases
    # only create missing tables such as volume_receipts.
    metadata.create_all(connection)

    # metadata.create_all does not add columns to an already-existing runs
    # table, so upgrade long-lived stacks explicitly.
    inspector = inspect(connection)
    run_columns = {column["name"] for column in inspector.get_columns("runs")}
    if "capacity" not in run_columns:
        connection.execute(text("ALTER TABLE runs ADD COLUMN capacity INTEGER"))
    if "batch_id" not in run_columns:
        connection.execute(text("ALTER TABLE runs ADD COLUMN batch_id VARCHAR(120)"))
