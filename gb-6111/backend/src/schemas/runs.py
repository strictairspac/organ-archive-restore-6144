from pydantic import BaseModel, Field
from src.constants.job_state import JobState


class RunInput(BaseModel):
    parameter: int = Field(strict=True, ge=1)
    capacity: int | None = Field(default=None, strict=True, ge=1)
    volume_capacity: int | None = Field(default=None, strict=True, ge=1)
    max_bytes: int | None = Field(default=None, strict=True, ge=1)
    byte_limit: int | None = Field(default=None, strict=True, ge=1)
    batch_id: str | None = Field(default=None, max_length=120)


class RunEntry(BaseModel):
    id: int
    case_id: int
    case_title: str
    status: JobState
    parameter: int
    capacity: int | None = None
    batch_id: str | None = None
    actor: str
    created_at: int
    finished_at: int | None
    result: dict | None
    failure: str | None
    digest: str | None
