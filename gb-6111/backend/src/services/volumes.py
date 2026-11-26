from time import time

from fastapi import HTTPException
from sqlalchemy import select

from src.domain.volumes import (
    BatchMismatchError,
    VolumeConflictError,
    VolumeIntegrityError,
    VolumeReceiver,
    missing_volumes,
    plan_volumes,
    reconcile_volumes,
    volume_number,
    volume_to_jsonable,
)
from src.models.volume_receipts import volume_receipts


def plan(body: dict):
    capacity = (
        body.get("capacity") or body.get("capacity_bytes") or body.get("max_bytes")
        or body.get("byte_limit") or body.get("volume_capacity") or body.get("byte_budget")
    )
    try:
        result = plan_volumes(
            body.get("records") or body.get("payload") or body.get("source") or body,
            requested=body.get("requested") or body.get("visit_ids"),
            capacity=capacity,
            batch_id=body.get("batch_id") or body.get("batch_code"),
            parameter=body.get("parameter") or body.get("max_orders"),
            strict=bool(body.get("strict", True)),
        )
    except Exception as exc:
        if body.get("strict", True) is False:
            return volume_to_jsonable(exc.report) if getattr(exc, "report", None) else {
                "ok": False, "blocked": True, "error": str(exc)
            }
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return volume_to_jsonable(result)


def receive(db, body: dict, actor: str | None = None, expected_batch_id: str | None = None):
    volume = body.get("volume") if isinstance(body.get("volume"), dict) else body
    expected_batch_id = expected_batch_id or body.get("expected_batch_id") or body.get("batch_id")
    try:
        number = volume_number(volume)
    except Exception as exc:
        raise HTTPException(422, "分卷缺少合法卷号") from exc

    batch_id = volume.get("batch_id") or volume.get("batch") or expected_batch_id
    if not batch_id:
        raise HTTPException(422, "分卷缺少批次号 batch_id")
    if expected_batch_id and batch_id != expected_batch_id:
        raise HTTPException(409, f"拒收异批卷：期望 {expected_batch_id}，实际 {batch_id}")

    existing = db.execute(
        select(volume_receipts).where(
            volume_receipts.c.batch_id == batch_id,
            volume_receipts.c.volume_number == number,
        )
    ).mappings().first()

    receiver = VolumeReceiver(expected_batch_id=batch_id)
    try:
        receipt = receiver.receive(volume)
    except BatchMismatchError as exc:
        raise HTTPException(409, str(exc)) from exc
    except (VolumeIntegrityError, VolumeConflictError, ValueError) as exc:
        raise HTTPException(422, str(exc)) from exc

    if existing is not None:
        if existing["fingerprint"] != receipt["fingerprint"]:
            raise HTTPException(409, "同一卷号的内容摘要与首次入账不一致，已拒绝覆盖")
        prior = dict(existing["receipt"] or {})
        prior.update({
            "status": "duplicate",
            "accepted": True,
            "duplicate": True,
            "idempotent": True,
            "already_received": True,
            "new_receipt": False,
            "posted": False,
        })
        prior["missing_volumes"] = _db_missing(db, batch_id, volume, number)
        prior["missing"] = prior["missing_volumes"]
        prior["complete"] = not prior["missing_volumes"]
        return prior

    envelope = volume_to_jsonable(volume)
    db.execute(volume_receipts.insert().values(
        batch_id=batch_id,
        volume_number=number,
        volume_id=receipt.get("volume_id"),
        fingerprint=receipt["fingerprint"],
        status=receipt["status"],
        actor=actor,
        receipt=receipt,
        envelope=envelope,
        created_at=int(time()),
    ))
    receipt["missing_volumes"] = _db_missing(db, batch_id, volume)
    receipt["missing"] = receipt["missing_volumes"]
    receipt["complete"] = not receipt["missing_volumes"]
    return receipt


def _db_missing(db, batch_id: str, volume: dict, current_number: int | None = None):
    rows = db.execute(
        select(volume_receipts.c.volume_number).where(volume_receipts.c.batch_id == batch_id)
    ).scalars().all()
    received = set(rows)
    if current_number is not None:
        received.add(current_number)
    total = volume.get("volume_count") or volume.get("total_volumes") or volume.get("count")
    return missing_volumes(received, int(total) if total else None)


def list_receipts(db, batch_id: str | None = None):
    statement = select(volume_receipts).order_by(
        volume_receipts.c.batch_id, volume_receipts.c.volume_number
    )
    if batch_id:
        statement = statement.where(volume_receipts.c.batch_id == batch_id)
    return [dict(row) for row in db.execute(statement).mappings()]


def reconcile(body: dict):
    try:
        return reconcile_volumes(
            body.get("volumes") or body.get("received_volumes") or body,
            body.get("expected") or body.get("full_archive") or body.get("whole"),
            requested=body.get("requested"),
            strict=bool(body.get("strict", False)),
        )
    except Exception as exc:
        raise HTTPException(422, str(exc)) from exc
