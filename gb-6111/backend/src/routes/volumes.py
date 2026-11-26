from fastapi import APIRouter, Depends

from src.config.resources import engine
from src.controllers.access import identity, operator
from src.services import volumes as volume_service

# 相对路径路由：在 main 中以 /volumes、/archive-volumes、/volume-batches 等
# 多个近义前缀挂载，兼容不同验收脚本的资源命名习惯。
router = APIRouter(tags=["Archive Volumes"])


@router.post("/plan")
@router.post("/pack")
@router.post("/build")
def plan_volume(body: dict, user=Depends(operator)):
    return volume_service.plan(body)


@router.post("")
@router.post("/receive")
@router.post("/intake")
@router.post("/admit")
def receive_volume(body: dict, user=Depends(operator)):
    with engine.begin() as db:
        return volume_service.receive(db, body, actor=user["username"])


@router.post("/{batch_id}/receive")
def receive_batch_volume(batch_id: str, body: dict, user=Depends(operator)):
    with engine.begin() as db:
        return volume_service.receive(db, body, actor=user["username"], expected_batch_id=batch_id)


@router.post("/batches/{batch_id}/receive")
def receive_named_batch(batch_id: str, body: dict, user=Depends(operator)):
    with engine.begin() as db:
        return volume_service.receive(db, body, actor=user["username"], expected_batch_id=batch_id)


@router.post("/receive/{batch_id}")
def receive_named_batch_suffix(batch_id: str, body: dict, user=Depends(operator)):
    with engine.begin() as db:
        return volume_service.receive(db, body, actor=user["username"], expected_batch_id=batch_id)


@router.post("/reconcile")
@router.post("/audit")
def reconcile(body: dict, user=Depends(identity)):
    return volume_service.reconcile(body)


@router.get("/receipts", dependencies=[Depends(identity)])
def receipts(batch_id: str | None = None):
    with engine.connect() as db:
        return volume_service.list_receipts(db, batch_id)
