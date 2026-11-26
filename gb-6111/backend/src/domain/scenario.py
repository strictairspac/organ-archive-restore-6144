from sqlalchemy import text

from .archive import apply_archive, damage_segment, export_selection, reexport
from .fixtures import FIXTURES
from .relations import RELATIONS, create_tables, validate, write_source
from .volumes import OversizedRepairOrder, volume_to_jsonable


CONFIG = {
    "title": "风栎琴作·管风琴维护卷复原",
    "team": "风栎琴作 12 人巡修与档案组",
    "description": "从整月维护卷择单归档，把关联琴体、维修工单和测记复原到隔离表，再用证据确认缺卷或传输损伤。",
    "direction": "逻辑备份与选择性恢复",
    "mechanism": "PostgreSQL 原生 COPY、外键依赖闭包和逐段 SHA-256 字节核验。",
    "modules": ["维修库卷宗", "复原范围", "归档演练", "复原证明"],
    "unit": "维护卷",
    "max_parameter": 100,
    "default_parameter": 20,
    "parameter_label": "本次最多复原工单数",
    "facts": ["源卷目录", "外键闭包", "隔离复原"],
    "palette": 11,
    "fixture": "organ-ledger-2026-11.copy",
}


def run(payload, parameter, db, capacity=None):
    validate(payload)
    source = create_tables(db, "source")
    write_source(db, source, payload)
    selected = payload["requested"][:parameter]
    capacity = capacity if capacity is not None else payload.get("volume_capacity")
    if capacity is None:
        capacity = payload.get("capacity_bytes") or payload.get("max_bytes") or payload.get("capacity")
    volume_plan = None
    if capacity is not None:
        # 按 PostgreSQL COPY 三段的真实字节贪心装卷；单单超容时形成阻断证据，
        # 不导出/发出任何分卷。
        from .volumes import plan_db_volumes

        try:
            volume_plan = plan_db_volumes(
                db, source, selected, int(capacity), payload.get("batch_id") or payload.get("batch_code")
            )
        except OversizedRepairOrder as exc:
            blocked_plan = exc.report or {}
            return {
                "outcome": "capacity_blocked",
                "blocked": True,
                "issued": False,
                "summary": str(exc),
                "oversized_visit_id": exc.visit_id,
                "oversized_order_id": exc.visit_id,
                "超限工单": exc.visit_id,
                "metrics": {
                    "入选工单": len(selected),
                    "还原工单": 0,
                    "损伤段": 0,
                    "分卷数": 0,
                    "卷容量字节": int(capacity),
                },
                "rows": [],
                "volume_plan": blocked_plan,
                "volumes": [],
            }
    segments = damage_segment(export_selection(db, source, selected), payload.get("simulate_damage"))
    bad_segments = sum(not segment.verified() for segment in segments)
    target = None
    if not bad_segments:
        target = create_tables(db, "target")
        apply_archive(db, target, segments)
    rows = []
    restored_visits = 0
    for rel, segment in zip(RELATIONS, segments, strict=True):
        count = db.execute(text(f"SELECT count(*) FROM {target[rel.key]}")).scalar_one() if target else 0
        if rel.key == "visits":
            restored_visits = count
        digest_match = target and reexport(db, target, rel).digest == segment.digest
        rows.append({
            "归档段": rel.label,
            "源卷行数": len(payload[rel.key]),
            "归档字节": len(segment.data),
            "还原行数": count,
            "SHA-256": segment.digest,
            "核验": "确认" if digest_match else ("段受损" if not segment.verified() else "未导入"),
        })
    summary = (
        f"归档校验失败（{bad_segments} 段），已阻断复原。"
        if bad_segments else f"已还原 {restored_visits} 张工单；目标外键和重新导出的字节摘要均已核验。"
    )
    if target and any(row["核验"] != "确认" for row in rows):
        raise ValueError("复原后的 COPY 字节摘要与原归档不一致")
    result = {
        "summary": summary,
        "metrics": {"入选工单": len(selected), "还原工单": restored_visits, "损伤段": bad_segments},
        "rows": rows,
    }
    if volume_plan is not None:
        result["volume_plan"] = volume_to_jsonable(volume_plan)
        result["volumes"] = result["volume_plan"]["volumes"]
        result["metrics"]["分卷数"] = len(volume_plan)
        result["metrics"]["卷容量字节"] = int(capacity)
    return result
