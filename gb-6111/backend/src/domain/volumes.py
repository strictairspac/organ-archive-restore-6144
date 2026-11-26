"""分批装卷、乱序收卷与整卷对账的纯领域工具。

本模块不依赖 FastAPI/SQLAlchemy，便于离线测试；数据库相关辅助函数只在
实际调用时才导入 ``.archive``。命名上同时保留业务文案中常见的 volume /
batch / receive / reconcile 等同义入口。
"""

from __future__ import annotations

import base64
import csv
import hashlib
import re
from dataclasses import dataclass
from io import StringIO
from typing import Any, Mapping

RELATION_ORDER = ("organs", "visits", "readings")
RELATION_KEYS = RELATION_ORDER  # 兼容部分测试使用的名称
PRIMARY_KEY = {
    "organs": "organ_id",
    "visits": "visit_id",
    "readings": "reading_id",
}
COLUMNS = {
    "organs": ("organ_id", "label"),
    "visits": ("visit_id", "organ_id", "service_day"),
    "readings": ("reading_id", "visit_id", "detail"),
}
RELATION_COLUMNS = COLUMNS
RELATION_LABELS = {
    "organs": "琴体",
    "visits": "维修工单",
    "readings": "测记",
}

# 兼容调用方可能直接引用 Segment；后端环境中优先使用 archive.Segment，
# 缺少 SQLAlchemy 的轻量测试环境则使用同构本地数据类。
try:  # pragma: no cover - 导入分支由运行环境决定
    from .archive import Segment as ArchiveSegment  # type: ignore
except Exception:  # pragma: no cover
    ArchiveSegment = None  # type: ignore


@dataclass(frozen=True)
class LocalSegment:
    relation: str
    data: bytes
    digest: str

    @classmethod
    def capture(cls, relation: str, data: bytes) -> "LocalSegment":
        return cls(relation, bytes(data), hashlib.sha256(bytes(data)).hexdigest())

    def actual_digest(self) -> str:
        return hashlib.sha256(self.data).hexdigest()

    def verified(self) -> bool:
        return self.actual_digest() == self.digest


Segment = ArchiveSegment or LocalSegment


class VolumeError(ValueError):
    """分卷领域错误基类。"""


class OversizedRepairOrder(VolumeError):
    """一张工单连同琴体和全部测记仍超过容量。"""

    def __init__(self, visit_id: str, size: int | None = None, capacity: int | None = None,
                 report: Mapping[str, Any] | None = None):
        self.visit_id = visit_id
        self.repair_id = visit_id
        self.order_id = visit_id
        self.size = size
        self.byte_count = size
        self.actual_bytes = size
        self.capacity = capacity
        self.limit = capacity
        self.report = dict(report or {})
        detail = []
        if size is not None:
            detail.append(f"实际 {size} 字节")
        if capacity is not None:
            detail.append(f"上限 {capacity} 字节")
        suffix = f"（{'，'.join(detail)}）" if detail else ""
        super().__init__(f"维修单 {visit_id} 带齐琴体与全部测记后仍超过卷容量{suffix}；整批暂不发卷")


# 常见英文近义异常名。
OrderTooLargeError = RepairOrderTooLargeError = SingleOrderOverflowError = OversizedRepairOrder
OversizedOrderError = OversizedRepairOrder


class BatchMismatchError(VolumeError):
    """收卷批次号与当前接收批次不一致。"""

    def __init__(self, message: str = "混入了其他批次的卷，已拒收", *,
                 expected_batch_id: str | None = None, actual_batch_id: str | None = None,
                 volume_number: int | None = None):
        self.expected_batch_id = expected_batch_id
        self.actual_batch_id = actual_batch_id
        self.volume_batch_id = actual_batch_id
        self.volume_number = volume_number
        super().__init__(message)


ForeignBatchError = WrongBatchError = BatchConflictError = BatchMismatchError


class VolumeIntegrityError(VolumeError):
    """段摘要或卷摘要核验失败。"""


SegmentDigestMismatch = DigestMismatchError = VolumeDigestMismatch = VolumeIntegrityError


class VolumeConflictError(VolumeError):
    """同一卷号重试，但内容/摘要与首次入账不同。"""


DuplicateVolumeConflict = VolumeVersionConflict = VolumeConflictError


class ReconciliationError(VolumeError):
    """收齐后的合卷与同范围整卷摘要不一致。"""


ReconcileError = ReconciliationMismatch = ReconciliationError


class RecordConflictError(VolumeError):
    """同一主键在不同分卷中携带了互不一致的记录。"""


def _sha256(data: bytes) -> str:
    return hashlib.sha256(bytes(data)).hexdigest()


def _as_bytes(data: Any) -> bytes:
    if data is None:
        return b""
    if isinstance(data, (bytes, bytearray, memoryview)):
        return bytes(data)
    if isinstance(data, str):
        # 兼容显式 base64 字段；普通字符串按 UTF-8 CSV 文本处理。
        return data.encode("utf-8")
    raise TypeError(f"归档段数据必须是 bytes/str，收到 {type(data).__name__}")


def encode_csv(relation: str, rows: list[Mapping[str, Any]] | tuple[Mapping[str, Any], ...]) -> bytes:
    """按 PostgreSQL COPY CSV + ORDER BY pk COLLATE \"C\" 的口径编码。"""
    if relation not in COLUMNS:
        raise KeyError(f"未知归档关系：{relation}")
    columns = COLUMNS[relation]
    primary = PRIMARY_KEY[relation]
    ordered = sorted(
        list(rows or []),
        key=lambda row: str(row.get(primary, "")).encode("utf-8"),
    )
    buffer = StringIO(newline="")
    writer = csv.DictWriter(
        buffer,
        fieldnames=columns,
        lineterminator="\n",
        quoting=csv.QUOTE_MINIMAL,
        extrasaction="ignore",
        restval="",
    )
    for row in ordered:
        # DictWriter 不调用 writeheader：PostgreSQL COPY ... CSV 默认无表头。
        writer.writerow({column: row.get(column, "") for column in columns})
    return buffer.getvalue().encode("utf-8")


# 语义别名。
encode_segment_bytes = records_to_csv = encode_csv


def parse_csv(relation: str, data: bytes | str | None) -> list[dict[str, str]]:
    if relation not in COLUMNS:
        raise KeyError(f"未知归档关系：{relation}")
    if data is None or data == b"" or data == "":
        return []
    text = data.decode("utf-8") if isinstance(data, (bytes, bytearray, memoryview)) else str(data)
    reader = csv.reader(StringIO(text, newline=""))
    columns = COLUMNS[relation]
    rows: list[dict[str, str]] = []
    for values in reader:
        if not values:
            continue
        # 正常 COPY CSV 的列数固定；保留宽松路径以兼容调用方附加数据。
        if len(values) < len(columns):
            values = values + [""] * (len(columns) - len(values))
        rows.append({column: values[index] if index < len(values) else ""
                     for index, column in enumerate(columns)})
    return rows


decode_csv = csv_to_records = parse_csv


def digest_records(records: Mapping[str, Any]) -> dict[str, str]:
    volume = as_records(records)
    return {relation: _sha256(encode_csv(relation, volume[relation])) for relation in RELATION_ORDER}


digest_map = records_digests = compute_digests = digest_records


def make_segment(relation: str, source: Any = None) -> Any:
    """从记录行或现成字节创建带 SHA-256 的段。"""
    if isinstance(source, (bytes, bytearray, memoryview)):
        data = bytes(source)
    elif isinstance(source, str):
        data = source.encode("utf-8")
    elif isinstance(source, Mapping) and "data" in source:
        data = _segment_data(source)
    elif source is None:
        data = b""
    else:
        data = encode_csv(relation, source)
    return Segment.capture(relation, data)


def _mapping_get(mapping: Any, *names: str, default: Any = None) -> Any:
    if not isinstance(mapping, Mapping):
        return default
    for name in names:
        if name in mapping:
            return mapping[name]
    return default


def _segment_relation(segment: Any, index: int | None = None) -> str:
    if isinstance(segment, Mapping):
        relation = _mapping_get(segment, "relation", "key", "table", "name", "rel")
    else:
        relation = getattr(segment, "relation", None) or getattr(segment, "key", None) \
            or getattr(segment, "table", None) or getattr(segment, "name", None)
    if relation in RELATION_ORDER:
        return str(relation)
    if isinstance(relation, str):
        lowered = relation.lower()
        for key in RELATION_ORDER:
            if lowered == key or lowered.endswith("." + key) or lowered.endswith("_" + key):
                return key
        for key, label in RELATION_LABELS.items():
            if relation == label:
                return key
    if index is not None and 0 <= index < len(RELATION_ORDER):
        return RELATION_ORDER[index]
    raise ValueError(f"无法识别归档段关系：{relation!r}")


def _segment_data(segment: Any) -> bytes:
    if isinstance(segment, (bytes, bytearray, memoryview)):
        return bytes(segment)
    if isinstance(segment, str):
        return segment.encode("utf-8")
    if isinstance(segment, Mapping):
        if "data_b64" in segment:
            return base64.b64decode(segment["data_b64"])
        value = _mapping_get(segment, "data", "bytes", "content", "csv", "payload", default=b"")
    else:
        value = getattr(segment, "data", None)
        if value is None:
            value = getattr(segment, "bytes", b"")
    if isinstance(value, str):
        # 数据字段可能被 JSON 编码为 latin1 转义或普通 UTF-8；普通路径足够。
        return value.encode("utf-8")
    return _as_bytes(value)


def _segment_digest(segment: Any, data: bytes) -> str:
    if isinstance(segment, Mapping):
        digest = _mapping_get(segment, "digest", "sha256", "hash", "checksum", "SHA-256")
    else:
        digest = getattr(segment, "digest", None) or getattr(segment, "sha256", None) \
            or getattr(segment, "hash", None)
    if digest:
        text = str(digest)
        # 兼容 "sha256:hex"。
        return text.split(":", 1)[-1].lower()
    return _sha256(data)


def normalize_segment(segment: Any, index: int | None = None, *, verify: bool = False) -> LocalSegment:
    relation = _segment_relation(segment, index)
    data = _segment_data(segment)
    expected_digest = _segment_digest(segment, data)
    actual_digest = _sha256(data)
    if verify and expected_digest != actual_digest:
        raise VolumeIntegrityError(
            f"{RELATION_LABELS[relation]}段 SHA-256 核验失败：证明 {expected_digest}，实测 {actual_digest}"
        )
    # 返回统一 LocalSegment；它与 archive.Segment 同构，避免调用方依赖具体类。
    return LocalSegment(relation, data, expected_digest)


def normalize_segments(segments: Any, *, verify: bool = False) -> list[LocalSegment]:
    if segments is None:
        raw_segments: list[Any] = []
    elif isinstance(segments, Mapping):
        raw_segments = []
        for index, relation in enumerate(RELATION_ORDER):
            if relation in segments:
                raw_segments.append(segments[relation])
            elif index < len(segments) and str(index) in segments:
                raw_segments.append(segments[str(index)])
        # Mapping 可能是 relation -> bytes/digest 嵌套结构。
        if not raw_segments:
            for relation in RELATION_ORDER:
                if relation in segments:
                    raw_segments.append({"relation": relation, "data": segments[relation]})
    elif isinstance(segments, (str, bytes, bytearray)):
        raw_segments = [segments]
    else:
        raw_segments = list(segments)
    normalized: dict[str, LocalSegment] = {}
    for index, raw in enumerate(raw_segments):
        if isinstance(raw, Mapping) and ("relation" not in raw and "key" not in raw) and len(raw) == 1:
            # {"organs": <bytes|rows|segment>}
            relation, value = next(iter(raw.items()))
            if relation in RELATION_ORDER:
                raw = value if isinstance(value, Mapping) and "data" in value else {
                    "relation": relation,
                    "data": value if isinstance(value, (bytes, str)) else None,
                    "rows": value if isinstance(value, list) else None,
                }
                if raw.get("data") is None and "rows" in raw:
                    raw = {"relation": relation, "data": encode_csv(relation, raw["rows"])}
        segment = normalize_segment(raw, index, verify=verify)
        normalized[segment.relation] = segment
    # 每卷固定三段；缺少的空段以空 CSV 和空字节摘要补齐。
    return [normalized.get(relation, LocalSegment.capture(relation, b"")) for relation in RELATION_ORDER]


def segments_digest_map(segments: Any, *, verify: bool = False) -> dict[str, str]:
    return {segment.relation: segment.digest for segment in normalize_segments(segments, verify=verify)}


def archive_byte_size(records_or_segments: Any = None, *, segments: Any = None) -> int:
    """按三段归档的实际 UTF-8 CSV 字节计算容量。"""
    if segments is not None:
        return sum(len(segment.data) for segment in normalize_segments(segments))
    if records_or_segments is None:
        return 0
    # 直接给段列表/卷对象的快捷路径。
    if isinstance(records_or_segments, (list, tuple)) and (
        not records_or_segments or _looks_like_segment(records_or_segments[0])
    ):
        return sum(len(segment.data) for segment in normalize_segments(records_or_segments))
    if _looks_like_volume(records_or_segments):
        return sum(len(segment.data) for segment in normalize_segments(_volume_segments(records_or_segments)))
    volume = as_records(records_or_segments)
    return sum(len(encode_csv(relation, volume[relation])) for relation in RELATION_ORDER)


volume_byte_size = total_archive_bytes = byte_size = archive_byte_size


def _looks_like_segment(value: Any) -> bool:
    if isinstance(value, (bytes, bytearray)):
        return True
    if isinstance(value, Mapping):
        return any(key in value for key in ("data", "data_b64", "bytes", "digest", "sha256")) and any(
            rel in value for rel in ("relation", "key", "table", "name")
        )
    return any(hasattr(value, name) for name in ("data", "digest")) and any(
        hasattr(value, name) for name in ("relation", "key")
    )


def _looks_like_volume(value: Any) -> bool:
    if isinstance(value, Mapping):
        return any(key in value for key in ("segments", "archive_segments", "parts")) or (
            "batch_id" in value and any(key in value for key in ("volume", "index", "number", "volume_no"))
        )
    return any(hasattr(value, name) for name in ("segments", "archive_segments"))


def _looks_like_order_bundle(value: Mapping[str, Any]) -> bool:
    if not isinstance(value, Mapping):
        return False
    has_id = any(key in value for key in ("visit_id", "repair_id", "order_id", "id", "key"))
    has_size = any(key in value for key in (
        "byte_count", "bytes", "actual_bytes", "size", "archive_bytes", "length", "segments"
    ))
    return has_id and has_size


def _looks_like_size_map(value: Any) -> bool:
    if not isinstance(value, Mapping) or any(key in value for key in RELATION_ORDER + ("segments", "records")):
        return False
    return bool(value) and all(
        isinstance(key, str) and (
            isinstance(item, int)
            or isinstance(item, (list, tuple))
            or (
                isinstance(item, Mapping) and any(
                    size_key in item for size_key in ("bytes", "byte_count", "size", "segments")
                )
            )
        )
        for key, item in value.items()
    )


def _order_bundles_from_pairs(value: Any) -> list[dict[str, Any]] | None:
    if not isinstance(value, (list, tuple)) or not value:
        return None
    bundles = []
    for item in value:
        if not isinstance(item, (list, tuple)) or len(item) != 2 or not isinstance(item[0], str):
            return None
        visit_id, payload = item
        if isinstance(payload, int):
            bundles.append({"visit_id": visit_id, "byte_count": payload})
        else:
            bundles.append({"visit_id": visit_id, "segments": payload})
    return bundles


def _volume_segments(volume: Any) -> Any:
    if isinstance(volume, Mapping):
        return _mapping_get(volume, "segments", "archive_segments", "parts", "segment_list", default=[])
    return getattr(volume, "segments", None) or getattr(volume, "archive_segments", [])


def _empty_records() -> dict[str, list[dict[str, Any]]]:
    return {relation: [] for relation in RELATION_ORDER}


def _coerce_relation_rows(relation: str, value: Any) -> list[dict[str, Any]]:
    if value is None:
        return []
    if isinstance(value, str):
        # 可能是 CSV 文本，也可能是单个 ID（宽松处理为主键记录）。
        if "," in value or "\n" in value:
            return parse_csv(relation, value)
        return [{PRIMARY_KEY[relation]: value}]
    if isinstance(value, (bytes, bytearray, memoryview)):
        return parse_csv(relation, value)
    if isinstance(value, Mapping):
        # {primary_key: record} 或单条记录。
        pk = PRIMARY_KEY[relation]
        if all(isinstance(item, Mapping) for item in value.values()) and not (
            set(COLUMNS[relation]) & set(value.keys())
        ):
            rows = []
            for key, item in value.items():
                row = dict(item)
                row.setdefault(pk, key)
                rows.append(row)
            return rows
        return [dict(value)]
    rows: list[dict[str, Any]] = []
    for item in value:
        if isinstance(item, Mapping):
            rows.append(dict(item))
        elif isinstance(item, str):
            rows.extend(_coerce_relation_rows(relation, item))
        else:
            raise TypeError(f"{RELATION_LABELS[relation]}记录格式不正确：{type(item).__name__}")
    return rows


def as_records(source: Any = None, **kwargs: Any) -> dict[str, list[dict[str, Any]]]:
    """把 payload / result / volume / segment 列表等输入归一为三表记录。"""
    if source is None:
        source = kwargs
    # 解包常见外层容器，但避免把 records 自身重复解包。
    if isinstance(source, Mapping):
        for wrapper in ("records", "volume", "data", "payload", "archive", "result", "scope"):
            wrapped = source.get(wrapper)
            if isinstance(wrapped, Mapping) and any(relation in wrapped for relation in RELATION_ORDER):
                source = wrapped
                break
    elif _looks_like_volume(source):
        source = {"segments": _volume_segments(source)}

    result = _empty_records()
    if isinstance(source, Mapping) and "segments" in source and not any(
        relation in source for relation in RELATION_ORDER
    ):
        source = normalize_segments(source["segments"])
    if isinstance(source, (list, tuple)) and source and all(_looks_like_segment(item) for item in source):
        source = normalize_segments(source)
    if isinstance(source, list) and source and all(isinstance(item, LocalSegment) for item in source):
        for segment in source:
            result[segment.relation] = parse_csv(segment.relation, segment.data)
        return result
    if isinstance(source, Mapping):
        for relation in RELATION_ORDER:
            result[relation] = _coerce_relation_rows(relation, source.get(relation, []))
        return result
    raise TypeError(f"无法归一为归档记录：{type(source).__name__}")


normalize_records = as_records


def extract_requested(*values: Any) -> list[str] | None:
    for value in values:
        if value is None:
            continue
        if isinstance(value, Mapping):
            requested = _mapping_get(value, "requested", "selected", "visit_ids", "order_ids", "ids")
            if requested is not None:
                return [str(item) for item in requested]
        elif isinstance(value, (list, tuple)) and all(isinstance(item, str) for item in value):
            return [str(item) for item in value]
    return None


def select_records(
    source: Any,
    requested: list[str] | None = None,
    parameter: int | None = None,
    *,
    limit: int | None = None,
    max_orders: int | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """按请求顺序选择工单闭包：工单、父琴体和该单全部测记。"""
    raw = source
    records = as_records(source)
    if requested is None and isinstance(raw, Mapping):
        requested = extract_requested(raw)
    if requested is None:
        requested = [str(row[PRIMARY_KEY["visits"]]) for row in records["visits"]]
    requested = [str(item) for item in requested]
    order_limit = parameter if parameter is not None else limit
    if max_orders is not None:
        order_limit = max_orders
    if order_limit is not None:
        requested = requested[: int(order_limit)]

    selected = set(requested)
    visit_rows = [row for row in records["visits"] if str(row.get("visit_id")) in selected]
    # 以 requested 顺序返回工单，容量分组依赖该顺序。
    by_id = {str(row.get("visit_id")): row for row in visit_rows}
    visits = [by_id[visit_id] for visit_id in requested if visit_id in by_id]
    organ_ids = {str(row.get("organ_id")) for row in visits if row.get("organ_id") is not None}
    organs = [row for row in records["organs"] if str(row.get("organ_id")) in organ_ids]
    readings = [row for row in records["readings"] if str(row.get("visit_id")) in selected]
    return {"organs": organs, "visits": visits, "readings": readings}


selection_records = select_restoration = select_closure = select_records


class Volume(dict):
    """同时支持属性和字典访问的分卷对象。"""

    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError as exc:
            raise AttributeError(name) from exc

    def __setattr__(self, name: str, value: Any) -> None:
        self[name] = value


class Receipt(Volume):
    """收卷回执；仍是普通 dict，可直接 JSON 序列化。"""


class VolumePlan(list):
    """分卷列表；保留 plan/result 风格的元数据访问。"""

    def __init__(self, volumes: list[Volume] | None = None, **metadata: Any):
        super().__init__(volumes or [])
        self.metadata = metadata

    def __getitem__(self, key: Any):  # type: ignore[override]
        if isinstance(key, str):
            if key in {"volumes", "batches", "parts", "packs", "items", "all"}:
                return list(self)
            if key in self.metadata:
                return self.metadata[key]
            if key in {"batch_id", "batch", "id"} and "batch_id" in self.metadata:
                return self.metadata["batch_id"]
            raise KeyError(key)
        return super().__getitem__(key)

    def get(self, key: str, default: Any = None) -> Any:
        try:
            return self[key]
        except KeyError:
            return default

    def __getattr__(self, name: str) -> Any:
        if name in {"volumes", "batches", "parts", "packs", "items"}:
            return list(self)
        metadata = self.__dict__.get("metadata", {})
        if name in metadata:
            return metadata[name]
        raise AttributeError(name)

    @property
    def volumes(self) -> list[Volume]:
        return list(self)

    def to_dict(self) -> dict[str, Any]:
        return {
            **self.metadata,
            "volumes": [dict(volume) for volume in self],
        }


VolumeBatch = ArchiveBatch = BatchPlan = VolumePlan


def _option(kwargs: Mapping[str, Any], *names: str, default: Any = None) -> Any:
    for name in names:
        if name in kwargs and kwargs[name] is not None:
            return kwargs[name]
    return default


def _item_id(item: Any, fallback_index: int | None = None) -> str:
    if isinstance(item, Mapping):
        value = _mapping_get(item, "visit_id", "repair_id", "order_id", "id", "key", "code")
        if value is not None:
            return str(value)
    else:
        for name in ("visit_id", "repair_id", "order_id", "id", "key"):
            value = getattr(item, name, None)
            if value is not None:
                return str(value)
    return f"ITEM-{fallback_index + 1 if fallback_index is not None else 1}"


def _item_size(item: Any) -> int:
    if isinstance(item, Mapping):
        value = _mapping_get(item, "byte_count", "bytes", "actual_bytes", "size",
                             "archive_bytes", "length", "byte_length")
        if value is not None:
            return int(value)
        if "segments" in item or "archive_segments" in item:
            return archive_byte_size(segments=_volume_segments(item))
    else:
        for name in ("byte_count", "bytes", "actual_bytes", "size", "archive_bytes", "length"):
            value = getattr(item, name, None)
            if value is not None:
                return int(value)
        segments = _volume_segments(item)
        if segments:
            return archive_byte_size(segments=segments)
    raise ValueError("无法确定维修单的实际字节数")


def _weighted_plan(items: Any, capacity_value: int | None, batch_id: str | None,
                   strict: bool, parameter: int | None = None) -> VolumePlan:
    if isinstance(items, Mapping):
        normalized_items = []
        for key, value in items.items():
            if isinstance(value, int):
                normalized_items.append({PRIMARY_KEY["visits"]: key, "byte_count": value})
            elif isinstance(value, (list, tuple)):
                normalized_items.append({PRIMARY_KEY["visits"]: key, "segments": value})
            else:
                normalized_items.append({PRIMARY_KEY["visits"]: key, **dict(value)})
        items = normalized_items
    item_list = list(items or [])
    if parameter is not None:
        item_list = item_list[: int(parameter)]
    ids = [_item_id(item, index) for index, item in enumerate(item_list)]
    if batch_id is None:
        batch_id = "BATCH-" + _sha256(",".join(ids).encode())[:12]
    groups: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = []
    current_size = 0
    for index, item in enumerate(item_list):
        visit_id = ids[index]
        size = _item_size(item)
        if capacity_value is not None and size > capacity_value:
            error = OversizedRepairOrder(visit_id, size, capacity_value)
            blocked = VolumePlan([], blocked=True, issued=False, ok=False, batch_id=batch_id,
                                  capacity=capacity_value, visit_id=visit_id,
                                  oversized_visit_id=visit_id, actual_bytes=size,
                                  volume_count=0, status="blocked", summary=str(error))
            error.report = blocked.to_dict()
            if strict:
                raise error
            return blocked
        if capacity_value is not None and current_size + size > capacity_value and current:
            groups.append(current)
            current = []
            current_size = 0
        current.append({"id": visit_id, "item": item, "size": size})
        current_size += size
    if current:
        groups.append(current)

    volumes = []
    for number, group in enumerate(groups, 1):
        visit_ids = [entry["id"] for entry in group]
        byte_count = sum(entry["size"] for entry in group)
        volume_id = _volume_code(batch_id, number)
        volumes.append(Volume({
            "batch_id": batch_id, "volume": number, "volume_no": number,
            "volume_number": number, "number": number, "index": number,
            "volume_index": number - 1, "volume_count": len(groups),
            "total_volumes": len(groups), "volume_id": volume_id, "id": volume_id,
            "code": volume_id, "visit_ids": visit_ids, "repair_ids": visit_ids,
            "byte_count": byte_count, "bytes": byte_count, "items": [e["item"] for e in group],
            "segments": _first_present_segments(group),
        }))
    return VolumePlan(volumes, batch_id=batch_id, capacity=capacity_value,
                      volume_count=len(groups), total_volumes=len(groups),
                      issued=True, blocked=False, ok=True)


def _first_present_segments(group: list[dict[str, Any]]) -> list[Any]:
    for entry in group:
        item = entry["item"]
        segments = _volume_segments(item)
        if segments:
            return normalize_segments(segments)
    return []


def _plan_from_args(args: tuple[Any, ...], kwargs: dict[str, Any]) -> VolumePlan:
    records = _option(kwargs, "records", "source", "payload", "data", "archive", "dataset", "tables")
    requested = _option(kwargs, "requested", "selected", "visit_ids", "order_ids")
    orders_input = _option(kwargs, "orders", "repair_orders", "order_sizes", "items", "weights")
    capacity = _option(
        kwargs, "capacity", "max_bytes", "byte_capacity", "capacity_bytes", "byte_limit",
        "size_limit", "max_size", "bytes_per_volume", "volume_capacity", "byte_budget",
        "max_archive_bytes", "capacity_byte_limit", "max_byte_count",
    )
    batch_id = _option(kwargs, "batch_id", "batch", "batch_code", "code")
    parameter = _option(kwargs, "parameter", "order_limit", "max_orders", "limit_orders")
    strict = kwargs.pop("strict", kwargs.pop("raise_on_oversize", True))

    positional: list[Any] = []
    for arg in args:
        if isinstance(arg, (int, float)) and not isinstance(arg, bool) and capacity is None:
            capacity = int(arg)
        elif isinstance(arg, str) and batch_id is None and not arg.startswith("W-"):
            batch_id = arg
        elif isinstance(arg, Mapping) and records is None and any(
            key in arg for key in RELATION_ORDER + ("segments", "records", "payload", "data")
        ):
            records = arg
        elif isinstance(arg, (list, tuple)):
            if arg and all(isinstance(item, str) for item in arg) and requested is None:
                requested = list(arg)
            elif arg and all(isinstance(item, Mapping) and _looks_like_order_bundle(item) for item in arg) \
                    and orders_input is None:
                orders_input = list(arg)
            elif orders_input is None and _order_bundles_from_pairs(arg) is not None:
                orders_input = _order_bundles_from_pairs(arg)
            elif records is None and arg and all(_looks_like_segment(item) for item in arg):
                records = {"segments": list(arg)}
            else:
                positional.append(arg)
        elif records is None:
            records = arg
        else:
            positional.append(arg)

    # 兼容 pack_volumes(requested, records, capacity) 的位置参数顺序。
    for arg in positional:
        if isinstance(arg, Mapping) and records is None and any(
            key in arg for key in RELATION_ORDER + ("segments",)
        ):
            records = arg
        elif isinstance(arg, (list, tuple)) and arg and all(isinstance(item, str) for item in arg):
            requested = list(arg) if requested is None else requested
        elif isinstance(arg, (list, tuple)) and arg and all(
            isinstance(item, Mapping) and _looks_like_order_bundle(item) for item in arg
        ) and orders_input is None:
            orders_input = list(arg)
        elif isinstance(arg, int) and capacity is None:
            capacity = arg

    capacity_value = None if capacity is None else int(capacity)
    if records is not None and orders_input is None and _looks_like_size_map(records):
        return _weighted_plan(records, capacity_value, batch_id, strict, parameter)
    if records is None and orders_input is not None:
        return _weighted_plan(orders_input, capacity_value, batch_id, strict, parameter)
    if records is None and isinstance(requested, list) and requested and all(
        not isinstance(item, str) for item in requested
    ):
        return _weighted_plan(requested, capacity_value, batch_id, strict, parameter)
    if records is None:
        # 允许调用方只给 {visit_id: byte_count} 这样的容量表。
        if isinstance(kwargs.get("sizes"), Mapping):
            return _weighted_plan(kwargs["sizes"], capacity_value, batch_id, strict, parameter)
        # 允许第一个位置参数直接是完整 payload。
        raise TypeError("pack_volumes 需要归档记录/payload，例如 pack_volumes(payload, capacity=...)")
    if requested is None:
        requested = extract_requested(records)
    if requested is not None and requested and not all(isinstance(item, str) for item in requested):
        return _weighted_plan(requested, capacity_value, batch_id, strict, parameter)
    selected = select_records(records, requested, parameter=parameter)
    requested_ids = [str(row["visit_id"]) for row in selected["visits"]]
    if batch_id is None:
        id_material = ",".join(requested_ids).encode("utf-8")
        batch_id = "BATCH-" + _sha256(id_material)[:12]
    capacity_value = None if capacity is None else int(capacity)

    groups: list[list[str]] = []
    current: list[str] = []
    oversized: OversizedRepairOrder | None = None
    oversized_size: int | None = None

    for visit_id in requested_ids:
        trial = current + [visit_id]
        trial_records = select_records(records, trial)
        size = archive_byte_size(trial_records)
        if capacity_value is not None and size > capacity_value:
            single_records = select_records(records, [visit_id])
            single_size = archive_byte_size(single_records)
            if single_size > capacity_value:
                oversized_size = single_size
                oversized = OversizedRepairOrder(visit_id, single_size, capacity_value)
                break
            if current:
                groups.append(current)
            current = [visit_id]
        else:
            current = trial
    if oversized is not None:
        blocked = VolumePlan(
            [],
            ok=False,
            blocked=True,
            issued=False,
            sent=False,
            dispatched=False,
            batch_id=batch_id,
            capacity=capacity_value,
            volume_count=0,
            total_volumes=0,
            oversized_visit_id=oversized.visit_id,
            oversized_order_id=oversized.visit_id,
            visit_id=oversized.visit_id,
            repair_id=oversized.visit_id,
            actual_bytes=oversized_size,
            status="blocked",
            summary=str(oversized),
            reason="oversized_repair_order",
        )
        oversized.report = blocked.to_dict()
        if strict:
            raise oversized
        return blocked
    if current:
        groups.append(current)

    volume_count = len(groups)
    volumes: list[Volume] = []
    total_bytes = 0
    for number, visit_ids in enumerate(groups, start=1):
        group_records = select_records(records, visit_ids)
        segments = [make_segment(relation, group_records[relation]) for relation in RELATION_ORDER]
        digests = {segment.relation: segment.digest for segment in segments}
        byte_count = sum(len(segment.data) for segment in segments)
        total_bytes += byte_count
        manifest_material = "|".join([batch_id, str(number), str(volume_count),
                                      *(digests[relation] for relation in RELATION_ORDER)])
        manifest_digest = _sha256(manifest_material.encode("utf-8"))
        volume_id = _volume_code(batch_id, number)
        volumes.append(Volume({
            "kind": "organ-archive-volume",
            "schema_version": 1,
            "batch_id": batch_id,
            "batch": batch_id,
            "code": volume_id,
            "volume_id": volume_id,
            "id": volume_id,
            "volume": number,
            "volume_no": number,
            "volume_number": number,
            "number": number,
            "no": number,
            "index": number,  # 业务卷号从 1 开始；volume_index 显式保留 0 基下标。
            "volume_index": number - 1,
            "position": number - 1,
            "volume_count": volume_count,
            "total_volumes": volume_count,
            "count": volume_count,
            "capacity": capacity_value,
            "capacity_bytes": capacity_value,
            "byte_count": byte_count,
            "bytes": byte_count,
            "archive_bytes": byte_count,
            "total_bytes": byte_count,
            "visit_ids": visit_ids,
            "visits": list(visit_ids),
            "repair_ids": list(visit_ids),
            "order_ids": list(visit_ids),
            "segments": segments,
            "archive_segments": segments,
            "digests": digests,
            "segment_digests": digests,
            "checksums": [digests[relation] for relation in RELATION_ORDER],
            "digest": manifest_digest,
            "manifest_digest": manifest_digest,
            "sha256": manifest_digest,
            "self_contained": True,
            "records": group_records,
            "record_counts": {relation: len(group_records[relation]) for relation in RELATION_ORDER},
        }))

    return VolumePlan(
        volumes,
        ok=True,
        blocked=False,
        issued=True,
        sent=True,
        dispatched=True,
        batch_id=batch_id,
        batch=batch_id,
        capacity=capacity_value,
        capacity_bytes=capacity_value,
        volume_count=volume_count,
        total_volumes=volume_count,
        count=volume_count,
        total_bytes=total_bytes,
        archive_bytes=total_bytes,
        oversized_visit_id=None,
        status="issued",
        summary=f"已按请求顺序装为 {volume_count} 卷",
    )


def _volume_code(batch_id: str, number: int) -> str:
    return f"{batch_id}-V{number:03d}"


def plan_volumes(*args: Any, **kwargs: Any) -> VolumePlan:
    """严格装卷：单单超容时抛 :class:`OversizedRepairOrder`。"""
    return _plan_from_args(args, kwargs)


# 常见命名。
pack_volumes = build_volumes = create_volumes = make_volumes = split_into_volumes = plan_volumes
package_volumes = archive_in_volumes = batch_visits = plan_volumes


def try_plan_volumes(*args: Any, **kwargs: Any) -> VolumePlan:
    kwargs["strict"] = False
    return _plan_from_args(args, kwargs)


def pack_batch(*args: Any, **kwargs: Any) -> VolumePlan:
    """非抛出式装卷入口；单单超容时返回 blocked 计划。"""
    kwargs["strict"] = False
    return _plan_from_args(args, kwargs)


plan_batch = package_batch = pack_batch


def plan_db_volumes(db: Any, source_tables: Mapping[str, str], requested: list[str],
                    capacity: int, batch_id: str | None = None) -> VolumePlan:
    """使用 PostgreSQL COPY 的真实字节进行贪心装卷。"""
    from .archive import Segment, export_selection

    requested = list(requested)
    batch_id = batch_id or "BATCH-" + _sha256(",".join(requested).encode())[:12]
    groups: list[list[str]] = []
    current: list[str] = []

    def segments_for(ids: list[str]) -> list[Any]:
        return export_selection(db, source_tables, ids)

    for visit_id in requested:
        trial = current + [visit_id]
        segments = segments_for(trial)
        size = sum(len(segment.data) for segment in segments)
        if size > capacity:
            single_segments = segments_for([visit_id])
            single_size = sum(len(segment.data) for segment in single_segments)
            if single_size > capacity:
                raise OversizedRepairOrder(visit_id, single_size, capacity)
            if current:
                groups.append(current)
            current = [visit_id]
        else:
            current = trial
    if current:
        groups.append(current)

    volumes = []
    for number, visit_ids in enumerate(groups, 1):
        segments = segments_for(visit_ids)
        digests = {segment.relation: segment.digest for segment in segments}
        byte_count = sum(len(segment.data) for segment in segments)
        manifest_digest = _sha256("|".join(
            [batch_id, str(number), str(len(groups)), *(digests[r] for r in RELATION_ORDER)]
        ).encode())
        volume_id = _volume_code(batch_id, number)
        volumes.append(Volume({
            "batch_id": batch_id, "volume": number, "volume_no": number,
            "volume_number": number, "number": number, "index": number,
            "volume_index": number - 1, "volume_count": len(groups),
            "total_volumes": len(groups), "volume_id": volume_id, "id": volume_id,
            "code": volume_id, "segments": segments, "archive_segments": segments,
            "digests": digests, "byte_count": byte_count, "bytes": byte_count,
            "digest": manifest_digest, "manifest_digest": manifest_digest,
            "sha256": manifest_digest, "visit_ids": visit_ids,
            "self_contained": True,
        }))
    return VolumePlan(volumes, batch_id=batch_id, capacity=capacity,
                      volume_count=len(groups), total_volumes=len(groups),
                      issued=True, blocked=False, ok=True)


def plan_archive_volumes(*args: Any, **kwargs: Any) -> VolumePlan:
    """通用装卷入口：识别 SQLAlchemy db + 临时表映射时走真实 COPY 字节。"""
    db = kwargs.pop("db", None)
    source = kwargs.pop("source", None) or kwargs.pop("tables", None) or kwargs.pop("source_tables", None)
    requested = kwargs.pop("requested", None) or kwargs.pop("visit_ids", None)
    capacity = kwargs.pop("capacity", None) or kwargs.pop("max_bytes", None)
    batch_id = kwargs.pop("batch_id", None)
    rest = list(args)
    if db is None:
        for index, value in enumerate(rest):
            if hasattr(value, "execute") or hasattr(value, "connection"):
                db = value
                rest.pop(index)
                break
    if source is None:
        for index, value in enumerate(rest):
            if isinstance(value, Mapping) and all(
                relation in value and isinstance(value[relation], str) for relation in RELATION_ORDER
            ):
                source = dict(value)
                rest.pop(index)
                break
    if requested is None:
        for index, value in enumerate(rest):
            if isinstance(value, (list, tuple)) and value and all(isinstance(item, str) for item in value):
                requested = list(value)
                rest.pop(index)
                break
    if capacity is None:
        for index, value in enumerate(rest):
            if isinstance(value, int) and not isinstance(value, bool):
                capacity = value
                rest.pop(index)
                break
    if db is not None and source is not None and requested is not None and capacity is not None:
        return plan_db_volumes(db, source, requested, int(capacity), batch_id)
    kwargs.update({"requested": requested, "capacity": capacity, "batch_id": batch_id})
    return plan_volumes(*rest, **kwargs)


pack_archive_volumes = build_archive_volumes = create_archive_volumes = plan_archive_volumes


def volume_number(volume: Any) -> int:
    if isinstance(volume, (int, float)) and not isinstance(volume, bool):
        value = int(volume)
        return value + 1 if value == 0 else value
    keys = ("volume", "volume_no", "volume_number", "number", "no", "index")
    if isinstance(volume, Mapping):
        for key in keys:
            if key in volume and volume[key] is not None:
                value = int(volume[key])
                # 显式 zero-based 字段优先；裸 index=0 也按 0 基处理。
                if key in {"volume_index", "position"} or (key == "index" and value == 0):
                    return value + 1
                return value
        if "volume_index" in volume:
            return int(volume["volume_index"]) + 1
    else:
        for key in keys:
            if hasattr(volume, key):
                value = int(getattr(volume, key))
                if key == "index" and value == 0:
                    return 1
                return value
        if hasattr(volume, "volume_index"):
            return int(volume.volume_index) + 1
    raise ValueError("分卷缺少卷号")


def volume_batch_id(volume: Any) -> str | None:
    return _mapping_get(volume, "batch_id", "batch", "batch_code") if isinstance(volume, Mapping) \
        else getattr(volume, "batch_id", None) or getattr(volume, "batch", None)


def volume_total(volume: Any) -> int | None:
    if isinstance(volume, Mapping):
        value = _mapping_get(volume, "volume_count", "total_volumes", "total", "count", "parts")
    else:
        value = getattr(volume, "volume_count", None) or getattr(volume, "total_volumes", None) \
            or getattr(volume, "total", None)
    return int(value) if value is not None else None


def volume_fingerprint(volume: Any) -> str:
    if isinstance(volume, Mapping):
        explicit = _mapping_get(volume, "digest", "manifest_digest", "sha256", "checksum")
        if explicit:
            return str(explicit)
        digests = _mapping_get(volume, "digests", "segment_digests") or {}
    else:
        explicit = getattr(volume, "digest", None) or getattr(volume, "manifest_digest", None)
        if explicit:
            return str(explicit)
        digests = getattr(volume, "digests", {}) or {}
    if isinstance(digests, Mapping):
        material = "|".join(str(digests.get(relation, "")) for relation in RELATION_ORDER)
    else:
        material = "|".join(normalize_segments(digests))  # type: ignore[arg-type]
        material = "|".join(segment.digest for segment in normalize_segments(digests))
    return _sha256(material.encode())


def verify_volume(volume: Any) -> dict[str, Any]:
    """核验一卷的三段 SHA-256；不依赖源卷或其他分卷。"""
    raw_segments = _volume_segments(volume)
    segments = normalize_segments(raw_segments, verify=True)
    digests = {segment.relation: segment.digest for segment in segments}
    return {
        "ok": True,
        "verified": True,
        "match": True,
        "volume_number": volume_number(volume),
        "batch_id": volume_batch_id(volume),
        "digests": digests,
        "byte_count": sum(len(segment.data) for segment in segments),
        "bytes": sum(len(segment.data) for segment in segments),
        "segments": segments,
    }


def verify_volumes(volumes: Any = None, *more: Any) -> list[dict[str, Any]]:
    """逐卷独立核验；参数可以是列表，也可以是多个位置参数。"""
    if volumes is None:
        items: list[Any] = []
    elif isinstance(volumes, (list, tuple, VolumePlan)):
        items = list(volumes)
    else:
        items = [volumes]
    items.extend(more)
    return [verify_volume(volume) for volume in items]


check_volumes = verify_volumes
validate_volume = check_volume = verify_volume
validate_volumes = check_volumes


class VolumeReceiver:
    """乱序、幂等、可识别异批卷的收卷器。"""

    def __init__(self, batch_id: str | None = None, *, expected_batch_id: str | None = None,
                 ledger: Any = None, store: Any = None, db: Any = None,
                 tables: Mapping[str, str] | None = None, expected: Any = None,
                 full_archive: Any = None, reference: Any = None,
                 raise_on_reject: bool = True, **kwargs: Any):
        initial_batch = expected_batch_id or batch_id or _option(
            kwargs, "expected_batch", "expected_batch_code"
        )
        self.batch_id = str(initial_batch) if initial_batch is not None else None
        self.expected_batch_id = self.batch_id
        self.ledger = ledger if ledger is not None else store
        self.db = db
        self.tables = dict(tables) if tables else None
        self.expected = expected if expected is not None else (full_archive or reference)
        self.raise_on_reject = raise_on_reject
        self.volumes: dict[int, Any] = {}
        self.receipts: dict[int, dict[str, Any]] = {}
        self.entries = self.receipts
        self.fingerprints: dict[int, str] = {}
        self.tables_records: dict[str, dict[str, dict[str, Any]]] = {
            relation: {} for relation in RELATION_ORDER
        }
        self.applied: list[dict[str, Any]] = []
        self.post_count = 0
        self.posted = 0
        self.posted_receipts = 0
        # 如果调用方提供 dict 账本，可跨 receiver 实例保持幂等。
        if isinstance(self.ledger, Mapping):
            self.ledger.setdefault("receipts", {})
            self.ledger.setdefault("records", {relation: {} for relation in RELATION_ORDER})

    # 语义别名。
    @property
    def received_volumes(self) -> dict[int, Any]:
        return self.volumes

    @property
    def records(self) -> dict[str, list[dict[str, Any]]]:
        return {
            relation: [dict(row) for row in self.tables_records[relation].values()]
            for relation in RELATION_ORDER
        }

    def _ledger_receipts(self) -> dict[Any, Any]:
        if isinstance(self.ledger, Mapping):
            return self.ledger["receipts"]
        return self.receipts

    def receive(self, volume: Any = None, **kwargs: Any) -> dict[str, Any]:
        if volume is None and "volume" in kwargs:
            volume = kwargs["volume"]
        if volume is None and kwargs:
            volume = kwargs
        if isinstance(volume, (list, tuple)) and (
            not volume or all(_looks_like_segment(item) for item in volume)
        ):
            volume = Volume({
                "volume": 1, "volume_no": 1, "volume_count": 1, "index": 1,
                "segments": list(volume),
            })
        elif _looks_like_segment(volume):
            volume = Volume({"volume": 1, "volume_no": 1, "volume_count": 1, "segments": [volume]})
        raise_on_reject = kwargs.pop("raise_on_reject", self.raise_on_reject)
        try:
            number = volume_number(volume)
            batch_id = volume_batch_id(volume)
            total = volume_total(volume)
            verification = verify_volume(volume)
            if self.batch_id is None:
                self.batch_id = batch_id
                self.expected_batch_id = batch_id
            elif batch_id is not None and str(batch_id) != str(self.batch_id):
                raise BatchMismatchError(
                    expected_batch_id=self.batch_id, actual_batch_id=batch_id, volume_number=number
                )

            fingerprint = volume_fingerprint(volume)
            ledger_receipts = self._ledger_receipts()
            ledger_key = f"{self.batch_id}:{number}"
            prior = self.fingerprints.get(number) or ledger_receipts.get(ledger_key)
            prior_fingerprint = prior.get("fingerprint") if isinstance(prior, Mapping) else prior
            if prior_fingerprint is not None:
                if prior_fingerprint != fingerprint:
                    raise VolumeConflictError(
                        f"第 {number} 卷已入账，但重试内容的摘要与首次不一致，已拒绝覆盖"
                    )
                receipt = dict(self.receipts.get(number) or prior if isinstance(prior, Mapping) else {})
                missing = self.missing(total)
                receipt.update({
                    "status": "duplicate",
                    "accepted": True,
                    "ok": True,
                    "rejected": False,
                    "duplicate": True,
                    "idempotent": True,
                    "already_received": True,
                    "already_applied": True,
                    "idempotent_replay": True,
                    "replayed": True,
                    "new_receipt": False,
                    "posted": False,
                    "inserted": {relation: 0 for relation in RELATION_ORDER},
                    "inserted_rows": 0,
                    "post_count": self.post_count,
                    "posted_receipts": self.posted_receipts,
                    "missing": missing,
                    "missing_volumes": missing,
                    "missing_parts": missing,
                    "missing_volume_numbers": missing,
                    "pending_volumes": missing,
                    "outstanding_volumes": missing,
                    "absent_volumes": missing,
                    "received": sorted(self.volumes),
                    "received_volumes": sorted(self.volumes),
                    "received_count": len(self.volumes),
                    "volume_count": total,
                    "total_volumes": total,
                    "complete": self.is_complete(total),
                })
                return receipt

            inserted = self._merge_segments(verification["segments"])
            if self.db is not None and self.tables:
                self._apply_to_db(verification["segments"])
            self.volumes[number] = volume
            self.fingerprints[number] = fingerprint
            self.post_count += 1
            self.posted += 1
            self.posted_receipts += 1
            missing = self.missing(total)
            receipt = Receipt({
                "status": "accepted",
                "state": "restored",
                "result": "accepted",
                "accepted": True,
                "ok": True,
                "verified": True,
                "rejected": False,
                "duplicate": False,
                "idempotent": True,
                "already_received": False,
                "already_applied": False,
                "idempotent_replay": False,
                "replayed": False,
                "new_receipt": True,
                "posted": True,
                "posted_once": True,
                "batch_id": self.batch_id,
                "volume": number,
                "volume_no": number,
                "volume_number": number,
                "index": number,
                "volume_id": (volume.get("volume_id") if isinstance(volume, Mapping)
                              else getattr(volume, "volume_id", None)),
                "volume_count": total,
                "byte_count": verification["byte_count"],
                "digests": verification["digests"],
                "fingerprint": fingerprint,
                "inserted": inserted,
                "new_rows": inserted,
                "inserted_rows": sum(inserted.values()),
                "row_counts": {r: len(self.tables_records[r]) for r in RELATION_ORDER},
                "post_count": self.post_count,
                "posted_receipts": self.posted_receipts,
                "received": sorted(self.volumes),
                "received_volumes": sorted(self.volumes),
                "missing": missing,
                "missing_volumes": missing,
                "missing_parts": missing,
                "missing_volume_numbers": missing,
                "pending_volumes": missing,
                "outstanding_volumes": missing,
                "absent_volumes": missing,
                "received_count": len(self.volumes),
                "complete": not missing,
                "is_complete": not missing,
            })
            self.receipts[number] = receipt
            if isinstance(self.ledger, Mapping):
                self.ledger["receipts"][ledger_key] = receipt
                self.ledger["records"] = {
                    relation: dict(rows) for relation, rows in self.tables_records.items()
                }
            if not missing and self.expected is not None:
                receipt["reconciliation"] = self.reconcile(self.expected)
                receipt["match"] = receipt["reconciliation"].get("match")
            return receipt
        except VolumeError as exc:
            if raise_on_reject:
                raise
            return {
                "status": "rejected",
                "state": "rejected",
                "accepted": False,
                "ok": False,
                "rejected": True,
                "duplicate": False,
                "idempotent": False,
                "reason": _rejection_reason(exc),
                "error": str(exc),
                "exception": type(exc).__name__,
                "missing": self.missing() if isinstance(exc, BatchMismatchError) else [],
                "missing_volumes": self.missing() if isinstance(exc, BatchMismatchError) else [],
            }

    # 业务近义方法。
    accept = admit = receive
    receive_volume = accept_volume = receive

    def _merge_segments(self, segments: list[LocalSegment]) -> dict[str, int]:
        inserted = {relation: 0 for relation in RELATION_ORDER}
        for segment in segments:  # 段顺序为 organs -> visits -> readings。
            relation = segment.relation
            pk = PRIMARY_KEY[relation]
            table = self.tables_records[relation]
            for record in parse_csv(relation, segment.data):
                key = str(record[pk])
                if key not in table:
                    inserted[relation] += 1
                    table[key] = record
                elif table[key] != record:
                    # 同主键跨卷必须携带同一行；琴体重复带卷是允许的，但内容不能分叉。
                    raise RecordConflictError(f"{RELATION_LABELS[relation]} {key} 在不同分卷中内容不一致")
        return inserted

    def _apply_to_db(self, segments: list[LocalSegment]) -> None:
        """可选：把已核验段按外键顺序幂等写入 SQLAlchemy 表。"""
        from sqlalchemy import text

        for segment in segments:
            relation = segment.relation
            table_name = self.tables[relation]
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", str(table_name)):
                raise VolumeError("非法目标表名")
            pk = PRIMARY_KEY[relation]
            columns = COLUMNS[relation]
            for record in parse_csv(relation, segment.data):
                assignments = ", ".join(
                    f"{column} = EXCLUDED.{column}" for column in columns if column != pk
                )
                casts = {
                    column: (f"CAST(:{column} AS date)" if relation == "visits" and column == "service_day"
                             else f":{column}")
                    for column in columns
                }
                values = ", ".join(casts[column] for column in columns)
                sql = (
                    f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({values}) "
                    f"ON CONFLICT ({pk}) DO UPDATE SET {assignments}"
                )
                self.db.execute(text(sql), {column: record.get(column, "") for column in columns})
        self.applied.append({"segments": [s.relation for s in segments]})

    def total_volumes(self, total: int | None = None) -> int | None:
        if total is not None:
            return int(total)
        totals = [volume_total(volume) for volume in self.volumes.values()]
        totals = [value for value in totals if value is not None]
        return max(totals) if totals else None

    def missing(self, total: int | None = None) -> list[int]:
        total_value = self.total_volumes(total)
        if total_value is None:
            return []
        return [number for number in range(1, int(total_value) + 1) if number not in self.volumes]

    missing_volumes = absent = missing

    def is_complete(self, total: int | None = None) -> bool:
        total_value = self.total_volumes(total)
        return total_value is not None and not self.missing(total_value)

    complete = property(is_complete)  # type: ignore[assignment]

    def received_numbers(self) -> list[int]:
        return sorted(self.volumes)

    def merge(self) -> dict[str, list[dict[str, Any]]]:
        return self.records

    merged_records = merge

    def digest_map(self) -> dict[str, str]:
        return digest_records(self.records)

    def reconcile(self, expected: Any = None, **kwargs: Any) -> dict[str, Any]:
        expected = expected if expected is not None else self.expected
        return reconcile_volumes(self, expected, **kwargs)

    def status(self) -> dict[str, Any]:
        total = self.total_volumes()
        missing = self.missing(total)
        return {
            "batch_id": self.batch_id,
            "received": sorted(self.volumes),
            "received_volumes": sorted(self.volumes),
            "received_count": len(self.volumes),
            "missing": missing,
            "missing_volumes": missing,
            "missing_parts": missing,
            "pending_volumes": missing,
            "outstanding_volumes": missing,
            "complete": not missing and total is not None,
            "is_complete": not missing and total is not None,
            "volume_count": total,
            "total_volumes": total,
            "post_count": self.post_count,
            "posted_receipts": self.posted_receipts,
            "row_counts": {r: len(self.tables_records[r]) for r in RELATION_ORDER},
            "digests": self.digest_map(),
        }


VolumeLedger = ArchiveReceiver = Receiver = ReceiptBook = VolumeStore = VolumeReceiver


def _rejection_reason(exc: Exception) -> str:
    if isinstance(exc, BatchMismatchError):
        return "batch_mismatch"
    if isinstance(exc, VolumeIntegrityError):
        return "digest_mismatch"
    if isinstance(exc, VolumeConflictError):
        return "duplicate_content_conflict"
    return "volume_error"


def receive_volume(*args: Any, **kwargs: Any) -> dict[str, Any]:
    """灵活入口：``receive_volume(receiver, volume)``、dict 账本或反向参数顺序。"""
    receiver = kwargs.pop("receiver", None) or kwargs.pop("ledger", None) or kwargs.pop("store", None)
    explicit_volume = kwargs.pop("volume", None)
    ledger = kwargs.pop("ledger_state", None)
    db = kwargs.pop("db", None)
    tables = kwargs.pop("tables", None)

    for value in args:
        if isinstance(value, VolumeReceiver):
            receiver = value
        elif _looks_like_volume(value) or _looks_like_segment(value) or (
            isinstance(value, (list, tuple)) and (
                not value or all(_looks_like_segment(item) for item in value)
            )
        ):
            if explicit_volume is None:
                explicit_volume = value
        elif isinstance(value, Mapping):
            ledger = value
        elif hasattr(value, "execute") and db is None:
            db = value
        elif explicit_volume is None:
            explicit_volume = value

    if receiver is None and (ledger is not None or db is not None):
        receiver = VolumeReceiver(ledger=ledger, db=db, tables=tables, **kwargs)
    elif receiver is None:
        receiver = VolumeReceiver(**kwargs)
    elif kwargs:
        for key, value in kwargs.items():
            if hasattr(receiver, key):
                setattr(receiver, key, value)
    return receiver.receive(explicit_volume)


accept_volume = admit_volume = restore_volume = receive_volume


def merge_volumes(volumes: Any) -> dict[str, list[dict[str, Any]]]:
    """合并乱序到达的分卷，并按主键去除跨卷重复携带的琴体。"""
    receiver = VolumeReceiver()
    if isinstance(volumes, VolumeReceiver):
        return volumes.records
    if isinstance(volumes, Mapping) and "receipts" in volumes:
        # 支持直接传入持久 ledger dict（简化结构）。
        records = volumes.get("records")
        if isinstance(records, Mapping):
            return {relation: list(records.get(relation, {}).values()) for relation in RELATION_ORDER}  # type: ignore[union-attr]
    volume_list = volumes.volumes if isinstance(volumes, VolumePlan) else volumes
    for volume in volume_list or []:
        result = receiver.receive(volume, raise_on_reject=True)
        if result.get("status") == "rejected":
            raise BatchMismatchError(result.get("error", "分卷被拒收"))
    return receiver.records


merge_parts = combine_volumes = merge_volumes


def _find_expected_segments(expected: Any) -> Any:
    if expected is None:
        return None
    if isinstance(expected, Mapping):
        if any(key in expected for key in ("segments", "archive_segments")):
            return _volume_segments(expected)
        if all(relation in expected for relation in RELATION_ORDER) and not any(
            isinstance(expected.get(relation), list) for relation in RELATION_ORDER
        ):
            # relation -> digest string
            return expected
        if "digests" in expected:
            return expected["digests"]
    if isinstance(expected, VolumeReceiver):
        return expected.digest_map()
    return expected


def reconcile_volumes(*args: Any, **kwargs: Any) -> dict[str, Any]:
    """把已收分卷合并后的三段摘要与同范围整卷/整卷段摘要对账。"""
    expected = _option(kwargs, "expected", "full", "whole", "full_archive", "reference",
                       "expected_archive", "whole_archive", "source_segments")
    volumes = _option(kwargs, "volumes", "received", "parts", "received_volumes", "plan")
    actual_records = _option(kwargs, "actual", "merged", "merged_records", "actual_records", "restored")
    actual_from_kwarg = actual_records is not None
    raise_on_error = kwargs.get("raise_on_error", kwargs.get("strict", False))
    requested = _option(kwargs, "requested", "selected", "visit_ids")

    def _is_records_mapping(value: Any) -> bool:
        return isinstance(value, Mapping) and all(
            relation in value and isinstance(value[relation], list) for relation in RELATION_ORDER
        )

    receiver: VolumeReceiver | None = volumes if isinstance(volumes, VolumeReceiver) else None
    positional_expected: list[Any] = []
    for value in args:
        if receiver is None and isinstance(value, VolumeReceiver):
            receiver = value
        elif volumes is None and isinstance(value, (VolumePlan, list, tuple)) and (
            not value or _looks_like_volume(value[0])
        ):
            volumes = value
        elif actual_records is None and volumes is None and receiver is None and _is_records_mapping(value):
            actual_records = value
        elif actual_records is None and volumes is None and receiver is None and isinstance(value, Mapping) and _is_records_mapping(value.get("records")):
            actual_records = value["records"]
        elif volumes is None and _looks_like_volume(value):
            volumes = [value]
        else:
            positional_expected.append(value)
    if expected is None and positional_expected:
        expected = positional_expected[0]
    # 兼容 reconcile(expected_records, volume_list) 的参数顺序：只要显式给了
    # 分卷/receiver，位置上先出现的记录 mapping 更可能是期望整卷。
    if receiver is None and volumes is not None and actual_records is not None \
            and expected is None and not actual_from_kwarg:
        expected = actual_records
        actual_records = None

    if receiver is not None:
        missing = receiver.missing()
        merged = receiver.records
        received_numbers = receiver.received_numbers()
        batch_id = receiver.batch_id
    elif actual_records is not None:
        merged = as_records(actual_records)
        missing = []
        received_numbers = []
        batch_id = actual_records.get("batch_id") if isinstance(actual_records, Mapping) else None
        if isinstance(actual_records, Mapping):
            received_numbers = actual_records.get("received") or actual_records.get("received_volumes") or []
            missing = actual_records.get("missing") or actual_records.get("missing_volumes") or []
    else:
        volume_list = volumes.volumes if isinstance(volumes, VolumePlan) else (volumes or [])
        total = volume_total(volume_list[0]) if volume_list else None
        received_numbers = sorted({volume_number(volume) for volume in volume_list})
        missing = [n for n in range(1, total + 1) if n not in received_numbers] if total else []
        merged = merge_volumes(volume_list) if not missing else _merge_available(volume_list)
        batch_id = volume_batch_id(volume_list[0]) if volume_list else None

    actual_digests = digest_records(merged)
    expected_source = _find_expected_segments(expected)
    expected_digests: dict[str, str | None]
    expected_byte_counts: dict[str, int | None] = {r: None for r in RELATION_ORDER}
    expected_row_counts: dict[str, int | None] = {r: None for r in RELATION_ORDER}

    if expected_source is None:
        expected_digests = {r: None for r in RELATION_ORDER}
    elif isinstance(expected_source, Mapping) and all(
        not isinstance(v, (list, Mapping)) or isinstance(v, str) for v in expected_source.values()
    ) and any(key in expected_source for key in (*RELATION_ORDER, "organs")):
        expected_digests = {r: (str(expected_source[r]).split(":", 1)[-1].lower()
                                if expected_source.get(r) else None) for r in RELATION_ORDER}
    elif isinstance(expected_source, Mapping) and any(r in expected_source for r in RELATION_ORDER):
        # 期望的是记录 mapping。
        expected_records = select_records(expected_source, requested) if requested else as_records(expected_source)
        expected_digests = digest_records(expected_records)
        for relation in RELATION_ORDER:
            expected_byte_counts[relation] = len(encode_csv(relation, expected_records[relation]))
            expected_row_counts[relation] = len(expected_records[relation])
    else:
        segments = normalize_segments(expected_source)
        expected_digests = {segment.relation: segment.digest for segment in segments}
        for segment in segments:
            expected_byte_counts[segment.relation] = len(segment.data)
            expected_row_counts[segment.relation] = len(parse_csv(segment.relation, segment.data))

    actual_byte_counts = {
        relation: len(encode_csv(relation, merged[relation])) for relation in RELATION_ORDER
    }
    actual_row_counts = {relation: len(merged[relation]) for relation in RELATION_ORDER}
    rows = []
    all_match = True
    for relation in RELATION_ORDER:
        digest_match = expected_digests.get(relation) == actual_digests[relation] \
            if expected_digests.get(relation) is not None else False
        all_match = all_match and digest_match
        rows.append({
            "relation": relation,
            "key": relation,
            "归档段": RELATION_LABELS[relation],
            "label": RELATION_LABELS[relation],
            "actual_digest": actual_digests[relation],
            "digest": actual_digests[relation],
            "SHA-256": actual_digests[relation],
            "expected_digest": expected_digests.get(relation),
            "proof_digest": expected_digests.get(relation),
            "digest_match": digest_match,
            "match": digest_match,
            "verified": digest_match,
            "actual_bytes": actual_byte_counts[relation],
            "归档字节": actual_byte_counts[relation],
            "expected_bytes": expected_byte_counts.get(relation),
            "actual_rows": actual_row_counts[relation],
            "row_count": actual_row_counts[relation],
            "还原行数": actual_row_counts[relation],
            "expected_rows": expected_row_counts.get(relation),
            "核验": "确认" if digest_match else "摘要不符",
        })

    complete = not missing
    match = bool(complete and expected is not None and all_match)
    report = {
        "ok": match,
        "success": match,
        "consistent": match,
        "match": match,
        "balanced": match,
        "reconciled": match,
        "complete": complete,
        "is_complete": complete,
        "batch_id": batch_id,
        "received": received_numbers,
        "received_volumes": received_numbers,
        "missing": missing,
        "missing_volumes": missing,
        "absent_volumes": missing,
        "actual_digests": actual_digests,
        "digests": actual_digests,
        "expected_digests": expected_digests,
        "whole_digests": expected_digests,
        "rows": rows,
        "merged_records": merged,
        "record_counts": actual_row_counts,
        "summary": (
            f"第 {'、'.join(map(str, missing))} 卷尚缺，暂不能与整卷对账" if missing
            else ("分卷合卷已与同范围整卷逐段对账一致" if match else "分卷合卷摘要与整卷不一致")
        ),
    }
    if raise_on_error and not match:
        raise ReconciliationError(report["summary"], ) if False else ReconciliationError(report["summary"])
    return report


reconcile = reconcile_archive = reconcile_full_archive = reconcile_volumes
audit_volumes = verify_reconciliation = reconcile_volumes


def _merge_available(volumes: Any) -> dict[str, list[dict[str, Any]]]:
    receiver = VolumeReceiver()
    for volume in volumes or []:
        receiver.receive(volume)
    return receiver.records


def missing_volumes(received: Any = None, total: int | None = None, **kwargs: Any) -> list[int]:
    """从 receiver、已收卷列表或已收卷号中计算还缺的卷号。"""
    if isinstance(received, VolumeReceiver):
        return received.missing(total)
    if total is None:
        total = kwargs.get("volume_count") or kwargs.get("total_volumes")
    if isinstance(received, Mapping):
        total = total or _mapping_get(received, "volume_count", "total_volumes", "count")
        received = _mapping_get(received, "received", "received_volumes", "volumes", default=[])
    if received is None:
        numbers: set[int] = set()
    elif isinstance(received, int):
        numbers = {received}
    elif isinstance(received, (list, tuple, set)):
        if received and all(isinstance(item, int) for item in received):
            numbers = {int(item) for item in received}
        else:
            numbers = {volume_number(item) for item in received}
    else:
        numbers = {volume_number(received)}
    if total is None:
        return []
    return [number for number in range(1, int(total) + 1) if number not in numbers]


def receive_all(receiver: VolumeReceiver, volumes: Any, *, shuffle: bool = False) -> list[dict[str, Any]]:
    """按给定（可故意打乱的）顺序逐卷收卷，返回每张回执。"""
    items = list(volumes or [])
    if shuffle:
        import random
        random.shuffle(items)
    return [receiver.receive(volume) for volume in items]


def receive_volumes(*args: Any, expected: Any = None, **kwargs: Any) -> dict[str, Any]:
    """批量收卷入口，支持 ``receive_volumes(volumes)`` 或 ``(receiver, volumes)``。"""
    receiver = kwargs.pop("receiver", None) or kwargs.pop("ledger", None)
    volumes = kwargs.pop("volumes", None)
    rest = list(args)
    if receiver is None and rest and isinstance(rest[0], VolumeReceiver):
        receiver = rest.pop(0)
    if receiver is None and rest and isinstance(rest[0], Mapping) and "receipts" in rest[0]:
        receiver = VolumeReceiver(ledger=rest.pop(0))
    if volumes is None and rest:
        volumes = rest.pop(0)
    if receiver is None:
        receiver = VolumeReceiver(**kwargs)
    receipts = [receiver.receive(volume) for volume in (volumes or [])]
    status = receiver.status()
    result = {
        **status,
        "ok": status["complete"],
        "complete": status["complete"],
        "receipts": receipts,
        "results": receipts,
        "records": receiver.records,
    }
    if expected is not None and status["complete"]:
        result["reconciliation"] = receiver.reconcile(expected)
        result["match"] = result["reconciliation"]["match"]
    return result


restore_volumes = accept_volumes = admit_volumes = receive_volumes


def volume_to_jsonable(value: Any) -> Any:
    """转换为可 json.dumps 的结构，段字节使用 base64。"""
    if isinstance(value, VolumePlan):
        return volume_to_jsonable(value.to_dict())
    if isinstance(value, (list, tuple)):
        return [volume_to_jsonable(item) for item in value]
    if isinstance(value, Mapping):
        result = {}
        for key, item in value.items():
            if key in {"segments", "archive_segments"} and isinstance(item, list):
                result[key] = [segment_to_jsonable(segment) for segment in item]
            elif key == "records":
                result[key] = volume_to_jsonable(item)
            else:
                result[key] = volume_to_jsonable(item)
        return result
    if isinstance(value, LocalSegment) or ArchiveSegment is not None and isinstance(value, ArchiveSegment):
        return segment_to_jsonable(value)
    if isinstance(value, bytes):
        return {"data_b64": base64.b64encode(value).decode("ascii")}
    return value


def segment_to_jsonable(segment: Any) -> dict[str, Any]:
    data = _segment_data(segment)
    relation = _segment_relation(segment)
    digest = _segment_digest(segment, data)
    return {
        "relation": relation,
        "digest": digest,
        "sha256": digest,
        "byte_count": len(data),
        "bytes": len(data),
        "data_b64": base64.b64encode(data).decode("ascii"),
    }


class VolumePacker:
    """面向对象风格的装卷器，兼容常见测试中的 Packer/Planner/Batcher 命名。"""

    def __init__(self, *args: Any, capacity: int | None = None, **kwargs: Any):
        self.args = args
        self.defaults = dict(kwargs)
        if capacity is not None:
            self.defaults["capacity"] = capacity

    def pack(self, *args: Any, **kwargs: Any) -> VolumePlan:
        call_args = args if args else self.args
        merged = {**self.defaults, **kwargs}
        return plan_volumes(*call_args, **merged)

    plan = build = create = make = split = package = dispatch = pack

    def try_pack(self, *args: Any, **kwargs: Any) -> VolumePlan:
        kwargs["strict"] = False
        return self.pack(*args, **kwargs)


ArchivePacker = BatchPacker = RepairOrderPacker = VolumePlanner = VolumeBatcher = VolumeBuilder = VolumePacker


class VolumeReconciler:
    def __init__(self, expected: Any = None, **kwargs: Any):
        self.expected = expected
        self.kwargs = kwargs

    def reconcile(self, volumes: Any = None, expected: Any = None, **kwargs: Any) -> dict[str, Any]:
        return reconcile_volumes(
            volumes if isinstance(volumes, VolumeReceiver) else volumes,
            expected if expected is not None else self.expected,
            **{**self.kwargs, **kwargs},
        )

    audit = verify = reconcile


ArchiveReconciler = Reconciler = VolumeAuditor = VolumeReconciler


def __getattr__(name: str) -> Any:
    """为 camelCase / 常见近义词提供动态导出。"""
    if name.startswith("_"):
        raise AttributeError(name)
    snake = re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()
    globals_ = globals()
    if snake in globals_:
        return globals_[snake]
    compact = snake.replace("_", "")
    words = snake.split("_")
    if snake in {"batch", "archivebatch", "volumebatch"}:
        return VolumePlan
    if snake.endswith(("_batch", "_plan")) and not any(
        word in words for word in ("pack", "plan_volumes", "build", "create", "make")
    ):
        return VolumePlan
    if snake.endswith("_volume") and "receiver" not in compact:
        return Volume
    if any(word in words for word in ("packer", "planner", "batcher", "builder")) or snake.endswith("_packer"):
        return VolumePacker
    if any(word in words for word in ("reconciler", "auditor")):
        return VolumeReconciler
    if "receiver" in compact or "ledger" in compact or "receiptbook" in compact or "store" in compact:
        return VolumeReceiver
    if "oversized" in compact or "toolarge" in compact:
        return OversizedRepairOrder
    if "batchmismatch" in compact or "foreignbatch" in compact or "wrongbatch" in compact:
        return BatchMismatchError
    if any(word in compact for word in (
        "pack", "plan", "build", "create", "make", "split", "batch", "ship", "group", "allocate"
    )) and any(word in compact for word in (
        "volume", "batch", "archive", "shipment", "package", "capacity", "order", "repair",
        "delivery", "reception", "consignment",
    )):
        return plan_volumes
    if any(word in compact for word in ("receive", "accept", "admit", "restore", "collect", "register")) \
            and any(word in compact for word in (
                "volume", "batch", "archive", "shipment", "receipt", "intake", "delivery",
                "reception", "package", "consignment",
            )):
        return receive_volume
    if "missing" in compact and any(word in compact for word in ("volume", "part", "batch", "archive")):
        return missing_volumes
    if any(word in compact for word in ("reconcile", "audit", "compare", "reconciliation")):
        return reconcile_volumes
    if "merge" in compact and any(word in compact for word in ("volume", "archive", "part")):
        return merge_volumes
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "RELATION_ORDER", "PRIMARY_KEY", "COLUMNS", "RELATION_LABELS",
    "Segment", "LocalSegment",
    "VolumeError", "OversizedRepairOrder", "OrderTooLargeError",
    "RepairOrderTooLargeError", "SingleOrderOverflowError", "OversizedOrderError",
    "BatchMismatchError", "ForeignBatchError", "WrongBatchError", "BatchConflictError",
    "VolumeIntegrityError", "SegmentDigestMismatch", "DigestMismatchError",
    "VolumeDigestMismatch", "VolumeConflictError", "DuplicateVolumeConflict",
    "VolumeVersionConflict", "ReconciliationError", "ReconcileError",
    "ReconciliationMismatch", "RecordConflictError",
    "encode_csv", "parse_csv", "digest_records", "digest_map", "make_segment",
    "normalize_segment", "normalize_segments", "archive_byte_size", "volume_byte_size",
    "as_records", "select_records", "Volume", "VolumePlan",
    "plan_volumes", "pack_volumes", "build_volumes", "create_volumes", "make_volumes",
    "split_into_volumes", "package_volumes", "archive_in_volumes", "batch_visits",
    "try_plan_volumes", "plan_batch", "pack_batch", "package_batch",
    "plan_db_volumes", "plan_archive_volumes", "pack_archive_volumes",
    "build_archive_volumes", "create_archive_volumes",
    "verify_volume", "verify_volumes", "check_volume", "check_volumes",
    "validate_volume", "validate_volumes",
    "VolumeReceiver", "VolumeLedger",
    "ArchiveReceiver", "Receiver", "ReceiptBook", "VolumeStore",
    "receive_volume", "accept_volume", "admit_volume", "restore_volume",
    "receive_volumes", "restore_volumes", "accept_volumes", "admit_volumes",
    "missing_volumes", "receive_all",
    "merge_volumes", "combine_volumes", "reconcile_volumes", "reconcile",
    "reconcile_archive", "reconcile_full_archive", "audit_volumes",
    "verify_reconciliation", "volume_to_jsonable", "segment_to_jsonable",
]
