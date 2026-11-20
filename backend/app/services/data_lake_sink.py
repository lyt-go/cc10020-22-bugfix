"""数据落湖领域服务：数据写入本地存储、分区组织与生命周期管理。

将接入的数据按日期/来源/类型分区写入本地JSON文件，
支持多种写入格式和压缩策略。

同时维护「分区写入确认明细」（write receipt）：每次批量写入都会生成一条
可持久化、可幂等重放的写入凭证，记录分区、文件、条数与内容摘要。
落湖写入和回补任务共用同一份确认记录，因此：
- 同一秒内到达的多个批次各自落成独立文件，不再互相覆盖；
- 同一批次重复提交只保留一次有效写入；
- 写入成功但未收到确认的批次，可以沿回执查到实际文件并补确认。

跨模块关联：webhook_channel（事件来源）、batch_ingest（批量来源）、backfill_job（回补确认）、event_archive（归档接口）。
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import threading
import uuid
from datetime import datetime, timezone
from typing import Any

from app.persistence import json_store
from app.services.base import BaseModuleService

# 分区写入确认明细的持久化分区。
RECEIPT_MODULE = "data_lake_receipt"
STATUS_PENDING = "待确认"
STATUS_CONFIRMED = "已确认"

# 同进程内多个 DataLakeSinkService 实例可能并发写入同一个分区，
# 文件命名、回执判重和落盘必须在同一把锁内完成。
_WRITE_LOCK = threading.RLock()


class DataLakeSinkService(BaseModuleService):
    MODULE = "data_lake_sink"
    ENTITY = "落湖配置"
    OPERATION_TERM = "入湖"
    LIST_FIELDS = ["配置编号", "数据来源", "目标路径", "分区策略", "写入格式", "压缩算法", "保留天数", "落湖水位"]
    REQUIRED_FIELDS = ["配置编号", "数据来源", "目标路径"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用配置": "已停用"}
    NEGATIVE_ACTIONS = ["停用配置"]

    # 允许测试或部署时覆盖数据湖根目录；一旦设置，所有实例都指向该目录。
    LAKE_BASE: str | None = None

    def __init__(self, lake_base: str | None = None) -> None:
        super().__init__()
        if lake_base is None:
            lake_base = self.LAKE_BASE
        if lake_base is None:
            lake_base = os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
                "data_store", "lake",
            )
        self._lake_base = lake_base
        os.makedirs(self._lake_base, exist_ok=True)
        self._write_stats: dict[str, int] = {}

    # ──────────────────────────────────────────────────────────
    # 分区写入确认明细（回执）
    # ──────────────────────────────────────────────────────────
    @staticmethod
    def _records_digest(records: list[dict[str, Any]]) -> str:
        """计算一批写入内容的稳定摘要，用于识别重试是否同一批数据。"""
        payload = json.dumps(records, sort_keys=True, ensure_ascii=False, default=str)
        return hashlib.sha256(payload.encode()).hexdigest()[:32]

    @staticmethod
    def _normalize_partition_date(partition_date: str | None, now: datetime) -> str:
        """把日期统一成数据湖使用的 YYYY/MM/DD 分区格式。"""
        if not partition_date:
            return now.strftime("%Y/%m/%d")
        value = str(partition_date).strip()
        if "T" in value:
            value = value.split("T")[0]
        value = value.replace("-", "/")
        parts = [p for p in value.split("/") if p]
        if len(parts) == 3:
            return "/".join(parts)
        return now.strftime("%Y/%m/%d")

    def _partition_abs(self, partition: str) -> str:
        return os.path.join(self._lake_base, *partition.split("/"))

    @staticmethod
    def _write_json_atomic(file_path: str, data: Any) -> None:
        """先写临时文件再原子替换，避免并发/中断产生半截文件。"""
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        tmp_path = f"{file_path}.{uuid.uuid4().hex}.tmp"
        try:
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, default=str)
            os.replace(tmp_path, file_path)
        finally:
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass

    @staticmethod
    def _read_json_file(file_path: str) -> Any | None:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (OSError, json.JSONDecodeError):
            return None

    @staticmethod
    def _extract_payloads(raw: Any) -> list[dict[str, Any]]:
        """从落湖文件中还原调用方写入的原始数据。

        新格式是带 event_id/payload 的信封列表；历史文件可能是裸记录列表，
        两种都要能沿回执查回实际数据。
        """
        if isinstance(raw, list):
            if raw and all(isinstance(item, dict) and "payload" in item and "event_id" in item
                           for item in raw):
                return [item.get("payload", {}) for item in raw]
            return raw
        if isinstance(raw, dict):
            if "payload" in raw:
                return [raw.get("payload", {})]
            return [raw]
        return []

    @staticmethod
    def _legacy_receipt_id(seed: str) -> str:
        return "LGY-" + hashlib.sha1(seed.encode()).hexdigest()[:16]

    @staticmethod
    def _batch_token(batch_key: str) -> str:
        """批次键的稳定短摘要：同时用于文件名和回执号，回执丢失后可据此重建。"""
        return hashlib.sha256(batch_key.encode()).hexdigest()[:12]

    @classmethod
    def _batch_receipt_id(cls, batch_key: str) -> str:
        return f"LKW-{cls._batch_token(batch_key)}"

    def _normalize_receipt(self, raw: dict[str, Any]) -> dict[str, Any]:
        """把新、旧两种回执结构统一成同一份确认明细。"""
        receipt = dict(raw)
        file_path = str(receipt.get("file_path") or receipt.get("filePath") or "")
        if not file_path and receipt.get("partition") and receipt.get("file_name"):
            file_path = os.path.join(
                self._partition_abs(str(receipt["partition"])),
                str(receipt["file_name"]),
            )
        partition = str(receipt.get("partition") or "")
        if not partition and file_path:
            partition = os.path.dirname(os.path.relpath(file_path, self._lake_base)).replace(os.sep, "/")

        receipt_id = str(receipt.get("receipt_id") or receipt.get("receiptId") or "")
        legacy = bool(receipt.get("legacy"))
        if not receipt_id:
            receipt_id = self._legacy_receipt_id(file_path or json.dumps(receipt, sort_keys=True, default=str))
            legacy = True

        if "confirmed" in receipt:
            confirmed = bool(receipt.get("confirmed"))
        else:
            status = str(receipt.get("status", ""))
            confirmed = status in (STATUS_CONFIRMED, "成功", "已写入", "success") or bool(receipt.get("ok"))
        status = str(receipt.get("status") or (STATUS_CONFIRMED if confirmed else STATUS_PENDING))
        # 历史回执没有确认状态，只能按“已成功写入”处理。
        if legacy and not raw.get("confirmed"):
            confirmed = True
            status = STATUS_CONFIRMED

        written = int(receipt.get("written") or receipt.get("record_count") or 0)

        partition_date = receipt.get("partition_date") or receipt.get("partitionDate")
        if not partition_date and partition:
            partition_date = "/".join(partition.split("/")[2:])

        event_type = receipt.get("event_type") or ""
        if not event_type and partition.startswith("events/"):
            event_type = partition.split("/")[1]

        receipt.update({
            "receipt_id": receipt_id,
            "batch_key": receipt.get("batch_key") or receipt.get("batchKey"),
            "event_type": event_type,
            "partition": partition,
            "partition_date": partition_date or "",
            "file_path": file_path,
            "file_name": receipt.get("file_name") or (os.path.basename(file_path) if file_path else ""),
            "written": written,
            "record_count": int(receipt.get("record_count") or written),
            "content_digest": receipt.get("content_digest") or receipt.get("digest") or "",
            "source_channel_id": int(receipt.get("source_channel_id") or 0),
            "status": status,
            "confirmed": confirmed,
            "attempts": int(receipt.get("attempts") or 1),
            "legacy": legacy,
            "replayed": bool(receipt.get("replayed")),
        })
        return receipt

    def _find_raw_receipt(self, receipt_id: str) -> dict[str, Any] | None:
        for raw in json_store.get_all(RECEIPT_MODULE):
            if str(raw.get("receipt_id") or "") == receipt_id:
                return raw
        # 兼容旧凭证：旧结构可能没有 receipt_id，用归一化后的派生 ID 匹配。
        for raw in json_store.get_all(RECEIPT_MODULE):
            if self._normalize_receipt(raw).get("receipt_id") == receipt_id:
                return raw
        return None

    def _all_lake_files(self) -> list[str]:
        files: list[str] = []
        for root, _dirs, names in os.walk(self._lake_base):
            for name in names:
                if name.endswith(".json") and not name.endswith(".tmp"):
                    files.append(os.path.join(root, name))
        return files

    def _synthetic_receipt_for_file(self, file_path: str, batch_key: str | None = None,
                                    confirmed: bool | None = None) -> dict[str, Any]:
        """为没有持久化回执的历史分区文件生成可查询的兼容凭证。"""
        rel = os.path.relpath(file_path, self._lake_base).replace(os.sep, "/")
        parts = rel.split("/")
        event_type = parts[1] if len(parts) > 1 and parts[0] == "events" else ""
        partition = "/".join(parts[:-1])
        raw = self._read_json_file(file_path)
        payloads = self._extract_payloads(raw) if raw is not None else []
        if confirmed is None:
            confirmed = True
        return {
            "receipt_id": self._legacy_receipt_id(rel),
            "batch_key": batch_key,
            "event_type": event_type,
            "partition": partition,
            "partition_date": "/".join(parts[2:-1]),
            "file_path": file_path,
            "file_name": os.path.basename(file_path),
            "written": len(payloads),
            "record_count": len(payloads),
            "content_digest": self._records_digest(payloads) if payloads else "",
            "source_channel_id": 0,
            "status": STATUS_CONFIRMED if confirmed else STATUS_PENDING,
            "confirmed": bool(confirmed),
            "attempts": 1,
            "legacy": True,
            "replayed": False,
        }

    def _synthetic_receipts(self) -> list[dict[str, Any]]:
        return [self._synthetic_receipt_for_file(path) for path in self._all_lake_files()]

    def _find_lake_file_for_key(self, batch_key: str) -> str | None:
        """按批次键摘要定位同秒写入后丢失回执的文件。"""
        token = self._batch_token(batch_key)
        candidates = [
            path for path in self._all_lake_files()
            if re.match(rf"batch_\d{{6}}_{token}(?:_\d+)?\.json$", os.path.basename(path))
        ]
        return sorted(candidates)[0] if candidates else None

    def _find_lake_file_for_receipt_id(self, receipt_id: str) -> str | None:
        """回执号本身由批次键派生：只拿到回执号时也能沿分区文件找回。"""
        match = re.fullmatch(r"LKW-([0-9a-f]{12})", receipt_id)
        if not match:
            return None
        token = match.group(1)
        candidates = [
            path for path in self._all_lake_files()
            if re.match(rf"batch_\d{{6}}_{token}(?:_\d+)?\.json$", os.path.basename(path))
        ]
        return sorted(candidates)[0] if candidates else None

    def _synthetic_receipt_for_orphan(self, file_path: str, receipt_id: str | None = None,
                                       batch_key: str | None = None) -> dict[str, Any]:
        receipt = self._synthetic_receipt_for_file(file_path, batch_key=batch_key, confirmed=False)
        receipt["status"] = STATUS_PENDING
        receipt["confirmed"] = False
        if receipt_id:
            receipt["receipt_id"] = receipt_id
        return receipt

    def _find_receipt_locked(self, batch_key: str | None = None,
                             receipt_id: str | None = None) -> dict[str, Any] | None:
        if receipt_id:
            raw = self._find_raw_receipt(receipt_id)
            if raw is not None:
                return self._normalize_receipt(raw)
            for receipt in self._synthetic_receipts():
                if receipt["receipt_id"] == receipt_id:
                    return receipt
            # 回执行丢失但文件已写成：回执号可由批次键重建，沿文件名找回待确认状态。
            orphan = self._find_lake_file_for_receipt_id(receipt_id)
            if orphan:
                return self._synthetic_receipt_for_orphan(orphan, receipt_id=receipt_id)
        if not batch_key:
            return None
        # 已持久化的回执优先，最新的一条代表这一批次的最终结果。
        for raw in reversed(json_store.get_all(RECEIPT_MODULE)):
            if str(raw.get("batch_key") or "") == batch_key:
                return self._normalize_receipt(raw)
        # 回执丢失但文件已写成：沿批次键对应的文件名找回，标记为待确认。
        file_path = self._find_lake_file_for_key(batch_key)
        if file_path:
            return self._synthetic_receipt_for_orphan(
                file_path,
                receipt_id=self._batch_receipt_id(batch_key),
                batch_key=batch_key,
            )
        return None

    def find_receipt(self, batch_key: str | None = None,
                     receipt_id: str | None = None) -> dict[str, Any] | None:
        """按批次键或回执号查找写入确认记录（也兼容历史文件）。"""
        with _WRITE_LOCK:
            return self._find_receipt_locked(batch_key=batch_key, receipt_id=receipt_id)

    def find_receipt_by_batch_key(self, batch_key: str) -> dict[str, Any] | None:
        return self.find_receipt(batch_key=batch_key)

    def get_write_receipt(self, receipt_id: str) -> dict[str, Any] | None:
        """按回执号读取单条写入确认记录。"""
        return self.find_receipt(receipt_id=receipt_id)

    def list_write_receipts(self, *, event_type: str | None = None,
                            batch_key: str | None = None,
                            partition: str | None = None,
                            confirmed: bool | None = None,
                            include_legacy: bool = True,
                            limit: int = 1000) -> list[dict[str, Any]]:
        """列出分区写入确认明细，合并持久化回执与历史文件凭证。"""
        with _WRITE_LOCK:
            receipts = [self._normalize_receipt(raw) for raw in json_store.get_all(RECEIPT_MODULE)]
            if include_legacy:
                known = {os.path.abspath(r["file_path"]) for r in receipts if r.get("file_path")}
                for receipt in self._synthetic_receipts():
                    if os.path.abspath(receipt["file_path"]) not in known:
                        receipts.append(receipt)

            if event_type:
                receipts = [r for r in receipts if r.get("event_type") == event_type]
            if batch_key:
                receipts = [r for r in receipts if r.get("batch_key") == batch_key]
            if partition:
                receipts = [r for r in receipts if r.get("partition") == partition]
            if confirmed is not None:
                receipts = [r for r in receipts if bool(r.get("confirmed")) is bool(confirmed)]

            receipts.sort(key=lambda r: (str(r.get("created_at") or ""), str(r.get("receipt_id") or "")))
            return receipts[-limit:] if limit else receipts

    def get_write_receipts(self, **kwargs: Any) -> list[dict[str, Any]]:
        """`list_write_receipts` 的兼容别名。"""
        return self.list_write_receipts(**kwargs)

    def get_partition_write_confirmations(self, *, event_type: str | None = None,
                                          date_str: str | None = None) -> dict[str, Any]:
        """按分区汇总写入确认明细，供回补台账和页面核对使用。"""
        partition_filter = None
        if event_type and date_str:
            partition_filter = f"events/{event_type}/{date_str.strip().replace('-', '/')}"
        receipts = self.list_write_receipts(event_type=event_type, partition=partition_filter)
        by_partition: dict[str, list[dict[str, Any]]] = {}
        for receipt in receipts:
            by_partition.setdefault(receipt.get("partition", ""), []).append(receipt)

        partitions = []
        for name in sorted(by_partition):
            items = by_partition[name]
            partitions.append({
                "partition": name,
                "receipt_count": len(items),
                "confirmed": sum(1 for r in items if r.get("confirmed")),
                "pending": sum(1 for r in items if not r.get("confirmed")),
                "written": sum(int(r.get("written") or 0) for r in items),
                "receipts": items,
            })

        return {
            "ok": True,
            "total": len(receipts),
            "confirmed": sum(1 for r in receipts if r.get("confirmed")),
            "pending": sum(1 for r in receipts if not r.get("confirmed")),
            "partitions": partitions,
        }

    def confirm_write(self, receipt_id: str) -> dict[str, Any]:
        """确认一条已写入的批次；文件不存在时不能确认。"""
        with _WRITE_LOCK:
            result = self._confirm_receipt_locked(receipt_id)
            if result.get("ok"):
                result["data"] = self.query_receipt_data(receipt_id)
            return result

    def _persist_synthetic_receipt(self, receipt: dict[str, Any]) -> dict[str, Any]:
        """把由文件推导出的历史凭证固化为正式记录，避免重复生成。"""
        row = {k: v for k, v in receipt.items() if k != "id"}
        return json_store.insert(RECEIPT_MODULE, row)

    def _confirm_receipt_locked(self, receipt_id: str) -> dict[str, Any]:
        raw = self._find_raw_receipt(receipt_id)
        receipt = self._normalize_receipt(raw) if raw is not None else self._find_receipt_locked(receipt_id=receipt_id)
        if receipt is None:
            return {"ok": False, "message": f"写入回执 {receipt_id} 不存在", "receipt": None}

        file_path = str(receipt.get("file_path") or "")
        raw_data = self._read_json_file(file_path) if file_path else None
        if raw_data is None:
            return {
                "ok": False,
                "message": "回执对应数据文件不存在，无法确认",
                "receipt": receipt,
                "data_missing": True,
            }

        if receipt.get("confirmed"):
            return {
                "ok": True,
                "message": "该批次已确认，无需重复确认",
                "receipt": receipt,
                "written": receipt.get("written", 0),
                "already_confirmed": True,
            }

        if raw is None:
            raw = self._persist_synthetic_receipt(receipt)

        now = datetime.now(timezone.utc).isoformat()
        updates = {
            "status": STATUS_CONFIRMED,
            "confirmed": True,
            "confirmed_at": now,
            "attempts": int(receipt.get("attempts") or 1) + 1,
            "updated_at": now,
        }
        updated = json_store.update(RECEIPT_MODULE, int(raw["id"]), updates)
        receipt.update(updates)
        if updated is not None:
            receipt = self._normalize_receipt(updated)

        return {
            "ok": True,
            "message": "写入已确认",
            "receipt": receipt,
            "written": receipt.get("written", 0),
            "already_confirmed": False,
        }

    def resume_receipt(self, receipt_id: str) -> dict[str, Any]:
        """按原回执续做：补确认并沿回执查回实际数据，不重复写文件。"""
        with _WRITE_LOCK:
            confirmation = self._confirm_receipt_locked(receipt_id)
            data = self.query_receipt_data(receipt_id)
            return {
                "ok": bool(confirmation.get("ok")) and bool(data.get("ok")),
                "message": confirmation.get("message"),
                "receipt": confirmation.get("receipt"),
                "written": confirmation.get("written", 0),
                "confirmed": bool((confirmation.get("receipt") or {}).get("confirmed")),
                "already_confirmed": confirmation.get("already_confirmed", False),
                "data": data,
                "records": data.get("records", []),
                "total": data.get("total", 0),
            }

    recover_receipt = resume_receipt

    def query_receipt_data(self, receipt_id: str, limit: int | None = None) -> dict[str, Any]:
        """按回执实际查询该批次写入的数据；旧凭证也能沿 file_path 追查。"""
        with _WRITE_LOCK:
            receipt = self._find_receipt_locked(receipt_id=receipt_id)
            if receipt is None:
                return {"ok": False, "message": f"写入回执 {receipt_id} 不存在", "records": [], "total": 0}

            file_path = str(receipt.get("file_path") or "")
            if not file_path:
                return {
                    "ok": False,
                    "message": "回执缺少数据文件位置",
                    "receipt": receipt,
                    "records": [],
                    "total": 0,
                    "data_missing": True,
                }

            raw = self._read_json_file(file_path)
            if raw is None:
                return {
                    "ok": False,
                    "message": f"回执对应数据文件不存在：{file_path}",
                    "receipt": receipt,
                    "records": [],
                    "total": 0,
                    "data_missing": True,
                }

            payloads = self._extract_payloads(raw)
            envelopes = raw if isinstance(raw, list) else [raw]
            total = len(payloads)
            records = payloads if limit is None else payloads[:limit]
            digest = self._records_digest(payloads) if payloads else ""
            return {
                "ok": True,
                "receipt_id": receipt.get("receipt_id"),
                "batch_key": receipt.get("batch_key"),
                "event_type": receipt.get("event_type"),
                "partition": receipt.get("partition"),
                "file_path": file_path,
                "records": records,
                "data": records,
                "payloads": records,
                "envelopes": envelopes,
                "total": total,
                "written": int(receipt.get("written") or total),
                "confirmed": bool(receipt.get("confirmed")),
                "status": receipt.get("status"),
                "content_digest": receipt.get("content_digest"),
                "content_matched": (digest == receipt.get("content_digest")) if receipt.get("content_digest") else None,
                "legacy": bool(receipt.get("legacy")),
            }

    # ──────────────────────────────────────────────────────────
    # 写入
    # ──────────────────────────────────────────────────────────
    def write_event(self, event_id: str, event_type: str, payload: dict[str, Any],
                    source_channel_id: int = 0) -> dict[str, Any]:
        """将事件数据写入数据湖。

        写入流程：
        1. 确定分区路径（按日期/事件类型）
        2. 序列化数据（默认JSON格式）
        3. 写入分区文件
        4. 更新写入统计

        文件名使用 event_id 的完整摘要，避免同一秒内不同事件的短前缀相同而互相覆盖。

        Args:
            event_id: 事件唯一标识
            event_type: 事件类型
            payload: 事件数据
            source_channel_id: 来源通道ID

        Returns:
            { ok: bool, message: str, partition: str, file_path: str }
        """
        now = datetime.now(timezone.utc)
        date_partition = now.strftime("%Y/%m/%d")
        partition = f"events/{event_type}/{date_partition}"
        partition_path = self._partition_abs(partition)
        os.makedirs(partition_path, exist_ok=True)

        # 构建数据记录
        record = {
            "event_id": event_id,
            "event_type": event_type,
            "source_channel_id": source_channel_id,
            "payload": payload,
            "ingested_at": now.isoformat(),
            "record_hash": hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False).encode()).hexdigest()[:16],
        }

        token = hashlib.sha256(event_id.encode()).hexdigest()[:16]
        file_name = f"{now.strftime('%H%M%S')}_{token}.json"

        with _WRITE_LOCK:
            file_path = os.path.join(partition_path, file_name)
            # 同一 event_id 重试时覆盖同一文件即为幂等；不同事件撞名则换名保留。
            counter = 1
            while os.path.exists(file_path):
                existing = self._read_json_file(file_path)
                if existing is not None and existing.get("event_id") == event_id:
                    break
                file_name = f"{now.strftime('%H%M%S')}_{token}_{counter}.json"
                file_path = os.path.join(partition_path, file_name)
                counter += 1

            try:
                self._write_json_atomic(file_path, record)
                self._write_stats[event_type] = self._write_stats.get(event_type, 0) + 1

                return {
                    "ok": True,
                    "message": f"数据已写入数据湖分区 {date_partition}",
                    "partition": partition,
                    "file_path": file_path,
                    "record_hash": record["record_hash"],
                }
            except OSError as e:
                return {"ok": False, "message": f"写入失败: {e}", "partition": "", "file_path": ""}

    def write_batch(self, records: list[dict[str, Any]], event_type: str = "batch",
                    source_channel_id: int = 0, batch_key: str | None = None,
                    confirm: bool = True, partition_date: str | None = None,
                    receipt_id: str | None = None) -> dict[str, Any]:
        """批量写入多条记录到数据湖，并生成分区写入确认明细。

        同一秒内的多个批次使用批次键摘要命名文件，各自保留实际结果；
        同一 `batch_key`（或 `receipt_id`）重复提交时直接复用首次回执，
        不会再次落文件，也不会在分区里重复记录。

        Args:
            records: 待写入记录。
            event_type: 事件类型（决定分区目录）。
            source_channel_id: 来源通道/端点。
            batch_key: 批次幂等键。回补任务使用 job+日期区间生成。
            confirm: 是否在写入后立即确认；False 表示只落文件并保留待确认回执。
            partition_date: 可选分区日期（YYYY-MM-DD 或 YYYY/MM/DD）。
            receipt_id: 可选原回执号，用于按原回执续做。
        """
        if not records:
            return {
                "ok": True, "message": "无数据需要写入", "written": 0,
                "records_written": 0, "confirmed": True, "status": STATUS_CONFIRMED,
                "receipt_id": None, "replayed": False, "deduplicated": False,
                "partition": "", "file_path": "",
            }

        content_digest = self._records_digest(records)
        if receipt_id and not batch_key:
            batch_key = f"receipt:{receipt_id}"

        with _WRITE_LOCK:
            existing = self._find_receipt_locked(batch_key=batch_key, receipt_id=receipt_id)
            if existing is not None:
                # 同一批次重试：只保留首次有效写入。若尚未确认，按原文件补确认。
                if confirm and not existing.get("confirmed"):
                    confirmation = self._confirm_receipt_locked(existing["receipt_id"])
                    if confirmation.get("ok"):
                        existing = confirmation.get("receipt") or existing
                return self._build_write_result(
                    existing, replayed=True, content_digest=content_digest,
                    records=records,
                )

            now = datetime.now(timezone.utc)
            date_partition = self._normalize_partition_date(partition_date, now)
            partition = f"events/{event_type}/{date_partition}"
            partition_path = self._partition_abs(partition)
            os.makedirs(partition_path, exist_ok=True)

            token = (
                self._batch_token(batch_key)
                if batch_key else uuid.uuid4().hex[:12]
            )
            base_name = f"batch_{now.strftime('%H%M%S')}_{token}"
            file_name = f"{base_name}.json"
            file_path = os.path.join(partition_path, file_name)
            counter = 1
            while os.path.exists(file_path):
                file_name = f"{base_name}_{counter}.json"
                file_path = os.path.join(partition_path, file_name)
                counter += 1

            batch_records = []
            for i, record in enumerate(records):
                batch_records.append({
                    "event_id": f"batch_{now.strftime('%Y%m%d%H%M%S')}_{token}_{i:04d}",
                    "event_type": event_type,
                    "source_channel_id": source_channel_id,
                    "payload": record,
                    "ingested_at": now.isoformat(),
                })

            try:
                self._write_json_atomic(file_path, batch_records)
            except OSError as e:
                return {"ok": False, "message": f"批量写入失败: {e}", "written": 0,
                        "records_written": 0, "confirmed": False, "receipt_id": None}

            self._write_stats[event_type] = self._write_stats.get(event_type, 0) + len(batch_records)

            created = json_store.insert(RECEIPT_MODULE, {
                "receipt_id": (
                    self._batch_receipt_id(batch_key) if batch_key
                    else f"LKW-{now.strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8]}"
                ),
                "batch_key": batch_key,
                "event_type": event_type,
                "partition": partition,
                "partition_date": date_partition,
                "file_path": file_path,
                "file_name": file_name,
                "written": len(batch_records),
                "record_count": len(batch_records),
                "content_digest": content_digest,
                "source_channel_id": source_channel_id,
                "status": STATUS_PENDING,
                "confirmed": False,
                "attempts": 1,
                "legacy": False,
                "replayed": False,
                "created_at": now.isoformat(),
            })
            receipt = self._normalize_receipt(created)

            if confirm:
                confirmation = self._confirm_receipt_locked(receipt["receipt_id"])
                if confirmation.get("ok"):
                    receipt = confirmation.get("receipt") or receipt
                else:
                    # 文件已经写成但确认失败：保留待确认回执，供后续沿回执续做。
                    receipt["confirmation_error"] = confirmation.get("message")

            return self._build_write_result(
                receipt, replayed=False, content_digest=content_digest,
                records=records,
            )

    def _build_write_result(self, receipt: dict[str, Any], *, replayed: bool,
                            content_digest: str, records: list[dict[str, Any]]) -> dict[str, Any]:
        data = self.query_receipt_data(str(receipt.get("receipt_id")))
        matched = bool(receipt.get("content_digest")) and receipt.get("content_digest") == content_digest
        if replayed:
            message = (
                f"批次已存在，复用回执 {receipt.get('receipt_id')}，"
                f"保留首次写入 {receipt.get('written', 0)} 条"
            )
        elif receipt.get("confirmed"):
            message = f"批量 {receipt.get('written', 0)} 条写入完成并已确认"
        else:
            message = f"批量 {receipt.get('written', 0)} 条已写入，等待确认"

        result = {
            "ok": True,
            "message": message,
            "written": int(receipt.get("written") or 0),
            "records_written": int(receipt.get("written") or 0),
            "partition": receipt.get("partition", ""),
            "file_path": receipt.get("file_path", ""),
            "receipt_id": receipt.get("receipt_id"),
            "receipt": receipt,
            "confirmed": bool(receipt.get("confirmed")),
            "status": receipt.get("status"),
            "replayed": replayed,
            "deduplicated": replayed,
            "content_digest": receipt.get("content_digest"),
            "content_matched": matched,
            "data_available": bool(data.get("ok")),
            "total": int(data.get("total") or 0),
        }
        if not matched and replayed:
            result["message"] += "（本次提交内容与首次写入不同，未覆盖已确认结果）"
        if receipt.get("confirmation_error"):
            result["message"] += f"（未收到确认: {receipt['confirmation_error']}）"
            result["confirmation_error"] = receipt["confirmation_error"]
        return result

    # ──────────────────────────────────────────────────────────
    # 查询与统计
    # ──────────────────────────────────────────────────────────
    def query_lake_data(self, event_type: str, date_str: str | None = None,
                        limit: int = 100) -> dict[str, Any]:
        """查询数据湖中的指定分区数据。"""
        if date_str is None:
            date_str = datetime.now(timezone.utc).strftime("%Y/%m/%d")
        date_str = str(date_str).strip().replace("-", "/")

        partition = f"events/{event_type}/{date_str}"
        partition_path = self._partition_abs(partition)

        if not os.path.exists(partition_path):
            return {"ok": True, "records": [], "total": 0, "partition": partition}

        records = []
        for fname in sorted(os.listdir(partition_path))[:limit]:
            if fname.endswith(".json"):
                data = self._read_json_file(os.path.join(partition_path, fname))
                if data is None:
                    continue
                if isinstance(data, list):
                    records.extend(data)
                else:
                    records.append(data)

        return {
            "ok": True,
            "records": records[:limit],
            "total": len(records),
            "partition": partition,
        }

    def get_lake_stats(self) -> dict[str, Any]:
        """获取数据湖存储统计信息。"""
        total_files = 0
        total_size = 0
        by_type: dict[str, int] = {}

        for root, dirs, files in os.walk(self._lake_base):
            for f in files:
                if f.endswith(".json"):
                    total_files += 1
                    filepath = os.path.join(root, f)
                    try:
                        total_size += os.path.getsize(filepath)
                    except OSError:
                        pass

                # 按事件类型统计
                parts = root.split(os.sep)
                if "events" in parts:
                    idx = parts.index("events")
                    if len(parts) > idx + 1:
                        etype = parts[idx + 1]
                        by_type[etype] = by_type.get(etype, 0) + 1

        receipts = self.list_write_receipts(include_legacy=True)
        return {
            "base_dir": self._lake_base,
            "total_files": total_files,
            "total_size_bytes": total_size,
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "by_type": dict(sorted(by_type.items(), key=lambda x: x[1], reverse=True)),
            "write_stats": self._write_stats,
            "write_receipts": len(receipts),
            "confirmed_receipts": sum(1 for r in receipts if r.get("confirmed")),
            "pending_receipts": sum(1 for r in receipts if not r.get("confirmed")),
        }

    def get_partition_info(self) -> list[dict[str, Any]]:
        """获取所有分区信息。"""
        partitions = []
        for root, dirs, files in os.walk(self._lake_base):
            json_files = [f for f in files if f.endswith(".json")]
            if json_files:
                rel_path = os.path.relpath(root, self._lake_base)
                total = 0
                for f in json_files:
                    try:
                        total += os.path.getsize(os.path.join(root, f))
                    except OSError:
                        pass
                partitions.append({
                    "path": rel_path,
                    "file_count": len(json_files),
                    "size_bytes": total,
                })
        return partitions

    def domain_snapshot(self) -> dict[str, Any]:
        """输出数据落湖的数据质量快照。"""
        rows = json_store.get_all(self.MODULE)
        by_status = {status: 0 for status in self.STATUS_ORDER}
        for row in rows:
            s = row.get("status", "")
            if s in by_status:
                by_status[s] += 1

        lake_stats = self.get_lake_stats()
        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": "数据落湖数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for r in rows if r.get("abnormal")),
            "lake_files": lake_stats.get("total_files", 0),
            "lake_size_mb": lake_stats.get("total_size_mb", 0),
            "write_receipts": lake_stats.get("write_receipts", 0),
            "confirmed_receipts": lake_stats.get("confirmed_receipts", 0),
            "pending_receipts": lake_stats.get("pending_receipts", 0),
            "operation": self.OPERATION_TERM,
        }
