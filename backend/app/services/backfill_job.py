"""历史回补任务领域服务：历史区间回补、覆盖策略与失败区间重采。

管理历史时间序列数据的批量回补任务，支持日期区间分割、
并行采集、覆盖策略（追加/覆写）和失败区间追溯。

回补的每一次按日落湖都会在数据湖的「分区写入确认明细」中留下一条回执，
任务自身的「回补台账」只保存 batch_key / 回执号等引用，两处共用同一份
写入确认记录。因此：
- 同一天/同一秒的多个回补批次互不覆盖；
- 写入文件成功但没收到确认的批次，可以按原回补请求沿回执续做；
- 重复提交同一批只命中已有回执，分区和回补清单都只记一次。

跨模块关联：incre_cursor（起始水位）、batch_ingest（批量拉取）、data_lake_sink（回补落湖与写入确认）。
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any

from app.persistence import ensure_persisted, json_store
from app.services.base import BaseModuleService

LEDGER_FIELD = "回补台账"
CONCLUSION_FIELD = "回补结论"
RECEIPT_REF_FIELD = "写入确认"
STATUS_CONFIRMED = "已确认"
STATUS_PENDING = "待确认"
STATUS_FAILED = "写入失败"


class BackfillJobService(BaseModuleService):
    MODULE = "backfill_job"
    ENTITY = "回补任务"
    OPERATION_TERM = "调度"
    LIST_FIELDS = ["任务编号", "关联端点", "起始日期", "截止日期", "并行度", "覆盖策略", "完成水位", "失败区间"]
    REQUIRED_FIELDS = ["任务编号", "关联端点", "起始日期"]
    STATUS_ORDER = ["草稿", "运行中", "已完成", "已取消"]
    ACTION_RULES = {"启动回补": "运行中", "确认完成": "已完成", "取消回补": "已取消"}
    NEGATIVE_ACTIONS = []

    def __init__(self) -> None:
        super().__init__()
        self._failed_intervals: dict[int, list[dict[str, Any]]] = {}
        self._progress: dict[int, float] = {}

    # ──────────────────────────────────────────────────────────
    # 回补批次键与台账
    # ──────────────────────────────────────────────────────────
    def _load_job(self, job_id: int) -> dict[str, Any] | None:
        """读取回补任务；资源路由登记在内存存储时，按同 id 落到 JSON 存储。"""
        try:
            return ensure_persisted(self.MODULE, job_id)
        except Exception:
            return json_store.get_by_id(self.MODULE, job_id)

    @staticmethod
    def batch_key_for(job_id: int, day: str) -> str:
        """同一回补任务同一天的稳定批次键，用于幂等写入和回执查找。"""
        return f"backfill:{int(job_id)}:{str(day).strip().replace('/', '-')}"

    def _batch_key(self, job: dict[str, Any], job_id: int, day: str) -> str:
        # 关键字段进入键中：任务重跑时仍命中同一次写入；区间/策略变化才视为新批次。
        return (
            f"{self.batch_key_for(job_id, day)}"
            f":{job.get('起始日期', '')}:{job.get('截止日期', '')}"
            f":{job.get('覆盖策略', '')}"
        )

    def _job_prefix(self, job_id: int) -> str:
        return f"backfill:{int(job_id)}:"

    @staticmethod
    def _load_ledger(job: dict[str, Any]) -> list[dict[str, Any]]:
        raw = job.get(LEDGER_FIELD)
        if isinstance(raw, list):
            return [dict(item) for item in raw if isinstance(item, dict)]
        if isinstance(raw, str) and raw.strip():
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, list):
                    return [dict(item) for item in parsed if isinstance(item, dict)]
            except json.JSONDecodeError:
                return []
        return []

    def _upsert_ledger(self, job_id: int, entry: dict[str, Any]) -> list[dict[str, Any]]:
        """按 batch_key 更新回补台账：同一批只保留一条，绝不重复追加。"""
        job = self._load_job(job_id) or {}
        ledger = self._load_ledger(job)
        key = entry.get("batch_key") or entry.get("receipt_id")
        replaced = False
        for i, item in enumerate(ledger):
            if key and (item.get("batch_key") == key
                        or (entry.get("receipt_id") and item.get("receipt_id") == entry.get("receipt_id"))):
                ledger[i] = {**item, **entry}
                replaced = True
                break
        if not replaced:
            ledger.append(entry)
        journal = self._conclude(ledger)
        json_store.update(self.MODULE, job_id, {
            LEDGER_FIELD: ledger,
            CONCLUSION_FIELD: journal,
            RECEIPT_REF_FIELD: [item.get("receipt_id") for item in ledger if item.get("receipt_id")],
        })
        return ledger

    def _persist_ledger(self, job_id: int, ledger: list[dict[str, Any]]) -> dict[str, Any]:
        journal = self._conclude(ledger)
        json_store.update(self.MODULE, job_id, {
            LEDGER_FIELD: ledger,
            CONCLUSION_FIELD: journal,
            RECEIPT_REF_FIELD: [item.get("receipt_id") for item in ledger if item.get("receipt_id")],
        })
        return journal

    @staticmethod
    def _conclude(ledger: list[dict[str, Any]]) -> dict[str, Any]:
        confirmed = [item for item in ledger if item.get("confirmed")]
        pending = [item for item in ledger if not item.get("confirmed") and item.get("status") == STATUS_PENDING]
        failed = [item for item in ledger if item.get("status") == STATUS_FAILED]
        return {
            "total_batches": len(ledger),
            "confirmed_batches": len(confirmed),
            "pending_batches": len(pending),
            "failed_batches": len(failed),
            "records_written": sum(int(item.get("written") or 0) for item in ledger),
            "dates": [item.get("date") for item in ledger if item.get("date")],
        }

    def _reconcile_ledger(self, job: dict[str, Any], job_id: int) -> list[dict[str, Any]]:
        """把台账条目与分区写入确认明细对齐，清掉对不上分区的旧凭证。"""
        from app.services.data_lake_sink import DataLakeSinkService
        sink_svc = DataLakeSinkService()

        ledger = self._load_ledger(job)
        receipts = [
            r for r in sink_svc.list_write_receipts(include_legacy=True, limit=0)
            if str(r.get("batch_key") or "").startswith(self._job_prefix(job_id))
        ]
        by_key = {r.get("batch_key"): r for r in receipts if r.get("batch_key")}
        by_id = {r.get("receipt_id"): r for r in receipts}

        reconciled: list[dict[str, Any]] = []
        seen: set[str] = set()
        for item in ledger:
            key = item.get("batch_key")
            receipt = by_key.get(key) or by_id.get(item.get("receipt_id"))
            if receipt is None and key:
                receipt = sink_svc.find_receipt(batch_key=key)
            entry = dict(item)
            if receipt is not None:
                effective = self._effective_status(item, receipt)
                entry.update({
                    "receipt_id": receipt.get("receipt_id"),
                    "batch_key": key or receipt.get("batch_key"),
                    "partition": receipt.get("partition"),
                    "file_path": receipt.get("file_path"),
                    "written": receipt.get("written", entry.get("written", 0)),
                    "confirmed": bool(receipt.get("confirmed")),
                    "status": effective,
                    "legacy": bool(receipt.get("legacy")),
                    "data_available": os.path.exists(str(receipt.get("file_path") or "")),
                })
                seen.add(str(entry.get("receipt_id")))
                seen.add(str(entry.get("batch_key")))
            elif key and not entry.get("status"):
                entry["status"] = STATUS_FAILED
                entry["confirmed"] = False
            reconciled.append(entry)

        # 回执有、台账漏的批次（例如进程在写台账前中断）补进清单，仍只记一次。
        for receipt in receipts:
            ident = str(receipt.get("receipt_id"))
            if ident in seen or str(receipt.get("batch_key")) in seen:
                continue
            reconciled.append({
                "date": receipt.get("partition_date"),
                "batch_key": receipt.get("batch_key"),
                "receipt_id": receipt.get("receipt_id"),
                "partition": receipt.get("partition"),
                "file_path": receipt.get("file_path"),
                "written": receipt.get("written", 0),
                "confirmed": bool(receipt.get("confirmed")),
                "status": STATUS_CONFIRMED if receipt.get("confirmed") else STATUS_PENDING,
                "legacy": bool(receipt.get("legacy")),
                "source": "回执补齐",
                "data_available": os.path.exists(str(receipt.get("file_path") or "")),
            })

        reconciled.sort(key=lambda item: str(item.get("date") or ""))
        return reconciled

    @staticmethod
    def _effective_status(item: dict[str, Any], receipt: dict[str, Any]) -> str:
        """回执决定确认状态；文件已不存在时保留“写入失败”，不伪造成待确认。"""
        if receipt.get("confirmed"):
            return STATUS_CONFIRMED
        if item.get("status") == STATUS_FAILED:
            return STATUS_FAILED
        return STATUS_PENDING

    def _range_fully_confirmed(self, job: dict[str, Any], job_id: int) -> bool:
        """任务区间内每一天都已有确认回执时，重复提交只做对账。"""
        start = self._parse_date(str(job.get("起始日期", "")))
        end = self._parse_date(str(job.get("截止日期", "")))
        if start is None or end is None:
            return False
        confirmed_dates = {
            str(e.get("date"))
            for e in self._reconcile_ledger(job, job_id)
            if e.get("confirmed")
        }
        current = start
        while current <= end:
            if current.strftime("%Y-%m-%d") not in confirmed_dates:
                return False
            current += timedelta(days=1)
        return True

    def get_backfill_ledger(self, job_id: int) -> dict[str, Any]:
        """读取回补台账，并以分区写入确认明细为准做实时对齐。"""
        job = self._load_job(job_id)
        if job is None:
            return {"ok": False, "message": f"回补任务 {job_id} 不存在", "entries": []}
        entries = self._reconcile_ledger(job, job_id)
        persisted = self._load_ledger(job)
        if entries != persisted:
            self._persist_ledger(job_id, entries)
        return {
            "ok": True,
            "job_id": job_id,
            "job_no": job.get("任务编号"),
            "entries": entries,
            "conclusion": self._conclude(entries),
            "pending": [e for e in entries if not e.get("confirmed")],
        }

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        """任务详情读取写入确认明细，避免残留与分区对不上的旧凭证。"""
        job = json_store.get_by_id(self.MODULE, entry_id)
        if job is None:
            # 内存示例数据与 JSON 存储可能并存，回退到基类的存储视图。
            entry = super().get_entry(entry_id)
            if entry is None:
                return None
            job = dict(entry)
        try:
            job[LEDGER_FIELD] = self._reconcile_ledger(job, entry_id)
            job[CONCLUSION_FIELD] = self._conclude(job[LEDGER_FIELD])
        except Exception:
            # 对账失败不应让已有任务详情不可读。
            pass
        return job

    # ──────────────────────────────────────────────────────────
    # 执行与续做
    # ──────────────────────────────────────────────────────────
    def execute_backfill(self, job_id: int, confirm_writes: bool = True) -> dict[str, Any]:
        """执行历史数据回补任务。

        完整流程：
        1. 解析起始/截止日期区间
        2. 按日分割为子区间
        3. 逐日模拟拉取数据
        4. 按原回补请求生成稳定批次键
        5. 拉取数据写入数据湖并生成写入确认回执
        6. 回补结论回写台账（只引用回执，不复制凭证）
        7. 记录未确认/失败区间

        Returns:
            { ok: bool, days_processed: int, records_backfilled: int, failed_intervals: list }
        """
        job = self._load_job(job_id)
        if job is None:
            return {"ok": False, "message": f"回补任务 {job_id} 不存在", "days_processed": 0}

        replay_only = False
        if job.get("status") != "运行中":
            # 已完成的任务允许幂等重放：所有批次都能对上已确认回执时不再拒绝对账。
            replay_only = (
                job.get("status") == "已完成"
                and self._range_fully_confirmed(job, job_id)
            )
            if not replay_only:
                return {"ok": False, "message": f"任务状态{job['status']}，无法执行", "days_processed": 0}

        start_date = self._parse_date(str(job.get("起始日期", "")))
        end_date = self._parse_date(str(job.get("截止日期", "")))
        endpoint_id = int(job.get("关联端点", "0") or "0")
        overwrite_policy = str(job.get("覆盖策略", "追加不覆写"))

        if start_date is None or end_date is None:
            return {"ok": False, "message": "日期格式无效", "days_processed": 0}

        # 获取增量水位作为起始参考
        try:
            from app.services.incre_cursor import IncreCursorService
            cursor_svc = IncreCursorService()
            starting_position = cursor_svc.get_position(endpoint_id).get("position", 0)
        except Exception:
            starting_position = 0

        days_list = []
        current = start_date
        while current <= end_date:
            days_list.append(current.strftime("%Y-%m-%d"))
            current += timedelta(days=1)

        days_processed = 0
        records_backfilled = 0
        pending_intervals = []
        failed_intervals = []
        batch_results = []

        import random
        for day_str in days_list:
            batch_key = self._batch_key(job, job_id, day_str)
            try:
                daily_data = self._mock_fetch_day(endpoint_id, day_str, random.randint(10, 50))
                result = self._write_day(
                    job_id, endpoint_id, day_str, daily_data,
                    batch_key=batch_key, confirm=confirm_writes,
                )
                batch_results.append(result)

                if result.get("ok") and result.get("confirmed"):
                    days_processed += 1
                    records_backfilled += int(result.get("written") or 0)
                    self._progress[job_id] = days_processed / max(len(days_list), 1) * 100
                elif result.get("ok"):
                    pending_intervals.append({
                        "date": day_str,
                        "reason": "写入文件已生成但未收到确认",
                        "receipt_id": result.get("receipt_id"),
                        "batch_key": batch_key,
                    })
                else:
                    failed_intervals.append({"date": day_str, "reason": result.get("message", "落湖失败")})

            except Exception as e:
                failed_intervals.append({"date": day_str, "reason": str(e)})

        # 重新读取台账，确保和回执一致；未确认区间也保留在失败区间中供重试。
        job = self._load_job(job_id) or job
        ledger = self._reconcile_ledger(job, job_id)
        conclusion = self._persist_ledger(job_id, ledger)
        all_intervals = pending_intervals + failed_intervals
        self._failed_intervals[job_id] = all_intervals

        is_complete = conclusion["confirmed_batches"] == len(days_list) and not all_intervals
        json_store.update(self.MODULE, job_id, {
            "完成水位": f"{conclusion['confirmed_batches']}/{len(days_list)}",
            "失败区间": str(all_intervals) if all_intervals else "无",
            "status": "已完成" if is_complete else "运行中",
            "pending": not is_complete,
            "abnormal": len(all_intervals) > 0,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        })

        return {
            "ok": True,
            "message": (
                f"回补对账完成: {conclusion['confirmed_batches']}/{len(days_list)} 天已确认"
                if replay_only else
                f"回补完成: {conclusion['confirmed_batches']}/{len(days_list)} 天已确认，{records_backfilled} 条记录"
            ),
            "replay_only": replay_only,
            "days_processed": days_processed,
            "total_days": len(days_list),
            "records_backfilled": records_backfilled,
            "failed_intervals": all_intervals,
            "pending_intervals": pending_intervals,
            "progress_pct": round(conclusion["confirmed_batches"] / max(len(days_list), 1) * 100, 2),
            "batch_results": batch_results,
            "conclusion": conclusion,
            "starting_position": starting_position,
            "overwrite_policy": overwrite_policy,
        }

    def _write_day(self, job_id: int, endpoint_id: int, day_str: str,
                   records: list[dict[str, Any]], *, batch_key: str,
                   confirm: bool) -> dict[str, Any]:
        """按日写入数据湖，并把回执引用写入回补台账。"""
        from app.services.data_lake_sink import DataLakeSinkService
        sink_svc = DataLakeSinkService()
        result = sink_svc.write_batch(
            records,
            event_type="backfill",
            source_channel_id=endpoint_id,
            batch_key=batch_key,
            confirm=confirm,
            partition_date=day_str,
        )
        entry = {
            "date": day_str,
            "batch_key": batch_key,
            "receipt_id": result.get("receipt_id"),
            "partition": result.get("partition", ""),
            "file_path": result.get("file_path", ""),
            "written": result.get("written", 0),
            "confirmed": bool(result.get("confirmed")),
            "status": STATUS_CONFIRMED if result.get("confirmed") else (
                STATUS_PENDING if result.get("ok") else STATUS_FAILED
            ),
            "source": "回补执行",
            "replayed": bool(result.get("replayed")),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        self._upsert_ledger(job_id, entry)
        result["ledger_entry"] = entry
        return result

    def resume_backfill(self, job_id: int) -> dict[str, Any]:
        """按原回补请求续做，并依据回执逐批实际查询数据。

        - 未确认但文件已写成的批次：沿原回执补确认，不重复落文件；
        - 结果未变已确认的批次：只查询核对，不改变其写入结果；
        - 回执与文件都缺失的批次：按原请求重新拉取并写入同一批次键（仍只记一次）。
        """
        job = self._load_job(job_id)
        if job is None:
            return {"ok": False, "message": f"回补任务 {job_id} 不存在", "resumed": 0, "receipts": []}

        from app.services.data_lake_sink import DataLakeSinkService
        sink_svc = DataLakeSinkService()

        endpoint_id = int(job.get("关联端点", "0") or "0")
        ledger = self._reconcile_ledger(job, job_id)
        if not ledger:
            # 台账为空但任务已有区间：按原请求执行一次，生成回执和台账。
            return self.execute_backfill(job_id)

        receipts = []
        resumed = 0
        verified = 0
        recovered = 0
        failed = 0

        for entry in ledger:
            batch_key = entry.get("batch_key")
            receipt = None
            if entry.get("receipt_id"):
                receipt = sink_svc.find_receipt(receipt_id=entry.get("receipt_id"))
            if receipt is None and batch_key:
                receipt = sink_svc.find_receipt(batch_key=batch_key)

            if receipt is None:
                # 回执和文件都没有：按原回补请求重做这一天的写入。
                day_str = str(entry.get("date") or "")
                if not day_str or not batch_key:
                    failed += 1
                    receipts.append({"batch_key": batch_key, "ok": False, "reason": "台账条目缺少日期或批次键"})
                    continue
                import random
                data = self._mock_fetch_day(endpoint_id, day_str, random.randint(10, 50))
                result = self._write_day(
                    job_id, endpoint_id, day_str, data,
                    batch_key=batch_key, confirm=True,
                )
                receipts.append({
                    "batch_key": batch_key,
                    "receipt_id": result.get("receipt_id"),
                    "ok": bool(result.get("ok")),
                    "confirmed": bool(result.get("confirmed")),
                    "replayed": bool(result.get("replayed")),
                    "written": result.get("written", 0),
                    "data": sink_svc.query_receipt_data(str(result.get("receipt_id"))),
                })
                if result.get("confirmed"):
                    resumed += 1
                    recovered += int(result.get("written") or 0)
                else:
                    failed += 1
                continue

            # 文件已经写成但未确认：按原回执续做；已确认批次只做查询核对。
            if not receipt.get("confirmed"):
                confirmation = sink_svc.confirm_write(str(receipt.get("receipt_id")))
                if confirmation.get("ok"):
                    receipt = confirmation.get("receipt") or receipt
                    resumed += 1
                else:
                    failed += 1
                    receipts.append({
                        "batch_key": batch_key,
                        "receipt_id": receipt.get("receipt_id"),
                        "ok": False,
                        "confirmed": False,
                        "reason": confirmation.get("message"),
                    })
                    continue

            data = sink_svc.query_receipt_data(str(receipt.get("receipt_id")))
            if data.get("ok"):
                verified += 1
                recovered += int(data.get("total") or 0)
            else:
                failed += 1

            self._upsert_ledger(job_id, {
                "date": entry.get("date"),
                "batch_key": batch_key,
                "receipt_id": receipt.get("receipt_id"),
                "partition": receipt.get("partition"),
                "file_path": receipt.get("file_path"),
                "written": receipt.get("written", 0),
                "confirmed": bool(receipt.get("confirmed")),
                "status": self._effective_status(entry, receipt),
                "source": "回补续做",
                "replayed": True,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            })
            receipts.append({
                "date": entry.get("date"),
                "batch_key": batch_key,
                "receipt_id": receipt.get("receipt_id"),
                "ok": bool(data.get("ok")),
                "confirmed": bool(receipt.get("confirmed")),
                "written": receipt.get("written", 0),
                "partition": receipt.get("partition"),
                "file_path": receipt.get("file_path"),
                "data": data,
                "records": data.get("records", []),
                "total": data.get("total", 0),
            })

        job = self._load_job(job_id) or job
        final_ledger = self._reconcile_ledger(job, job_id)
        conclusion = self._persist_ledger(job_id, final_ledger)
        still_pending = [e for e in final_ledger if not e.get("confirmed")]
        total_days = max(len({e.get("date") for e in final_ledger}), 1)
        is_complete = not still_pending and conclusion["confirmed_batches"] >= total_days
        json_store.update(self.MODULE, job_id, {
            "完成水位": f"{conclusion['confirmed_batches']}/{total_days}",
            "失败区间": "无" if is_complete else str([
                {"date": e.get("date"), "reason": "写入未确认", "receipt_id": e.get("receipt_id")}
                for e in still_pending
            ]),
            "status": "已完成" if is_complete else job.get("status", "运行中"),
            "pending": not is_complete,
            "abnormal": bool(still_pending),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        })

        return {
            "ok": failed == 0,
            "message": (
                f"回补续做完成: 补确认 {resumed} 批, 查询核对 {verified} 批, "
                f"追回 {recovered} 条, 失败 {failed} 批"
            ),
            "resumed": resumed,
            "verified": verified,
            "recovered": recovered,
            "failed": failed,
            "conclusion": conclusion,
            "receipts": receipts,
            "ledger": final_ledger,
        }

    def retry_failed_intervals(self, job_id: int) -> dict[str, Any]:
        """重试失败区间：复用原批次键，成功或补确认后只保留一次有效写入。"""
        job = self._load_job(job_id)
        if job is None:
            return {"ok": False, "message": "任务不存在"}

        failed = self._failed_intervals.get(job_id)
        if failed is None:
            failed = [
                {"date": e.get("date"), "batch_key": e.get("batch_key"), "reason": e.get("status")}
                for e in self._reconcile_ledger(job, job_id) if not e.get("confirmed")
            ]
        if not failed:
            return {"ok": True, "message": "无失败区间需要重试", "retried": 0}

        endpoint_id = int(job.get("关联端点", "0") or "0")
        retried = 0
        still_failed = []
        first_retry = self.resume_backfill(job_id)

        for interval in failed:
            day_str = str(interval.get("date") or "")
            if not day_str:
                still_failed.append(interval)
                continue
            batch_key = interval.get("batch_key") or self._batch_key(job, job_id, day_str)
            try:
                from app.services.data_lake_sink import DataLakeSinkService
                sink_svc = DataLakeSinkService()
                existing = sink_svc.find_receipt(batch_key=batch_key)
                if existing is not None:
                    if not existing.get("confirmed"):
                        conf = sink_svc.confirm_write(str(existing.get("receipt_id")))
                        if not conf.get("ok"):
                            still_failed.append(interval)
                            continue
                    retried += 1
                    continue

                daily_data = self._mock_fetch_day(endpoint_id, day_str, 20)
                result = self._write_day(
                    job_id, endpoint_id, day_str, daily_data,
                    batch_key=batch_key, confirm=True,
                )
                if result.get("confirmed"):
                    retried += 1
                else:
                    still_failed.append(interval)
            except Exception:
                still_failed.append(interval)

        self._failed_intervals[job_id] = still_failed
        json_store.update(self.MODULE, job_id, {
            "失败区间": str(still_failed) if still_failed else "无",
            "abnormal": bool(still_failed),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        })

        return {
            "ok": True,
            "message": f"重试完成: 成功 {retried}/{len(failed)}",
            "retried": retried,
            "still_failed": len(still_failed),
            "resume": first_retry,
        }

    def _parse_date(self, date_str: str) -> datetime | None:
        """解析日期字符串（支持多种格式）。"""
        formats = ["%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"]
        if "T" in date_str:
            date_str = date_str.split("T")[0]
        for fmt in formats:
            try:
                return datetime.strptime(date_str.strip(), fmt)
            except ValueError:
                continue
        return None

    def _mock_fetch_day(self, endpoint_id: int, day: str, count: int) -> list[dict[str, Any]]:
        """模拟拉取一天的数据。"""
        import random
        return [
            {
                "id": i + 1,
                "date": day,
                "endpoint_id": endpoint_id,
                "value": random.randint(1, 1000),
                "category": random.choice(["A", "B", "C"]),
                "timestamp": f"{day}T{random.randint(0,23):02d}:{random.randint(0,59):02d}:00",
            }
            for i in range(count)
        ]

    def get_backfill_progress(self, job_id: int) -> dict[str, Any]:
        """获取回补任务进度（以回执确认情况为准）。"""
        job = self._load_job(job_id)
        if job is None:
            return {"ok": False, "message": "任务不存在"}

        ledger = self._reconcile_ledger(job, job_id)
        conclusion = self._conclude(ledger)
        start = self._parse_date(str(job.get("起始日期", "")))
        end = self._parse_date(str(job.get("截止日期", "")))
        total_days = (end - start).days + 1 if start and end and end >= start else len(ledger)
        confirmed = conclusion["confirmed_batches"]

        return {
            "ok": True,
            "job_id": job_id,
            "job_no": job.get("任务编号"),
            "status": job.get("status"),
            "progress_pct": round(confirmed / max(total_days, 1) * 100, 2),
            "failed_intervals": sum(1 for e in ledger if not e.get("confirmed")),
            "pending_receipts": sum(1 for e in ledger if not e.get("confirmed")),
            "total_days": total_days,
            "confirmed_batches": confirmed,
            "endpoint": job.get("关联端点"),
            "date_range": f"{job.get('起始日期')} ~ {job.get('截止日期')}",
            "conclusion": conclusion,
        }

    def domain_snapshot(self) -> dict[str, Any]:
        """输出历史回补任务的数据质量快照。"""
        rows = json_store.get_all(self.MODULE)
        by_status = {status: 0 for status in self.STATUS_ORDER}
        for row in rows:
            s = row.get("status", "")
            if s in by_status:
                by_status[s] += 1

        from app.services.data_lake_sink import DataLakeSinkService
        sink_svc = DataLakeSinkService()
        receipts = [r for r in sink_svc.list_write_receipts(include_legacy=True, limit=0)
                    if str(r.get("batch_key") or "").startswith("backfill:")]

        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": "历史回补任务数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for r in rows if r.get("abnormal")),
            "active_jobs": sum(1 for r in rows if r.get("status") == "运行中"),
            "completed_jobs": sum(1 for r in rows if r.get("status") == "已完成"),
            "write_receipts": len(receipts),
            "pending_receipts": sum(1 for r in receipts if not r.get("confirmed")),
            "operation": self.OPERATION_TERM,
        }
