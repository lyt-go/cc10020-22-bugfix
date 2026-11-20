"""分区写入确认明细与历史回补续做测试。

覆盖的验收点：
1. 同一秒到达的多个批次各自落成独立文件，不再互相覆盖；
2. 同一批次重复提交只保留一次有效写入（文件、回执、台账都不重复）；
3. 写入文件成功但未收到确认的批次，可按原回执续做并查回实际数据；
4. 回补台账与分区写入确认明细共用同一份回执记录；
5. 回补续做不改变另一批已确认结果，分区与回补清单各只记一次；
6. 历史写入凭证（旧文件/旧回执结构）仍可查询。
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.persistence import JsonStore
import app.persistence as persistence_mod
import app.services.data_lake_sink as sink_mod
import app.services.backfill_job as backfill_mod


class _IsolatedStore(unittest.TestCase):
    """每个测试使用独立 JSON 存储和数据湖目录。"""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.store = JsonStore(base_dir=os.path.join(self._tmp.name, "store"))
        self.lake = os.path.join(self._tmp.name, "lake")
        self._orig_store = persistence_mod.json_store
        # 其它测试文件可能 reload 过服务模块，因此始终从模块对象上取当前类。
        self._orig_lake_base = sink_mod.DataLakeSinkService.LAKE_BASE
        persistence_mod.json_store = self.store
        sink_mod.json_store = self.store
        backfill_mod.json_store = self.store
        sink_mod.DataLakeSinkService.LAKE_BASE = self.lake
        self.sink = sink_mod.DataLakeSinkService()
        self.backfill = backfill_mod.BackfillJobService()

    def tearDown(self) -> None:
        persistence_mod.json_store = self._orig_store
        sink_mod.json_store = self._orig_store
        backfill_mod.json_store = self._orig_store
        sink_mod.DataLakeSinkService.LAKE_BASE = self._orig_lake_base
        self._tmp.cleanup()

    def make_job(self, job_no: str, start: str, end: str, status: str = "运行中") -> dict:
        return self.store.insert("backfill_job", {
            "任务编号": job_no,
            "关联端点": "7",
            "起始日期": start,
            "截止日期": end,
            "并行度": "2",
            "覆盖策略": "追加不覆写",
            "完成水位": "0/0",
            "失败区间": "无",
            "status": status,
            "pending": status == "运行中",
            "abnormal": False,
        })

    def partition_files(self, date_path: str) -> list[str]:
        directory = os.path.join(self.lake, "events", "backfill", *date_path.split("/"))
        return sorted(os.listdir(directory)) if os.path.isdir(directory) else []


class TestSameSecondPartitionWrites(_IsolatedStore):
    def test_two_batches_in_same_second_keep_both_results(self):
        first = self.sink.write_batch(
            [{"id": i, "batch": "A"} for i in range(3)],
            event_type="backfill", batch_key="backfill:1:2024-01-01",
            partition_date="2024-01-01",
        )
        second = self.sink.write_batch(
            [{"id": i, "batch": "B"} for i in range(5)],
            event_type="backfill", batch_key="backfill:2:2024-01-01",
            partition_date="2024-01-01",
        )

        self.assertTrue(first["ok"] and second["ok"])
        self.assertNotEqual(first["file_path"], second["file_path"])
        self.assertEqual(len(self.partition_files("2024/01/01")), 2)

        data_a = self.sink.query_receipt_data(first["receipt_id"])
        data_b = self.sink.query_receipt_data(second["receipt_id"])
        self.assertEqual(data_a["total"], 3)
        self.assertEqual(data_b["total"], 5)
        self.assertEqual({r["batch"] for r in data_a["records"]}, {"A"})
        self.assertEqual({r["batch"] for r in data_b["records"]}, {"B"})

        # 两次成功响应都能查到实际数据
        self.assertEqual(self.sink.query_lake_data("backfill", "2024-01-01")["total"], 8)
        self.assertEqual(len(self.sink.list_write_receipts(event_type="backfill")), 2)

    def test_event_writes_in_same_second_do_not_collide(self):
        # 历史实现用 event_id[:8] 命名，同前缀事件会互相覆盖。
        self.sink.write_event("abcdefgh-1", "shared", {"n": 1})
        self.sink.write_event("abcdefgh-2", "shared", {"n": 2})
        records = self.sink.query_lake_data("shared")["records"]
        self.assertEqual(len(records), 2)
        self.assertEqual({r["payload"]["n"] for r in records}, {1, 2})


class TestIdempotentRetry(_IsolatedStore):
    def test_same_batch_key_retry_writes_once(self):
        first = self.sink.write_batch(
            [{"id": i} for i in range(4)],
            event_type="backfill", batch_key="backfill:1:2024-01-01",
            partition_date="2024-01-01",
        )
        retry = self.sink.write_batch(
            [{"id": i} for i in range(4)],
            event_type="backfill", batch_key="backfill:1:2024-01-01",
            partition_date="2024-01-01",
        )

        self.assertTrue(retry["replayed"])
        self.assertEqual(retry["receipt_id"], first["receipt_id"])
        self.assertEqual(len(self.partition_files("2024/01/01")), 1)
        self.assertEqual(len(self.sink.list_write_receipts(event_type="backfill")), 1)
        self.assertEqual(self.sink.query_receipt_data(first["receipt_id"])["total"], 4)

    def test_retry_with_different_content_keeps_first_write(self):
        first = self.sink.write_batch(
            [{"id": i} for i in range(4)],
            event_type="backfill", batch_key="backfill:1:2024-01-01",
            partition_date="2024-01-01",
        )
        retry = self.sink.write_batch(
            [{"id": 99}],
            event_type="backfill", batch_key="backfill:1:2024-01-01",
            partition_date="2024-01-01",
        )

        self.assertTrue(retry["replayed"])
        self.assertFalse(retry["content_matched"])
        self.assertEqual(retry["written"], 4)
        self.assertEqual(self.sink.query_receipt_data(first["receipt_id"])["total"], 4)

    def test_batch_without_key_still_isolated(self):
        first = self.sink.write_batch([{"id": 1}], event_type="adhoc")
        second = self.sink.write_batch([{"id": 2}], event_type="adhoc")
        self.assertNotEqual(first["file_path"], second["file_path"])
        self.assertNotEqual(first["receipt_id"], second["receipt_id"])


class TestLostConfirmationRecovery(_IsolatedStore):
    def test_resume_pending_receipt_returns_actual_data(self):
        pending = self.sink.write_batch(
            [{"id": i} for i in range(3)],
            event_type="backfill", batch_key="backfill:1:2024-01-01",
            partition_date="2024-01-01", confirm=False,
        )
        self.assertFalse(pending["confirmed"])
        self.assertEqual(pending["status"], "待确认")

        resumed = self.sink.resume_receipt(pending["receipt_id"])
        self.assertTrue(resumed["ok"])
        self.assertTrue(resumed["confirmed"])
        self.assertEqual(resumed["total"], 3)
        self.assertTrue(self.sink.get_write_receipt(pending["receipt_id"])["confirmed"])

    def test_receipt_row_lost_still_recoverable_by_batch_key(self):
        written = self.sink.write_batch(
            [{"id": i} for i in range(2)],
            event_type="backfill", batch_key="backfill:1:2024-01-01",
            partition_date="2024-01-01", confirm=False,
        )
        # 模拟回执记录丢失：文件已写成，但没有持久化确认记录。
        self.store.truncate("data_lake_receipt")

        recovered = self.sink.find_receipt(batch_key="backfill:1:2024-01-01")
        self.assertIsNotNone(recovered)
        self.assertFalse(recovered["confirmed"])
        self.assertEqual(recovered["file_path"], written["file_path"])

        confirmed = self.sink.confirm_write(recovered["receipt_id"])
        self.assertTrue(confirmed["ok"])
        self.assertEqual(self.sink.query_receipt_data(recovered["receipt_id"])["total"], 2)

    def test_receipt_row_lost_still_recoverable_by_receipt_id(self):
        written = self.sink.write_batch(
            [{"id": i} for i in range(2)],
            event_type="backfill", batch_key="backfill:1:2024-01-01",
            partition_date="2024-01-01",
        )
        self.store.truncate("data_lake_receipt")

        # 调用方手里仍有回执号，且回执号由批次键派生，可沿文件名找回。
        resumed = self.sink.resume_receipt(written["receipt_id"])
        self.assertTrue(resumed["ok"])
        self.assertTrue(resumed["confirmed"])
        self.assertEqual(resumed["total"], 2)

    def test_confirm_fails_when_data_file_missing(self):
        written = self.sink.write_batch(
            [{"id": 1}], event_type="backfill",
            batch_key="backfill:1:2024-01-01", partition_date="2024-01-01", confirm=False,
        )
        os.remove(written["file_path"])
        result = self.sink.confirm_write(written["receipt_id"])
        self.assertFalse(result["ok"])
        self.assertTrue(result["data_missing"])


class TestBackfillLedger(_IsolatedStore):
    def test_execute_writes_receipts_and_ledger(self):
        job = self.make_job("BF-1", "2024-03-01", "2024-03-02")
        result = self.backfill.execute_backfill(job["id"])

        self.assertTrue(result["ok"])
        self.assertEqual(result["days_processed"], 2)
        entries = self.backfill.get_backfill_ledger(job["id"])["entries"]
        self.assertEqual(len(entries), 2)
        self.assertTrue(all(e["confirmed"] for e in entries))
        for entry in entries:
            receipt = self.sink.get_write_receipt(entry["receipt_id"])
            self.assertIsNotNone(receipt)
            self.assertEqual(receipt["batch_key"], entry["batch_key"])
            self.assertEqual(receipt["partition"], entry["partition"])

    def test_ledger_reconciles_stale_receipt_reference(self):
        job = self.make_job("BF-1", "2024-03-01", "2024-03-01")
        self.backfill.execute_backfill(job["id"])
        entry = self.backfill.get_backfill_ledger(job["id"])["entries"][0]

        # 模拟旧台账残留了过期凭证号：与分区实际写入对不上。
        self.store.update("backfill_job", job["id"], {
            "回补台账": [{**entry, "receipt_id": "LKW-STALE000000"}],
            "回补结论": {"total_batches": 1},
        })

        detail = self.backfill.get_entry(job["id"])
        reconciled = detail["回补台账"]
        self.assertEqual(len(reconciled), 1)
        self.assertEqual(reconciled[0]["receipt_id"], entry["receipt_id"])
        self.assertTrue(reconciled[0]["confirmed"])
        self.assertEqual(detail["回补结论"]["confirmed_batches"], 1)

    def test_ledger_backfills_missing_entry_from_receipts(self):
        job = self.make_job("BF-1", "2024-03-01", "2024-03-01")
        self.backfill.execute_backfill(job["id"])
        # 模拟进程在写台账前中断：回执已有、台账为空。
        self.store.update("backfill_job", job["id"], {"回补台账": []})

        ledger = self.backfill.get_backfill_ledger(job["id"])
        self.assertEqual(len(ledger["entries"]), 1)
        self.assertEqual(ledger["entries"][0]["source"], "回执补齐")
        self.assertTrue(ledger["entries"][0]["confirmed"])


class TestTwoBackfillBatchesSameSecond(_IsolatedStore):
    def test_resume_first_batch_keeps_second_batch_intact(self):
        job_a = self.make_job("BF-A", "2024-03-01", "2024-03-01")
        job_b = self.make_job("BF-B", "2024-03-01", "2024-03-01")

        # A 批文件已写成但没收到确认；B 批正常结束。
        first_run = self.backfill.execute_backfill(job_a["id"], confirm_writes=False)
        self.assertEqual(first_run["days_processed"], 0)
        self.assertEqual(len(first_run["pending_intervals"]), 1)

        second_run = self.backfill.execute_backfill(job_b["id"])
        self.assertEqual(second_run["days_processed"], 1)

        receipt_b = self.backfill.get_backfill_ledger(job_b["id"])["entries"][0]["receipt_id"]
        data_b_before = self.sink.query_receipt_data(receipt_b)

        # 按原回补请求续做 A 批，并依据两个回执实际查询各自数据。
        resumed = self.backfill.resume_backfill(job_a["id"])
        self.assertTrue(resumed["ok"], resumed)
        self.assertEqual(resumed["resumed"], 1)
        self.assertGreater(resumed["recovered"], 0)

        receipt_a = resumed["receipts"][0]["receipt_id"]
        data_a = self.sink.query_receipt_data(receipt_a)
        self.assertGreater(data_a["total"], 0)

        # 两批各自只记一次：分区两个文件、两条回执、每个台账一条。
        self.assertEqual(len(self.partition_files("2024/03/01")), 2)
        partition_receipts = self.sink.list_write_receipts(
            event_type="backfill", partition="events/backfill/2024/03/01")
        self.assertEqual(len(partition_receipts), 2)
        self.assertEqual({r["receipt_id"] for r in partition_receipts}, {receipt_a, receipt_b})
        self.assertEqual(len(self.backfill.get_backfill_ledger(job_a["id"])["entries"]), 1)
        self.assertEqual(len(self.backfill.get_backfill_ledger(job_b["id"])["entries"]), 1)

        # B 批已确认的结果不被改变。
        data_b_after = self.sink.query_receipt_data(receipt_b)
        self.assertEqual(data_b_after["content_digest"], data_b_before["content_digest"])
        self.assertEqual(data_b_after["total"], data_b_before["total"])

    def test_resubmitting_first_batch_matches_same_write(self):
        job_a = self.make_job("BF-A", "2024-03-01", "2024-03-01")
        job_b = self.make_job("BF-B", "2024-03-01", "2024-03-01")
        self.backfill.execute_backfill(job_a["id"], confirm_writes=False)
        self.backfill.execute_backfill(job_b["id"])
        self.backfill.resume_backfill(job_a["id"])

        # 已完成的任务再次提交：对账到原回执，不新增写入。
        resubmitted = self.backfill.execute_backfill(job_a["id"])
        self.assertTrue(resubmitted["ok"], resubmitted)
        self.assertEqual(len(self.partition_files("2024/03/01")), 2)
        self.assertEqual(len(self.sink.list_write_receipts(
            event_type="backfill", partition="events/backfill/2024/03/01")), 2)
        entries = self.backfill.get_backfill_ledger(job_a["id"])["entries"]
        self.assertEqual(len(entries), 1)
        self.assertTrue(entries[0]["confirmed"])

    def test_retry_failed_intervals_confirms_without_duplicate(self):
        job_a = self.make_job("BF-A", "2024-03-01", "2024-03-01")
        self.backfill.execute_backfill(job_a["id"], confirm_writes=False)
        result = self.backfill.retry_failed_intervals(job_a["id"])

        self.assertTrue(result["ok"])
        self.assertEqual(result["retried"], 1)
        self.assertEqual(result["still_failed"], 0)
        self.assertEqual(len(self.partition_files("2024/03/01")), 1)
        self.assertEqual(len(self.sink.list_write_receipts(event_type="backfill")), 1)


class TestLegacyReceipts(_IsolatedStore):
    def _write_legacy_file(self, payloads: list[dict]) -> str:
        directory = os.path.join(self.lake, "events", "backfill", "2024", "02", "01")
        os.makedirs(directory, exist_ok=True)
        path = os.path.join(directory, "batch_120000_legacy000001.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payloads, f)
        return path

    def test_legacy_lake_file_listed_and_queryable(self):
        path = self._write_legacy_file([{"id": 1, "v": "old"}])
        receipts = self.sink.list_write_receipts(event_type="backfill", include_legacy=True)
        self.assertEqual(len(receipts), 1)
        receipt = receipts[0]
        self.assertTrue(receipt["legacy"])
        self.assertTrue(receipt["confirmed"])

        data = self.sink.query_receipt_data(receipt["receipt_id"])
        self.assertTrue(data["ok"])
        self.assertEqual(data["total"], 1)
        self.assertEqual(data["records"][0]["v"], "old")

        confirm = self.sink.confirm_write(receipt["receipt_id"])
        self.assertTrue(confirm["ok"])
        self.assertTrue(confirm["already_confirmed"])
        # 历史文件不被重复落湖
        self.assertEqual(os.path.exists(path), True)
        self.assertEqual(len(self.partition_files("2024/02/01")), 1)

    def test_legacy_receipt_record_with_old_shape(self):
        path = self._write_legacy_file([{"id": 2}, {"id": 3}])
        # 旧版回执只有文件路径和条数，没有 receipt_id / confirmed 字段。
        self.store.insert("data_lake_receipt", {
            "partition": "events/backfill/2024/02/01",
            "file_path": path,
            "written": 2,
            "status": "成功",
        })
        receipts = self.sink.list_write_receipts(event_type="backfill", include_legacy=False)
        self.assertEqual(len(receipts), 1)
        receipt = receipts[0]
        self.assertTrue(receipt["receipt_id"])  # 派生出的兼容凭证号
        self.assertTrue(receipt["confirmed"])
        self.assertEqual(self.sink.query_receipt_data(receipt["receipt_id"])["total"], 2)

    def test_envelope_and_bare_records_both_readable(self):
        envelope_file = self._write_legacy_file([
            {"event_id": "batch_x_0000", "payload": {"id": 10}},
        ])
        second_dir = os.path.join(self.lake, "events", "backfill", "2024", "02", "02")
        os.makedirs(second_dir, exist_ok=True)
        with open(os.path.join(second_dir, "batch_120001_bare.json"), "w", encoding="utf-8") as f:
            json.dump([{"id": 11}], f)

        env_receipt = self.sink._synthetic_receipt_for_file(envelope_file)
        bare_receipt = self.sink._synthetic_receipt_for_file(
            os.path.join(second_dir, "batch_120001_bare.json"))
        self.assertEqual(self.sink.query_receipt_data(env_receipt["receipt_id"])["records"], [{"id": 10}])
        self.assertEqual(self.sink.query_receipt_data(bare_receipt["receipt_id"])["records"], [{"id": 11}])


class TestPartitionConfirmationSummary(_IsolatedStore):
    def test_partition_summary_counts_confirmed_and_pending(self):
        self.sink.write_batch([{"id": 1}], event_type="backfill",
                              batch_key="backfill:1:2024-01-01", partition_date="2024-01-01")
        self.sink.write_batch([{"id": 2}], event_type="backfill",
                              batch_key="backfill:2:2024-01-01", partition_date="2024-01-01",
                              confirm=False)

        summary = self.sink.get_partition_write_confirmations(
            event_type="backfill", date_str="2024-01-01")
        self.assertEqual(summary["total"], 2)
        self.assertEqual(summary["confirmed"], 1)
        self.assertEqual(summary["pending"], 1)
        self.assertEqual(summary["partitions"][0]["partition"], "events/backfill/2024/01/01")

    def test_lake_stats_surface_receipt_counts(self):
        self.sink.write_batch([{"id": 1}], event_type="backfill",
                              batch_key="backfill:1:2024-01-01", partition_date="2024-01-01")
        stats = self.sink.get_lake_stats()
        self.assertEqual(stats["write_receipts"], 1)
        self.assertEqual(stats["confirmed_receipts"], 1)
        self.assertEqual(stats["pending_receipts"], 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
