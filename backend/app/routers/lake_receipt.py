"""数据落湖写入确认明细接口：查询回执、按回执查回实际数据、补确认与续做。

写入确认记录由 data_lake_sink 生成，历史回补任务回写台账时共用同一份记录。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

router = APIRouter(prefix="/api/lake-receipts", tags=["分区写入确认明细"])


@router.get("")
def list_receipts(
    event_type: str | None = Query(default=None),
    batch_key: str | None = Query(default=None),
    partition: str | None = Query(default=None),
    confirmed: bool | None = Query(default=None),
    include_legacy: bool = Query(default=True),
    limit: int = Query(default=200, ge=1, le=5000),
) -> dict[str, Any]:
    """列出分区写入确认明细（含历史写入凭证）。"""
    from app.services.data_lake_sink import DataLakeSinkService
    svc = DataLakeSinkService()
    receipts = svc.list_write_receipts(
        event_type=event_type, batch_key=batch_key, partition=partition,
        confirmed=confirmed, include_legacy=include_legacy, limit=limit,
    )
    return {"ok": True, "items": receipts, "total": len(receipts)}


@router.get("/partitions")
def partition_confirmations(
    event_type: str | None = Query(default=None),
    date_str: str | None = Query(default=None),
) -> dict[str, Any]:
    """按分区汇总写入确认明细。"""
    from app.services.data_lake_sink import DataLakeSinkService
    return DataLakeSinkService().get_partition_write_confirmations(
        event_type=event_type, date_str=date_str)


@router.get("/{receipt_id}")
def get_receipt(receipt_id: str) -> dict[str, Any]:
    """读取单条写入确认记录。"""
    from app.services.data_lake_sink import DataLakeSinkService
    receipt = DataLakeSinkService().get_write_receipt(receipt_id)
    if receipt is None:
        raise HTTPException(status_code=404, detail=f"写入回执 {receipt_id} 不存在")
    return {"ok": True, "receipt": receipt}


@router.get("/{receipt_id}/data")
def get_receipt_data(receipt_id: str, limit: int | None = Query(default=None, ge=1)) -> dict[str, Any]:
    """按回执实际查询该批次写入的数据。"""
    from app.services.data_lake_sink import DataLakeSinkService
    result = DataLakeSinkService().query_receipt_data(receipt_id, limit=limit)
    if not result.get("ok") and result.get("data_missing") is None and "不存在" in str(result.get("message")):
        raise HTTPException(status_code=404, detail=result["message"])
    return result


@router.post("/{receipt_id}/confirm")
def confirm_receipt(receipt_id: str) -> dict[str, Any]:
    """补确认一条已写入但未收到确认的批次。"""
    from app.services.data_lake_sink import DataLakeSinkService
    return DataLakeSinkService().confirm_write(receipt_id)


@router.post("/{receipt_id}/resume")
def resume_receipt(receipt_id: str) -> dict[str, Any]:
    """按原回执续做：补确认并沿回执查回实际数据。"""
    from app.services.data_lake_sink import DataLakeSinkService
    return DataLakeSinkService().resume_receipt(receipt_id)
