"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import resource, pipeline, lake_receipt

# lake_receipt / pipeline 使用固定前缀，必须排在 /api/{module} 通配路由之前，
# 否则 GET /api/lake-receipts 会被当作未知业务模块。
ROUTERS = [lake_receipt, pipeline, resource]
