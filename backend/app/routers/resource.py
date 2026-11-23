"""统一资源接口：查询、导出、登记和动作按模块 key 分派到领域服务。"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, Body, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload, PageResult
from app.services import service_for
from app.services.data_flow import flow_engine

router = APIRouter(prefix="/api", tags=["业务资源"])


def require_service(module: str):
    service = service_for(module)
    if service is None:
        raise HTTPException(status_code=404, detail=f"未知业务模块：{module}")
    return service


@router.get("/{module}", response_model=PageResult[dict])
def list_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    items, total = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/{module}/export")
def export_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> StreamingResponse:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    items, _ = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=1, size=1_000_000)
    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", *service.LIST_FIELDS, "status"])
    for item in items:
        values = [item.get("id"), *(item.get(field, "") for field in service.LIST_FIELDS),
                  item.get("status", "")]
        writer.writerow([
            "'" + str(value) if str(value or "").startswith(("=", "+", "-", "@")) else value
            for value in values
        ])
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{module}.csv"'},
    )


@router.get("/{module}/reconcile")
def reconcile_entries(module: str) -> dict[str, Any]:
    """离线导出管道对账表：接受凭据 × 存储回执 × 交接结算（只读）。

    必须注册在 /{module}/{entry_id} 之前，否则 reconcile 会被当成 entry_id。
    """
    require_service(module)
    if module != "offline_exp":
        raise HTTPException(status_code=404, detail="管道对账仅适用于离线导出模块")
    table = flow_engine.reconcile_exports()
    return {"module": module, **table}


@router.post("/{module}/reconcile/settle", response_model=ActionResult)
def settle_handover(
    module: str, payload: dict[str, Any] = Body(default_factory=dict)
) -> ActionResult:
    """值班员提交本班导出批次的交接结算；同批最终只结算一次。"""
    require_service(module)
    if module != "offline_exp":
        raise HTTPException(status_code=404, detail="交接结算仅适用于离线导出模块")
    record, message = flow_engine.settle_handover(
        action_id=payload.get("动作编号") or payload.get("action_id"),
        batch=payload.get("批次") or payload.get("batch"),
        operator=payload.get("值班员") or payload.get("operator"),
        shift=payload.get("班次") or payload.get("shift"),
    )
    return ActionResult(ok=record is not None, message=message, entry=record)


@router.get("/{module}/{entry_id}", response_model=dict)
def get_entry(module: str, entry_id: int) -> dict[str, Any]:
    service = require_service(module)
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"{service.ENTITY} {entry_id} 不存在或已归档")
    return entry


@router.post("/{module}", response_model=ActionResult)
def create_entry(module: str, payload: EntryPayload) -> ActionResult:
    service = require_service(module)
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message=f"{service.ENTITY}已登记", entry=entry)


@router.get("/{module}/{entry_id}/flow-trace")
def get_flow_trace(module: str, entry_id: int):
    """查看某个 entry 参与的跨模块数据流副作用。

    只读接口：回传动作轨迹日志里已记录的事实，
    不会再次认领作业，也不会推动任何新管道进入运行中。
    """
    require_service(module)
    return flow_engine.trace(module, entry_id)


@router.post("/{module}/{entry_id}/receipts", response_model=ActionResult)
def register_receipt(
    module: str, entry_id: int, payload: dict[str, Any] = Body(default_factory=dict)
) -> ActionResult:
    """登记存储回执（存储动作的第二次写入）；同一动作不重复登记。"""
    require_service(module)
    receipt, message = flow_engine.register_receipt(
        module, entry_id, action_id=payload.get("动作编号") or payload.get("action_id"))
    return ActionResult(ok=receipt is not None, message=message, entry=receipt)


@router.post("/{module}/{entry_id}/actions", response_model=ActionResult)
def run_action(module: str, entry_id: int, payload: ActionPayload) -> ActionResult:
    service = require_service(module)
    entry, message = service.run_action(entry_id, payload.action.strip())
    # 动作执行后触发跨模块数据流
    flow_result = flow_engine.on_action(module, payload.action.strip(), entry_id)
    return ActionResult(ok=entry is not None, message=message, entry=entry,
                        flow=flow_result if flow_result["hits"] > 0 else None)
