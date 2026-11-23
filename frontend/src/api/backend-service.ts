import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type {
  ActionResult,
  EntryRow,
  FlowTraceResult,
  OverviewResult,
  PageResult,
  ReconcileResult,
} from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

export async function loadFlowTrace(key: string, id: number): Promise<FlowTraceResult> {
  const response = await request(`/api/${key}/${id}/flow-trace`)
  if (!response.ok) throw new Error('联动轨迹读取失败')
  return await response.json()
}

export async function loadReconcile(key: string): Promise<ReconcileResult> {
  const response = await request(`/api/${key}/reconcile`)
  if (!response.ok) throw new Error('管道对账表读取失败')
  return await response.json()
}

export async function settleBatch(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/reconcile/settle`, {
    method: 'POST',
    body: JSON.stringify(values),
  })
  return await response.json()
}

export async function registerReceipt(
  key: string,
  id: number,
  actionId?: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/receipts`, {
    method: 'POST',
    body: JSON.stringify(actionId ? { 动作编号: actionId } : {}),
  })
  return await response.json()
}
