/** 纯前端数据层的公共类型：与全栈版后端返回的结构保持一致，换回后端时页面不用改。 */

export type EntryRow = {
  id: number
  status: string
  pending: boolean
  abnormal: boolean
  [field: string]: string | number | boolean
}

export type ModuleMeta = {
  key: string
  name: string
  entity: string
  desc: string
  fields: string[]
  statuses: string[]
  actions: string[]
  actionTargets: Record<string, string>
  metrics: string[]
}

export type PageResult = {
  items: EntryRow[]
  total: number
  page: number
  size: number
}

export type ActionResult = {
  ok: boolean
  message: string
  entry?: Record<string, unknown> | null
  flow?: Record<string, unknown> | null
}

export type FlowEffectItem = {
  action_id: string
  name: string
  module: string
  entry_id: number
  description: string
  timestamp: string
  batch: string | null
  origin_module: string
  origin_id: number
  origin_action: string
}

export type FlowTraceResult = {
  module: string
  entry_id: number
  action_ids: string[]
  effects: FlowEffectItem[]
}

export type ReconcileGap = {
  存储配置: string
  存储配置ID: number
  状态: string
  缺口原因: string
}

export type ReconcileRow = {
  批次: string
  动作编号: string
  管道编号: string
  管道ID: number
  存储配置: string
  存储配置ID: number | null
  接受时间: string
  管道状态: string
  回执状态: string
  回执编号: string | null
  回执时间: string | null
  交付核查: string
  结算状态: string
  结算结果: string | null
  结算时间: string | null
}

export type ReconcileResult = {
  module: string
  items: ReconcileRow[]
  total: number
  gaps: ReconcileGap[]
}

export type OverviewResult = {
  cards: { label: string; value: number }[]
  modules: { name: string; created: number; pending: number; abnormal: number }[]
}
