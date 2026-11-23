<template>
  <section class="page" :data-module="meta.key">
    <header class="page-head">
      <div>
        <h2>{{ meta.name }}</h2>
        <p class="page-desc">{{ meta.desc }}</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">
          登记{{ meta.entity }}
        </button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">
          导出{{ meta.name }}清单
        </button>
        <button
          v-if="meta.key === 'offline_exp'"
          class="btn"
          data-testid="open-reconcile"
          type="button"
          @click="toggleReconcile"
        >
          管道对账
        </button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <p class="status-legend">
      <span v-for="item in statusSummary" :key="item.status" class="legend-item">
        {{ item.status }}：{{ item.count }}
      </span>
    </p>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :data-testid="`filter-${field}`" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in meta.fields" :key="column">{{ column }}</th>
          <th>当前状态</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in meta.fields" :key="column">{{ row[column] ?? '—' }}</td>
          <td>{{ row.status }}</td>
          <td class="row-actions">
            <button
              v-for="action in meta.actions"
              :key="action"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-${action}`"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
            <button
              class="link"
              type="button"
              :data-testid="`trace-${row.id}`"
              @click="openTrace(row)"
            >
              联动轨迹
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="meta.fields.length + 2" class="empty-state">
            暂无{{ meta.name }}数据，可先登记{{ meta.entity }}
          </td>
        </tr>
      </tbody>
    </table>

    <div v-if="showReconcile && meta.key === 'offline_exp'" class="panel" data-testid="reconcile-panel">
      <h3>管道对账表</h3>
      <table class="data-table">
        <thead>
          <tr>
            <th>批次</th>
            <th>动作编号</th>
            <th>管道编号</th>
            <th>管道状态</th>
            <th>回执状态</th>
            <th>交付核查</th>
            <th>结算结果</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="row in reconcileRows" :key="row.批次" :data-testid="`reconcile-${row.批次}`">
            <td>{{ row.批次 }}</td>
            <td>{{ row.动作编号 }}</td>
            <td>{{ row.管道编号 }}</td>
            <td>{{ row.管道状态 }}</td>
            <td>{{ row.回执状态 }}</td>
            <td>{{ row.交付核查 }}</td>
            <td>{{ row.结算结果 ?? '—' }}</td>
            <td>
              <button
                class="link"
                type="button"
                :data-testid="`settle-${row.批次}`"
                @click="submitSettle(row)"
              >
                交接结算
              </button>
            </td>
          </tr>
          <tr v-if="!reconcileRows.length">
            <td colspan="8" class="empty-state">暂无已接受的导出批次</td>
          </tr>
        </tbody>
      </table>
      <p v-for="gap in reconcileGaps" :key="gap.存储配置ID" class="gap-note">
        缺口：{{ gap.存储配置 }}（{{ gap.状态 }}）—— {{ gap.缺口原因 }}
      </p>
    </div>

    <div v-if="tracePanel" class="panel" data-testid="trace-panel">
      <h3>联动轨迹 · {{ meta.entity }} #{{ tracePanel.entry_id }}</h3>
      <p v-if="tracePanel.action_ids.length" class="trace-action-ids">
        动作编号：{{ tracePanel.action_ids.join('、') }}
      </p>
      <ul v-if="tracePanel.effects.length" class="trace-list">
        <li v-for="(effect, index) in tracePanel.effects" :key="index">
          [{{ effect.action_id }}] {{ effect.name }} → {{ effect.module }}#{{ effect.entry_id
          }}<template v-if="effect.batch">（批次 {{ effect.batch }}）</template>：{{ effect.description }}
        </li>
      </ul>
      <p v-else class="empty-state">暂无已记录的联动事件</p>
      <div class="page-actions">
        <button
          v-if="meta.key === 'feature_store' && tracePanel.action_ids.length"
          class="btn"
          data-testid="register-receipt"
          type="button"
          @click="submitReceipt"
        >
          登记存储回执
        </button>
        <button class="btn ghost" type="button" @click="tracePanel = null">关闭</button>
      </div>
    </div>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记{{ meta.entity }}</h3>
        <label v-for="field in meta.fields" :key="field" class="create-field">
          <span>{{ field }}</span>
          <input v-model="createValues[field]" :data-testid="`create-${field}`" required />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" data-testid="submit-create" type="submit" :disabled="creating">
            {{ creating ? '提交中…' : '确认登记' }}
          </button>
        </div>
      </form>
    </div>

    <footer class="page-foot">
      <span>共 {{ total }} 条{{ meta.name }}记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  createEntry,
  downloadEntries,
  listEntries,
  loadFlowTrace,
  loadReconcile,
  moduleMeta,
  registerReceipt,
  runAction as applyAction,
  settleBatch,
} from '@/api/backend-service'
import type { EntryRow, FlowTraceResult, ReconcileGap, ReconcileRow } from '@/data/types'

const props = defineProps<{ moduleKey: string }>()
const meta = moduleMeta(props.moduleKey)
const rows = ref<EntryRow[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = meta.fields.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})
const tracePanel = ref<FlowTraceResult | null>(null)
const showReconcile = ref(false)
const reconcileRows = ref<ReconcileRow[]>([])
const reconcileGaps = ref<ReconcileGap[]>([])
const stats = computed(() => [
  { label: meta.metrics[0], value: total.value },
  { label: meta.metrics[1], value: rows.value.filter((row) => row.status === meta.statuses[1]).length },
  {
    label: meta.metrics[2],
    value: rows.value.filter((row) => row.status === meta.statuses[meta.statuses.length - 1]).length,
  },
])
const statusSummary = computed(() =>
  meta.statuses.map((status) => ({
    status,
    count: rows.value.filter((row) => String(row.status) === status).length,
  })),
)

function resetFilters() {
  filters.value = {}
  void reload()
}

async function exportRows() {
  errorMessage.value = ''
  try {
    await downloadEntries(meta.key, filters.value)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

function openCreate() {
  createValues.value = Object.fromEntries(meta.fields.map((field) => [field, '']))
  errorMessage.value = ''
  showCreate.value = true
}

async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  try {
    const result = await createEntry(meta.key, createValues.value)
    if (!result.ok) throw new Error(result.message)
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

async function runAction(action: string, row: EntryRow) {
  errorMessage.value = ''
  try {
    const result = await applyAction(meta.key, Number(row.id), action)
    if (!result.ok) throw new Error(result.message)
    await reload()
    if (tracePanel.value) await openTrace({ id: tracePanel.value.entry_id } as EntryRow)
    if (showReconcile.value) await reloadReconcile()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '操作失败'
  }
}

async function openTrace(row: EntryRow) {
  errorMessage.value = ''
  try {
    tracePanel.value = await loadFlowTrace(meta.key, Number(row.id))
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '联动轨迹读取失败'
  }
}

async function submitReceipt() {
  if (!tracePanel.value) return
  errorMessage.value = ''
  try {
    const actionIds = tracePanel.value.action_ids
    const result = await registerReceipt(
      meta.key,
      tracePanel.value.entry_id,
      actionIds[actionIds.length - 1],
    )
    if (!result.ok) throw new Error(result.message)
    tracePanel.value = await loadFlowTrace(meta.key, tracePanel.value.entry_id)
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '回执登记失败'
  }
}

async function toggleReconcile() {
  showReconcile.value = !showReconcile.value
  if (showReconcile.value) await reloadReconcile()
}

async function reloadReconcile() {
  errorMessage.value = ''
  try {
    const result = await loadReconcile(meta.key)
    reconcileRows.value = result.items
    reconcileGaps.value = result.gaps
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '管道对账表读取失败'
  }
}

async function submitSettle(row: ReconcileRow) {
  errorMessage.value = ''
  try {
    const result = await settleBatch(meta.key, { 批次: row.批次 })
    if (!result.ok) throw new Error(result.message)
    await reloadReconcile()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '交接结算失败'
  }
}

async function reload() {
  errorMessage.value = ''
  try {
    const payload = await listEntries(meta.key, filters.value)
    rows.value = payload.items
    total.value = payload.total
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '业务列表读取失败'
  }
}

onMounted(reload)
</script>

<style scoped>
.panel {
  border: 1px solid var(--border);
  border-radius: 8px;
  background: #fff;
  padding: 12px 16px;
  margin-bottom: 12px;
}
.panel h3 {
  margin: 0 0 8px;
  font-size: 14px;
}
.trace-action-ids {
  font-size: 13px;
  margin: 0 0 8px;
}
.trace-list {
  margin: 0 0 8px;
  padding-left: 18px;
  font-size: 13px;
}
.trace-list li {
  margin-bottom: 4px;
}
.gap-note {
  color: #b42318;
  font-size: 12px;
  margin: 8px 0 0;
}
</style>
