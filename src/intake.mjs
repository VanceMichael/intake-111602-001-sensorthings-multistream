import { randomUUID } from 'node:crypto';
import {
  parseDataArrayPage,
  parseDatastreamPage,
  parseDefinition,
  parseMultiStream,
} from './sensorthings.mjs';

const MAX_PAGES = 1000;

export function defaultObservationsPath(kind, streamId) {
  return kind === 'Datastream'
    ? `/Datastreams(${streamId})/Observations`
    : `/MultiDatastreams(${streamId})/Observations?$resultFormat=dataArray`;
}

export class IntakeError extends Error {}

function stableFingerprint(parts) {
  return JSON.stringify(parts, (key, value) => {
    if (value && typeof value === 'object' && !Array.isArray(value)) {
      return Object.keys(value).sort().reduce((sorted, name) => {
        sorted[name] = value[name];
        return sorted;
      }, {});
    }
    return value;
  });
}

function now() {
  return new Date().toISOString();
}

export function createState({ kind, streamId, observationsPath }) {
  if (!['Datastream', 'MultiDatastream'].includes(kind)) throw new IntakeError('只支持 Datastream 与 MultiDatastream');
  if (!Number.isInteger(streamId) || streamId <= 0) throw new IntakeError('观测流编号无效');
  const stamp = now();
  return {
    id: randomUUID(),
    kind,
    streamId,
    slug: `${kind === 'Datastream' ? 'datastream' : 'multi'}-${streamId}`,
    observationsPath: observationsPath || defaultObservationsPath(kind, streamId),
    createdAt: stamp,
    updatedAt: stamp,
    runs: [],
    fetches: [],
    empty: null,
    // 解释版本上下文，按 parameters.definition / properties.definition 引用分别保存，
    // 新版本定义不会覆盖旧版本，旧结果始终按其引用的版本解释。
    contexts: {},
    snapshots: [],
    observations: {},
  };
}

function contextFor(state, definition) {
  if (!definition) return null;
  if (!state.contexts[definition]) {
    state.contexts[definition] = { definition, definitionDoc: null, snapshot: null, warnings: [] };
  }
  return state.contexts[definition];
}

export class IntakeJob {
  constructor({ catalog, client, state }) {
    this.catalog = catalog;
    this.client = client;
    this.state = state;
  }

  async persist() {
    this.state.updatedAt = now();
    await this.catalog.saveIntake(this.state.slug, this.state);
  }

  // 领取一份原文：成功即进入内容摘要存储并登记凭据；失败只登记领取记录，不抛弃已收原文。
  async fetchRaw(path) {
    const record = { path, requestedAt: now(), ok: false };
    try {
      const result = await this.client.request(path);
      record.status = result.status;
      if (result.status !== 200) {
        record.error = `来源响应：${result.status}`;
        this.state.fetches.push(record);
        return { ok: false, record };
      }
      const receipt = await this.catalog.accept({
        source: result.url,
        bytes: result.bytes,
        mediaType: result.headers['content-type'] || 'application/octet-stream',
        preview: { kind: 'intake-fetch', stream: this.state.streamId, path },
      });
      record.ok = true;
      record.digest = receipt.digest;
      record.receivedAt = receipt.receivedAt;
      this.state.fetches.push(record);
      return { ok: true, record, bytes: result.bytes, digest: receipt.digest };
    } catch (error) {
      record.status = null;
      record.error = error.message;
      this.state.fetches.push(record);
      return { ok: false, record, error };
    }
  }

  // 定义文档是传感器与解释口径的唯一来源；领取失败只登记为暂时缺失，后续运行再补。
  async ensureDefinition(definition) {
    const context = contextFor(this.state, definition);
    if (!context || context.definitionDoc?.status === 'ready') return context;
    const fetched = await this.fetchRaw(`/definitions/${definition}`);
    if (!fetched.ok) {
      context.definitionDoc = {
        status: 'missing',
        lastError: fetched.record.error || '领取失败',
        attemptedAt: now(),
      };
      return context;
    }
    let parsed;
    try {
      parsed = parseDefinition(fetched.bytes);
    } catch (error) {
      context.definitionDoc = { status: 'missing', lastError: error.message, attemptedAt: now() };
      return context;
    }
    context.definitionDoc = { status: 'ready', digest: fetched.digest, fetchedAt: now(), ...parsed };
    if (context.snapshot && parsed.order && stableFingerprint(parsed.order) !== stableFingerprint(context.snapshot.order)) {
      const message = `定义 ${definition} 的属性次序与流快照不一致，观测仍按流快照位置解释`;
      if (!context.warnings.includes(message)) context.warnings.push(message);
    }
    return context;
  }

  async fetchStreamSnapshot() {
    const path = `/MultiDatastreams(${this.state.streamId})`;
    const fetched = await this.fetchRaw(path);
    if (!fetched.ok) return null;
    let snapshot;
    try {
      snapshot = parseMultiStream(fetched.bytes);
    } catch (error) {
      this.state.fetches.at(-1).error = error.message;
      return null;
    }
    const stored = { ...snapshot, digest: fetched.digest, fetchedAt: now() };
    const context = contextFor(this.state, snapshot.definition);
    // 快照按定义版本不可变保存；换版重排生成新快照，绝不修改旧快照。
    if (!context.snapshot || context.snapshot.digest !== fetched.digest) {
      context.snapshot = stored;
      if (!this.state.snapshots.some(item => item.digest === fetched.digest)) this.state.snapshots.push(stored);
    }
    return stored;
  }

  ingestDatastreamItem(item, pageDigest) {
    const observations = this.state.observations;
    if (!observations[item.observationId]) {
      observations[item.observationId] = { observationId: item.observationId, versions: [] };
    }
    const entry = observations[item.observationId];
    const fingerprint = stableFingerprint({
      definition: item.definition,
      result: item.result,
      phenomenonTime: item.phenomenonTime,
    });
    if (entry.versions.some(version => version.fingerprint === fingerprint)) return false;
    entry.versions.push({
      fingerprint,
      receivedAt: now(),
      sourceDigest: pageDigest,
      definition: item.definition,
      phenomenonTime: item.phenomenonTime,
      result: item.result,
      embedded: item.embedded,
    });
    return true;
  }

  ingestMultiRow(row, pageDigest, snapshot) {
    const observations = this.state.observations;
    if (!observations[row.observationId]) {
      observations[row.observationId] = { observationId: row.observationId, versions: [] };
    }
    const entry = observations[row.observationId];
    // 版本指纹只看行内容；定义绑定是可后补的解释依据，元数据恢复后重跑不应制造幽灵更正。
    const fingerprint = stableFingerprint({
      phenomenonTime: row.phenomenonTime,
      values: row.indexedResults.map(cell => cell.value),
    });
    if (entry.versions.some(version => version.fingerprint === fingerprint)) return false;
    entry.versions.push({
      fingerprint,
      receivedAt: now(),
      sourceDigest: pageDigest,
      boundDefinition: snapshot ? snapshot.definition : null,
      phenomenonTime: row.phenomenonTime,
      cells: row.indexedResults.map(cell => ({ index: cell.index, value: cell.value })),
    });
    return true;
  }

  async reconcile() {
    // 先补齐所有被引用但尚未拿到的定义文档。
    for (const definition of Object.keys(this.state.contexts)) {
      await this.ensureDefinition(definition);
    }
    // MultiDatastream 早期没拿到元数据的行，在首个快照可用时一次性绑定，之后不再改绑。
    if (this.state.kind === 'MultiDatastream' && this.state.snapshots.length) {
      const firstSnapshot = this.state.snapshots[0];
      for (const entry of Object.values(this.state.observations)) {
        for (const version of entry.versions) {
          if (version.boundDefinition === null) version.boundDefinition = firstSnapshot.definition;
        }
      }
    }
  }

  async run() {
    const run = { startedAt: now(), finishedAt: null, outcome: null, error: null };
    this.state.runs.push(run);
    let receivedAnyPage = false;
    try {
      if (this.state.kind === 'MultiDatastream') {
        await this.fetchStreamSnapshot();
      }
      let path = this.state.observationsPath;
      let pages = 0;
      let sawRows = false;
      while (path) {
        if (++pages > MAX_PAGES) throw new IntakeError('分页超出安全上限，停止领取');
        const fetched = await this.fetchRaw(path);
        if (!fetched.ok) {
          if (!receivedAnyPage) throw new IntakeError(`观测资料领取失败：${fetched.record.error || '未知错误'}`);
          run.outcome = 'incomplete';
          run.error = fetched.record.error;
          break;
        }
        receivedAnyPage = true;
        if (this.state.kind === 'Datastream') {
          const page = parseDatastreamPage(fetched.bytes);
          if (page.observations.length) sawRows = true;
          for (const item of page.observations) {
            this.ingestDatastreamItem(item, fetched.digest);
            if (item.definition) await this.ensureDefinition(item.definition);
          }
          path = page.nextLink;
        } else {
          const page = parseDataArrayPage(fetched.bytes);
          const snapshot = this.state.snapshots.at(-1) ?? null;
          for (const block of page.blocks) {
            if (block.rows.length) sawRows = true;
            for (const row of block.rows) this.ingestMultiRow(row, fetched.digest, snapshot);
          }
          if (snapshot?.definition) await this.ensureDefinition(snapshot.definition);
          path = page.nextLink;
        }
      }
      this.state.empty = !sawRows;
      await this.reconcile();
      run.outcome = run.outcome || 'completed';
      return this.state;
    } catch (error) {
      run.outcome = receivedAnyPage ? 'incomplete' : 'failed';
      run.error = error.message;
      throw error;
    } finally {
      run.finishedAt = now();
      await this.persist();
    }
  }
}

// 重评单个观测版本的解释状态。缺依据的列保持 pending，依据补齐后自动转 ready。
function evaluateVersion(state, version) {
  if (state.kind === 'Datastream') {
    const reasons = [];
    const property = version.embedded?.observedProperty ?? null;
    const unit = version.embedded?.unit ?? null;
    if (!property || !('@iot.id' in property)) reasons.push('缺少 ObservedProperty 资料');
    if (!unit) reasons.push('缺少单位资料');
    const context = version.definition ? state.contexts[version.definition] : null;
    if (!version.definition) reasons.push('缺少解释版本引用 parameters.definition');
    else if (!context?.definitionDoc || context.definitionDoc.status !== 'ready') {
      reasons.push(`解释版本 ${version.definition} 的定义文档暂未领取`);
    }
    const sensor = context?.definitionDoc?.status === 'ready' ? context.definitionDoc.sensor : null;
    if (sensor === null || sensor === undefined) {
      if (!reasons.some(reason => reason.includes('定义文档'))) reasons.push('定义文档缺少传感器资料');
    }
    return {
      status: reasons.length ? 'pending' : 'ready',
      reasons,
      reading: reasons.length ? null : {
        observedProperty: property,
        unit,
        definition: version.definition,
        sensor,
        result: version.result,
        // 明确空值与暂时缺失分开：null 是站点给出的空结果，仍属可用观测。
        valueKind: version.result === null ? 'null' : 'value',
      },
    };
  }
  const reasons = [];
  let snapshot = null;
  if (version.boundDefinition) {
    snapshot = state.contexts[version.boundDefinition]?.snapshot ?? null;
    if (!snapshot) reasons.push(`绑定的流快照 ${version.boundDefinition} 缺失`);
  } else {
    snapshot = state.snapshots[0] ?? null;
    if (!snapshot) reasons.push('MultiDatastream 元数据尚未领取，无法识别列身份');
  }
  const context = snapshot ? state.contexts[snapshot.definition] : null;
  if (snapshot && (!context?.definitionDoc || context.definitionDoc.status !== 'ready')) {
    reasons.push(`解释版本 ${snapshot.definition} 的定义文档暂未领取`);
  }
  const sensor = context?.definitionDoc?.status === 'ready' ? context.definitionDoc.sensor : null;
  const columnCount = snapshot?.columns.length ?? 0;
  const providedIndices = new Set(version.cells.map(cell => cell.index));
  const cells = version.cells.map(cell => {
    const column = snapshot?.columns[cell.index] ?? null;
    const cellReasons = [];
    if (!column) cellReasons.push('暂时缺失的列：流元数据中没有对应位置的 ObservedProperty');
    if (snapshot && (sensor === null || sensor === undefined)) cellReasons.push('传感器资料暂缺');
    if (cellReasons.length) {
      return { index: cell.index, propertyId: column?.propertyId ?? null, value: cell.value, status: 'pending', reasons: cellReasons };
    }
    return {
      index: cell.index,
      propertyId: column.propertyId,
      name: column.name,
      unit: column.unit,
      dataType: column.dataType,
      definition: snapshot.definition,
      sensor,
      value: cell.value,
      // null 是站点明确给出的空值；pending 才是暂时缺资料的列。
      valueKind: cell.value === null ? 'null' : 'value',
      status: 'ready',
    };
  });
  // 行内结果列短于流公布的属性数：没有给出的列是暂时缺失，不能当作观测不存在或空值。
  if (snapshot) {
    for (let index = 0; index < columnCount; index += 1) {
      if (providedIndices.has(index)) continue;
      const column = snapshot.columns[index];
      cells.push({
        index,
        propertyId: column?.propertyId ?? null,
        value: undefined,
        status: 'pending',
        valueKind: 'absent',
        reasons: ['暂时缺失的列：该观测行未提供此 ObservedProperty 的结果'],
      });
    }
    cells.sort((a, b) => a.index - b.index);
  }
  if (cells.some(cell => cell.status === 'pending')) reasons.push('存在暂时缺失的列');
  return { status: reasons.length ? 'pending' : 'ready', reasons, cells, boundDefinition: snapshot?.definition ?? version.boundDefinition ?? null };
}

function latestVersion(entry) {
  return entry.versions.at(-1);
}

// 对外视图：可用观测集只收录最新版本解释齐全的观测；其余进入待补清单。
export function buildViews(state) {
  const ready = [];
  const pending = [];
  for (const entry of Object.values(state.observations)) {
    const version = latestVersion(entry);
    const evaluation = evaluateVersion(state, version);
    const common = {
      observationId: entry.observationId,
      phenomenonTime: version.phenomenonTime,
      versions: entry.versions.length,
      corrections: entry.versions.length - 1,
      definition: state.kind === 'Datastream' ? version.definition : evaluation.boundDefinition,
      history: entry.versions.map(item => ({
        receivedAt: item.receivedAt,
        definition: state.kind === 'Datastream' ? item.definition : item.boundDefinition,
        sourceDigest: item.sourceDigest,
      })),
    };
    if (evaluation.status === 'ready') {
      ready.push(state.kind === 'Datastream'
        ? { ...common, reading: evaluation.reading }
        : { ...common, readings: evaluation.cells });
    } else {
      pending.push({
        ...common,
        status: 'pending',
        reasons: evaluation.reasons,
        raw: state.kind === 'Datastream'
          ? { result: version.result, embedded: version.embedded, sourceDigest: version.sourceDigest }
          : { cells: version.cells, sourceDigest: version.sourceDigest },
        cells: state.kind === 'MultiDatastream' ? evaluation.cells : undefined,
      });
    }
  }
  const comparator = (a, b) => String(a.observationId).localeCompare(String(b.observationId));
  ready.sort(comparator);
  pending.sort(comparator);
  return {
    stream: { kind: state.kind, streamId: state.streamId },
    empty: state.empty === true,
    counts: {
      observations: Object.keys(state.observations).length,
      ready: ready.length,
      pending: pending.length,
      explicitNulls: countNulls(state),
    },
    ready,
    pending,
  };
}

function countNulls(state) {
  let total = 0;
  for (const entry of Object.values(state.observations)) {
    const version = latestVersion(entry);
    if (state.kind === 'Datastream') {
      if (version.result === null) total += 1;
    } else {
      total += version.cells.filter(cell => cell.value === null).length;
    }
  }
  return total;
}

export function summarize(state) {
  const views = buildViews(state);
  return {
    id: state.id,
    slug: state.slug,
    kind: state.kind,
    streamId: state.streamId,
    observationsPath: state.observationsPath,
    createdAt: state.createdAt,
    updatedAt: state.updatedAt,
    empty: views.empty,
    counts: views.counts,
    runs: state.runs.map(run => ({ startedAt: run.startedAt, finishedAt: run.finishedAt, outcome: run.outcome, error: run.error })),
    definitions: Object.fromEntries(Object.entries(state.contexts).map(([reference, context]) => [
      reference,
      {
        status: context.definitionDoc?.status || 'missing',
        sensor: context.definitionDoc?.sensor ?? null,
        warnings: context.warnings,
        snapshot: context.snapshot
          ? { digest: context.snapshot.digest, order: context.snapshot.order, fetchedAt: context.snapshot.fetchedAt }
          : null,
      },
    ])),
    failedFetches: state.fetches.filter(item => !item.ok).map(({ path, status, error }) => ({ path, status, error })),
  };
}
