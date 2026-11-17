import { mkdir, readdir, readFile, rename, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { randomUUID } from 'node:crypto';

const DEFINITIONS = '/definitions/';

// ---------- 路径与载荷解释（纯函数，便于独立校验） ----------

// 识别可领取的观测流路径，区分普通 Datastream 与 MultiDatastream。
export function streamFromPath(path) {
  let url;
  try { url = new URL(path, 'http://localhost'); }
  catch { throw new Error('观测流路径无法解析'); }
  const match = url.pathname.match(/^\/(MultiDatastreams|Datastreams)\(([^)]+)\)\/Observations\/?$/i);
  if (!match) throw new Error('不是可领取的观测流路径');
  const multi = match[1].toLowerCase() === 'multidatastreams';
  return { type: multi ? 'multidatastream' : 'datastream', streamId: match[2],
    pathname: url.pathname, search: url.search,
    id: url.pathname.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '') };
}

// 解析 dataArray 响应：components 决定行字段，结果位置对应流公布的有序属性集合。
export function parseDataArray(payload) {
  const entry = payload?.value?.[0];
  if (!entry || !Array.isArray(entry.components) || !Array.isArray(entry.dataArray)) {
    throw new Error('MultiDatastream 响应缺少 dataArray');
  }
  const { components } = entry;
  const idAt = components.indexOf('id');
  const timeAt = components.indexOf('phenomenonTime');
  const resultAt = components.indexOf('result');
  if (idAt < 0 || resultAt < 0) throw new Error('dataArray components 缺少 id 或 result');
  const rows = entry.dataArray.map((row) => {
    if (!Array.isArray(row)) throw new Error('dataArray 行不是数组');
    return { id: row[idAt], phenomenonTime: timeAt >= 0 ? row[timeAt] : null,
      result: Array.isArray(row[resultAt]) ? row[resultAt] : [row[resultAt]] };
  });
  return { navigationLink: entry['MultiDatastream@iot.navigationLink'] || null, rows };
}

// 流元数据快照：有序属性身份（@iot.id）与单位，以及该批观测引用的解释版本。
export function snapshotFromMetadata(payload) {
  const properties = payload?.ObservedProperties;
  if (!Array.isArray(properties) || properties.length === 0) {
    throw new Error('MultiDatastream 元数据缺少 ObservedProperties');
  }
  const units = Array.isArray(payload?.unitOfMeasurements) ? payload.unitOfMeasurements : [];
  return { definition: payload?.properties?.definition || null,
    properties: properties.map((item) => ({ id: item?.['@iot.id'] ?? null, name: item?.name ?? null })),
    units: properties.map((_, index) => units[index]?.symbol ?? null) };
}

// 校验流公布的属性顺序、单位与解释版本文档是否一致；不一致即缺少可解释依据。
export function basisConflict(snapshot, doc) {
  if (!doc || typeof doc !== 'object' || Array.isArray(doc)) return '定义文档不是对象';
  const ids = snapshot.properties.map((item) => item.id);
  if (Array.isArray(doc.order)
    && (doc.order.length !== ids.length || doc.order.some((id, index) => id !== ids[index]))) {
    return `定义属性顺序 ${JSON.stringify(doc.order)} 与流公布顺序 ${JSON.stringify(ids)} 不一致`;
  }
  if (Array.isArray(doc.units)
    && (doc.units.length !== snapshot.units.length
      || doc.units.some((unit, index) => snapshot.units[index] !== null && unit !== snapshot.units[index]))) {
    return `定义单位 ${JSON.stringify(doc.units)} 与流公布单位 ${JSON.stringify(snapshot.units)} 不一致`;
  }
  return null;
}

// ---------- 按观测流领取 ----------

// 每个观测版本绑定领取当时的解释依据；旧版本不随站点改版而被新定义覆盖。
export class Collector {
  constructor({ client, catalog, root }) {
    if (!client || typeof client.request !== 'function') throw new TypeError('需要来源客户端');
    if (!catalog || typeof catalog.accept !== 'function') throw new TypeError('需要原件目录');
    this.client = client;
    this.catalog = catalog;
    this.root = root;
    this.locks = new Map();
  }

  async initialize() {
    await mkdir(this.root, { recursive: true });
    return this;
  }

  // 运行一次领取；同一流重复调用即为继续领取，可补齐此前缺失的解释资料。
  async collect(path) {
    const stream = streamFromPath(path);
    await this.initialize();
    const previous = this.locks.get(stream.id) || Promise.resolve();
    const current = previous.catch(() => {}).then(() => this.pass(stream, path));
    this.locks.set(stream.id, current);
    try { return await current; }
    finally { if (this.locks.get(stream.id) === current) this.locks.delete(stream.id); }
  }

  async list() {
    await this.initialize();
    const names = await readdir(this.root).catch((error) => {
      if (error.code === 'ENOENT') return [];
      throw error;
    });
    const collections = [];
    for (const name of names) {
      const state = await this.load(name).catch(() => null);
      if (state) collections.push(this.summaryOf(state));
    }
    return collections.sort((a, b) => a.id.localeCompare(b.id));
  }

  async view(id) {
    const state = await this.load(id).catch((error) => {
      if (error.code === 'ENOENT') return null;
      throw error;
    });
    return state ? this.viewOf(state) : null;
  }

  async load(id) {
    return JSON.parse(await readFile(join(this.root, id, 'state.json'), 'utf8'));
  }

  async save(state) {
    const directory = join(this.root, state.id);
    await mkdir(directory, { recursive: true });
    const temporary = join(directory, `state.json.${randomUUID()}.tmp`);
    await writeFile(temporary, JSON.stringify(state, null, 2));
    await rename(temporary, join(directory, 'state.json'));
  }

  async pass(stream, path) {
    let state = await this.load(stream.id).catch((error) => {
      if (error.code === 'ENOENT') return null;
      throw error;
    });
    const created = !state;
    if (!state) {
      state = { id: stream.id, stream: path, type: stream.type, streamId: stream.streamId,
        createdAt: new Date().toISOString(), observations: {}, definitions: {}, runs: [] };
    }
    const at = new Date().toISOString();
    state.updatedAt = at;
    const run = { id: randomUUID(), at, pages: [], fetched: [], corrections: [], resolved: [], errors: [] };
    if (stream.type === 'datastream') await this.passDatastream(state, path, run);
    else await this.passMulti(state, stream, path, run);
    await this.resolvePending(state, run);
    state.runs.push({ id: run.id, at: run.at, pages: run.pages.length, fetched: run.fetched.length,
      corrections: run.corrections.length, resolved: run.resolved.length, errors: run.errors });
    await this.save(state);
    return { id: state.id, created, run: this.runView(run), view: this.viewOf(state) };
  }

  async passDatastream(state, path, run) {
    let url = path;
    const visited = new Set();
    while (url) {
      if (visited.has(url)) { run.errors.push({ url, error: '分页出现循环' }); return; }
      visited.add(url);
      let page;
      try { page = await this.fetchJson(url); }
      catch (error) { run.errors.push({ url, error: error.message }); return; }
      run.pages.push({ url: page.url, digest: page.digest });
      const value = Array.isArray(page.json?.value) ? page.json.value : [];
      for (const observation of value) await this.mergeDatastream(state, observation, page.digest, run);
      url = typeof page.json?.['@iot.nextLink'] === 'string' ? page.json['@iot.nextLink'] : null;
    }
  }

  async passMulti(state, stream, path, run) {
    const url = new URL(path, 'http://localhost');
    if (!url.searchParams.has('$resultFormat')) url.searchParams.set('$resultFormat', 'dataArray');
    let page;
    try { page = await this.fetchJson(url.pathname + url.search); }
    catch (error) { run.errors.push({ url: path, error: error.message }); return; }
    run.pages.push({ url: page.url, digest: page.digest });
    let parsed;
    try { parsed = parseDataArray(page.json); }
    catch (error) { run.errors.push({ url: page.url, error: error.message }); return; }
    const metadataUrl = parsed.navigationLink || `/MultiDatastreams(${stream.streamId})`;
    let snapshot = null;
    try {
      const metadata = await this.fetchJson(metadataUrl);
      snapshot = snapshotFromMetadata(metadata.json);
      state.metadata = { ...snapshot, digest: metadata.digest, fetchedAt: run.at };
      run.pages.push({ url: metadata.url, digest: metadata.digest });
    } catch (error) {
      run.errors.push({ url: metadataUrl, error: error.message });
    }
    const basis = await this.basisForMulti(state, snapshot, run);
    for (const row of parsed.rows) {
      if (row.id === undefined || row.id === null) { run.errors.push({ error: 'dataArray 行缺少 id' }); continue; }
      run.fetched.push(row.id);
      this.mergeVersion(state, row.id, {
        result: row.result,
        phenomenonTime: row.phenomenonTime,
        definition: snapshot?.definition || null,
        revision: null,
        snapshot: snapshot ? { properties: snapshot.properties, units: snapshot.units } : null,
        source: page.digest,
        fetchedAt: run.at,
        basis,
      }, run);
    }
  }

  async mergeDatastream(state, observation, digest, run) {
    const id = observation?.['@iot.id'];
    if (id === undefined || id === null) { run.errors.push({ error: '观测缺少 @iot.id' }); return; }
    run.fetched.push(id);
    const definition = observation?.parameters?.definition || null;
    this.mergeVersion(state, id, {
      result: observation?.result ?? null,
      phenomenonTime: observation?.phenomenonTime ?? null,
      definition,
      revision: observation?.parameters?.revision ?? null,
      snapshot: null,
      source: digest,
      fetchedAt: run.at,
      basis: await this.basisForDefinition(state, definition, run),
    }, run);
  }

  // 同一观测再次领取时按（结果、解释版本、来源修订、时间）判重；
  // 任一变化都记为新版本，更正值由此进入采集结果并可继续追踪。
  mergeVersion(state, id, version, run) {
    const key = String(id);
    let entry = state.observations[key];
    if (!entry) entry = state.observations[key] = { id, versions: [], current: -1 };
    const last = entry.current >= 0 ? entry.versions[entry.current] : null;
    const signature = (v) => JSON.stringify([v.result, v.definition, v.revision, v.phenomenonTime]);
    if (last && signature(last) === signature(version)) return;
    entry.versions.push(version);
    entry.current = entry.versions.length - 1;
    if (last) {
      const changed = [];
      if (JSON.stringify(last.result) !== JSON.stringify(version.result)) changed.push('result');
      if (last.definition !== version.definition) changed.push('definition');
      if (last.revision !== version.revision) changed.push('revision');
      if (changed.length) {
        run.corrections.push({ id, changed,
          from: { definition: last.definition, revision: last.revision },
          to: { definition: version.definition, revision: version.revision } });
      }
    }
  }

  // 普通流观测的解释依据：parameters.definition 指向的定义文档。
  async basisForDefinition(state, definition, run) {
    if (!definition) return { status: 'pending', missing: ['definition'] };
    const doc = await this.definitionDoc(state, definition, run);
    if (!doc) return { status: 'pending', missing: [`definition:${definition}`] };
    return { status: 'ready', definition, sensor: doc.sensor ?? null, digest: doc.digest };
  }

  // 多维流一批观测的解释依据：领取当时的元数据快照加上定义文档，两者须一致。
  async basisForMulti(state, snapshot, run) {
    if (!snapshot) return { status: 'pending', missing: ['metadata'] };
    if (!snapshot.definition) return { status: 'pending', missing: ['definition'] };
    const doc = await this.definitionDoc(state, snapshot.definition, run);
    if (!doc) return { status: 'pending', missing: [`definition:${snapshot.definition}`] };
    const conflict = basisConflict(snapshot, doc);
    if (conflict) return { status: 'pending', missing: [], conflict };
    return { status: 'ready', definition: snapshot.definition, sensor: doc.sensor ?? null, digest: doc.digest };
  }

  // 定义文档按版本引用缓存；失败的引用在下次领取时重试，以便补齐。
  async definitionDoc(state, ref, run) {
    const cached = state.definitions[ref];
    if (cached?.status === 'fetched') return cached;
    if (cached?.attemptedAt === run.at) return null;
    let raw;
    try { raw = await this.fetchRaw(`${DEFINITIONS}${ref}`); }
    catch (error) {
      state.definitions[ref] = { status: 'failed', attemptedAt: run.at, error: error.message };
      return null;
    }
    if (raw.status !== 200) {
      state.definitions[ref] = { status: 'failed', attemptedAt: run.at, error: `来源响应：${raw.status}` };
      return null;
    }
    let doc;
    try { doc = JSON.parse(raw.bytes.toString('utf8')); }
    catch {
      state.definitions[ref] = { status: 'failed', attemptedAt: run.at, error: '定义文档无法解析' };
      return null;
    }
    state.definitions[ref] = { status: 'fetched', attemptedAt: run.at, fetchedAt: run.at,
      digest: raw.digest, sensor: doc.sensor ?? null,
      order: Array.isArray(doc.order) ? doc.order : null,
      units: Array.isArray(doc.units) ? doc.units : null };
    return state.definitions[ref];
  }

  // 继续领取时补齐：对所有仍缺依据的版本重试解释，观测本身不变。
  async resolvePending(state, run) {
    for (const entry of Object.values(state.observations)) {
      for (const version of entry.versions) {
        if (version.basis?.status !== 'pending') continue;
        const basis = state.type === 'multidatastream'
          ? await this.basisForMulti(state,
              version.snapshot ? { ...version.snapshot, definition: version.definition } : null, run)
          : await this.basisForDefinition(state, version.definition, run);
        if (basis.status === 'ready') {
          version.basis = basis;
          if (!run.resolved.includes(entry.id)) run.resolved.push(entry.id);
        }
      }
    }
  }

  async fetchJson(url) {
    const raw = await this.fetchRaw(url);
    if (raw.status !== 200) {
      const error = new Error(`来源响应：${raw.status}`);
      error.status = raw.status;
      throw error;
    }
    try { return { ...raw, json: JSON.parse(raw.bytes.toString('utf8')) }; }
    catch { throw new Error('响应不是有效 JSON'); }
  }

  // 成功领取的原文一律进入原件目录，与解释结果相互独立。
  async fetchRaw(url) {
    const result = await this.client.request(url);
    let digest = null;
    if (result.status === 200) {
      const receipt = await this.catalog.accept({ source: result.url, bytes: result.bytes,
        mediaType: result.headers['content-type'] || null, preview: null });
      digest = receipt.digest;
    }
    return { status: result.status, bytes: result.bytes, digest, url: result.url };
  }

  // ---------- 可用观测集视图 ----------

  viewOf(state) {
    const readings = [];
    const pending = [];
    const observations = [];
    let corrections = 0;
    for (const entry of Object.values(state.observations)) {
      const current = entry.versions[entry.current];
      if (!current) continue;
      if (entry.versions.length > 1) corrections += 1;
      const target = this.readingsFor(state, entry, current);
      readings.push(...target.usable);
      pending.push(...target.withheld);
      observations.push({ id: entry.id, phenomenonTime: current.phenomenonTime, state: target.state,
        versions: entry.versions.map((version) => this.versionSummary(state, version)) });
    }
    const usable = readings.filter((reading) => reading.state === 'value').length;
    const explicitNull = readings.filter((reading) => reading.state === 'explicitNull').length;
    return { id: state.id, stream: state.stream, type: state.type, streamId: state.streamId,
      updatedAt: state.updatedAt,
      empty: observations.length === 0,
      summary: { observations: observations.length, usable, explicitNull, pending: pending.length, corrections },
      readings, pending, observations,
      runs: state.runs.slice(-5) };
  }

  readingsFor(state, entry, version) {
    const ready = version.basis?.status === 'ready';
    if (state.type === 'multidatastream') return this.multiReadings(state, entry, version, ready);
    return this.datastreamReading(state, entry, version, ready);
  }

  datastreamReading(state, entry, version, ready) {
    const base = { observation: entry.id, phenomenonTime: version.phenomenonTime,
      stream: { datastream: state.streamId },
      definition: version.definition, revision: version.revision, source: version.source };
    if (version.result === null) {
      const reading = { ...base, state: 'explicitNull', value: null };
      this.attachBasis(reading, version, ready);
      return { usable: [reading], withheld: [], state: 'explicitNull' };
    }
    if (ready) {
      const doc = state.definitions[version.definition];
      const reading = { ...base, state: 'value', value: version.result,
        sensor: version.basis.sensor, digest: version.basis.digest,
        layout: doc ? { order: doc.order, units: doc.units } : null };
      return { usable: [reading], withheld: [], state: 'value' };
    }
    return { usable: [], withheld: [{ ...base, state: 'pending', ...this.missingOf(version) }], state: 'pending' };
  }

  multiReadings(state, entry, version, ready) {
    const usable = [];
    const withheld = [];
    const snapshot = version.snapshot;
    const base = { observation: entry.id, phenomenonTime: version.phenomenonTime,
      stream: { multidatastream: state.streamId },
      definition: version.definition, source: version.source };
    if (!snapshot) {
      withheld.push({ ...base, property: null, state: 'pending', ...this.missingOf(version) });
      return { usable, withheld, state: 'pending' };
    }
    let hasPending = false;
    let hasValue = false;
    version.result.forEach((cellValue, index) => {
      const property = snapshot.properties[index] || { id: null, name: null };
      const unit = snapshot.units[index] ?? null;
      if (cellValue === null) {
        const cell = { ...base, property, state: 'explicitNull', value: null, unit };
        this.attachBasis(cell, version, ready);
        usable.push(cell);
      } else if (ready) {
        hasValue = true;
        usable.push({ ...base, property, state: 'value', value: cellValue, unit,
          sensor: version.basis.sensor, digest: version.basis.digest });
      } else {
        hasPending = true;
        withheld.push({ ...base, property, state: 'pending', ...this.missingOf(version) });
      }
    });
    return { usable, withheld, state: hasPending ? 'pending' : (hasValue ? 'value' : 'explicitNull') };
  }

  attachBasis(target, version, ready) {
    if (ready) {
      target.sensor = version.basis.sensor;
      target.digest = version.basis.digest;
    } else {
      Object.assign(target, this.missingOf(version));
    }
  }

  missingOf(version) {
    const out = { missing: version.basis?.missing || [] };
    if (version.basis?.conflict) out.conflict = version.basis.conflict;
    return out;
  }

  // 更正追踪用的版本沿革；缺少解释依据的数值即使在沿革中也不展示。
  versionSummary(state, version) {
    const ready = version.basis?.status === 'ready';
    const summary = { phenomenonTime: version.phenomenonTime,
      definition: version.definition, revision: version.revision,
      fetchedAt: version.fetchedAt, basis: version.basis?.status || 'pending', source: version.source };
    if (ready) summary.sensor = version.basis.sensor;
    if (version.basis?.missing?.length) summary.missing = version.basis.missing;
    if (version.basis?.conflict) summary.conflict = version.basis.conflict;
    if (version.snapshot?.properties) summary.order = version.snapshot.properties.map((item) => item.id);
    const isNull = version.result === null
      || (Array.isArray(version.result) && version.result.every((value) => value === null));
    if (ready || isNull) summary.result = version.result;
    else summary.withheld = true;
    return summary;
  }

  summaryOf(state) {
    const view = this.viewOf(state);
    return { id: view.id, stream: view.stream, type: view.type,
      updatedAt: view.updatedAt, empty: view.empty, summary: view.summary };
  }

  runView(run) {
    return { id: run.id, at: run.at, pages: run.pages, fetched: run.fetched.length,
      corrections: run.corrections, resolved: run.resolved, errors: run.errors };
  }
}
