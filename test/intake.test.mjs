import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Catalog } from '../src/catalog.mjs';
import { createState, IntakeJob, buildViews, summarize } from '../src/intake.mjs';
import { parseDataArrayPage, parseDatastreamPage, parseMultiStream } from '../src/sensorthings.mjs';

function jsonResponse(value) {
  const bytes = Buffer.from(JSON.stringify(value));
  return { status: 200, headers: { 'content-type': 'application/json' }, bytes, url: `http://127.0.0.1/${value.name ?? ''}` };
}

const routeKey = path => {
  const parsed = new URL(path, 'http://127.0.0.1');
  return parsed.pathname + parsed.search;
};

class FakeClient {
  constructor(routes) {
    this.routes = Object.fromEntries(Object.entries(routes).map(([key, value]) => [routeKey(key), value]));
    this.calls = [];
  }

  async request(path) {
    const key = routeKey(path);
    this.calls.push(key);
    const route = this.routes[key];
    if (!route) return { status: 404, headers: {}, bytes: Buffer.from('不存在'), url: path };
    if (route.throw) throw new Error(route.throw);
    return jsonResponse(route.body);
  }
}

const DS_PAGE = revision => ({
  value: [{
    '@iot.id': 101,
    phenomenonTime: '2026-10-12T01:00:00Z',
    result: revision === 2 ? 24.1 : 23.4,
    Datastream: {
      '@iot.id': 7,
      unitOfMeasurement: { symbol: 'Cel' },
      ObservedProperty: { '@iot.id': 9, name: '温度' },
    },
    parameters: { definition: revision === 2 ? 'station-a:4' : 'station-a:3', revision },
  }],
  ...(revision === 1 ? { '@iot.nextLink': '/Datastreams(7)/Observations?$skip=1' } : {}),
});

const DS_NULL_PAGE = {
  value: [{
    '@iot.id': 102,
    phenomenonTime: '2026-10-12T01:00:00Z',
    result: null,
    Datastream: { '@iot.id': 7, unitOfMeasurement: { symbol: 'Cel' }, ObservedProperty: { '@iot.id': 9, name: '温度' } },
    parameters: { definition: 'station-a:3', revision: 1 },
  }],
};

const multiStream = definition => ({
  '@iot.id': 8,
  multiObservationDataTypes: ['OM_Measurement', 'OM_Measurement'],
  unitOfMeasurements: definition === 'station-a:4'
    ? [{ symbol: '%' }, { symbol: 'Cel' }]
    : [{ symbol: 'Cel' }, { symbol: '%' }],
  ObservedProperties: definition === 'station-a:4'
    ? [{ '@iot.id': 10, name: '湿度' }, { '@iot.id': 9, name: '温度' }]
    : [{ '@iot.id': 9, name: '温度' }, { '@iot.id': 10, name: '湿度' }],
  properties: { definition },
});

const dataArray = values => ({
  value: [{
    'MultiDatastream@iot.navigationLink': '/MultiDatastreams(8)',
    components: ['id', 'phenomenonTime', 'result'],
    dataArray: values,
  }],
});

test('普通流分页按字段身份解析，nextLink 缺失即结束', () => {
  const page = parseDatastreamPage(Buffer.from(JSON.stringify(DS_PAGE(1))));
  assert.equal(page.observations[0].observationId, 101);
  assert.equal(page.observations[0].definition, 'station-a:3');
  assert.equal(page.nextLink, '/Datastreams(7)/Observations?$skip=1');
  const last = parseDatastreamPage(Buffer.from(JSON.stringify(DS_NULL_PAGE)));
  assert.equal(last.nextLink, null);
  assert.equal(last.observations[0].result, null);
});

test('dataArray 只按 components 列名取值，重排列顺序不影响身份解析', () => {
  const shuffled = parseDataArrayPage(Buffer.from(JSON.stringify({
    value: [{
      components: ['result', 'id', 'phenomenonTime'],
      dataArray: [[[22.1, 61], 201, '2026-10-12T01:00:00Z']],
    }],
  })));
  const row = shuffled.blocks[0].rows[0];
  assert.equal(row.observationId, 201);
  assert.deepEqual(row.indexedResults.map(cell => cell.value), [22.1, 61]);
  assert.throws(() => parseDataArrayPage(Buffer.from(JSON.stringify({ value: [{ components: ['id'], dataArray: [[1]] }] }))), /result/);
});

test('流元数据按定义版本解析出有序属性列', () => {
  const v3 = parseMultiStream(Buffer.from(JSON.stringify(multiStream('station-a:3'))));
  assert.deepEqual(v3.order, [9, 10]);
  const v4 = parseMultiStream(Buffer.from(JSON.stringify(multiStream('station-a:4'))));
  assert.deepEqual(v4.order, [10, 9]);
});

test('普通流：定义文档缺失时数值留在待补清单，补齐后进入可用集并保留更正历史', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'intake-unit-'));
  try {
    const catalog = await new Catalog(directory).initialize();
    const client = new FakeClient({
      '/Datastreams(7)/Observations': { body: DS_PAGE(1) },
      '/Datastreams(7)/Observations?$skip=1': { body: DS_NULL_PAGE },
    });
    const state = createState({ kind: 'Datastream', streamId: 7 });
    await new IntakeJob({ catalog, client, state }).run();

    let views = buildViews(state);
    assert.equal(views.counts.observations, 2);
    assert.equal(views.ready.length, 0);
    assert.equal(views.pending.length, 2);
    assert.match(views.pending[0].reasons[0], /station-a:3/);
    // 原文已留存，可以凭摘要回读。
    const digest = views.pending[0].raw.sourceDigest;
    assert.deepEqual(JSON.parse((await catalog.object(digest)).toString('utf8')).value[0]['@iot.id'], 101);

    // 站点发布更正值与新定义；继续领取。
    client.routes['/Datastreams(7)/Observations'] = { body: DS_PAGE(2) };
    client.routes['/definitions/station-a:3'] = { body: { order: [9], units: ['Cel'], sensor: 'sensor-1' } };
    client.routes['/definitions/station-a:4'] = { body: { order: [9], units: ['Cel'], sensor: 'sensor-2' } };
    await new IntakeJob({ catalog, client, state }).run();

    views = buildViews(state);
    assert.equal(views.ready.length, 2);
    assert.equal(views.pending.length, 0);
    const corrected = views.ready.find(item => item.observationId === 101);
    assert.equal(corrected.reading.result, 24.1);
    assert.equal(corrected.reading.definition, 'station-a:4');
    assert.equal(corrected.reading.sensor, 'sensor-2');
    assert.equal(corrected.corrections, 1);
    assert.deepEqual(corrected.history.map(item => item.definition), ['station-a:3', 'station-a:4']);
    // 明确空值是独立的取值状态，不等于暂时缺失。
    const empty = views.ready.find(item => item.observationId === 102);
    assert.equal(empty.reading.valueKind, 'null');
    assert.equal(empty.reading.result, null);
    assert.equal(views.counts.explicitNulls, 1);

    const summary = summarize(state);
    assert.equal(summary.runs.at(-1).outcome, 'completed');
    assert.equal(summary.definitions['station-a:3'].sensor, 'sensor-1');
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test('MultiDatastream：换版重排不反套旧结果，缺列与明确空值分开，重跑补齐', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'intake-multi-'));
  try {
    const catalog = await new Catalog(directory).initialize();
    const client = new FakeClient({
      '/MultiDatastreams(8)': { body: multiStream('station-a:3') },
      '/MultiDatastreams(8)/Observations?$resultFormat=dataArray': {
        body: dataArray([[201, '2026-10-12T01:00:00Z', [22.1, 61]], [202, '2026-10-12T01:01:00Z', [null, 62]], [203, '2026-10-12T01:02:00Z', [21.7]]]),
      },
    });
    const state = createState({ kind: 'MultiDatastream', streamId: 8 });
    await new IntakeJob({ catalog, client, state }).run();

    // 没有定义文档，所有观测待补；但列身份已经按第一份快照解析出来。
    let views = buildViews(state);
    assert.equal(views.counts.observations, 3);
    assert.equal(views.ready.length, 0);
    const first = views.pending.find(item => item.observationId === 201);
    assert.deepEqual(first.cells.map(cell => cell.propertyId), [9, 10]);
    assert.equal(first.cells[0].value, 22.1);
    // 203 只有一列：第二列根本没给值，是暂时缺失而不是空值；已给列仍按快照识别身份。
    const shortRow = views.pending.find(item => item.observationId === 203);
    assert.equal(shortRow.cells.length, 2);
    assert.equal(shortRow.cells[0].propertyId, 9);
    assert.equal(shortRow.cells[0].value, 21.7);
    assert.equal(shortRow.cells[1].propertyId, 10);
    assert.equal(shortRow.cells[1].valueKind, 'absent');
    assert.match(shortRow.reasons.join(' '), /暂时缺失的列/);
    // 202 的第一列是站点明确给出的 null。
    const nullRow = views.pending.find(item => item.observationId === 202);
    assert.equal(nullRow.raw.cells[0].value, null);

    // 定义补齐，旧结果按 station-a:3 的次序解释；203 仍缺一列，保持待补。
    client.routes['/definitions/station-a:3'] = { body: { order: [9, 10], units: ['Cel', '%'], sensor: 'sensor-1' } };
    await new IntakeJob({ catalog, client, state }).run();
    views = buildViews(state);
    assert.equal(views.ready.length, 2);
    assert.equal(views.pending.length, 1);
    const stillShort = views.pending.find(item => item.observationId === 203);
    const absent = stillShort.cells.find(cell => cell.valueKind === 'absent');
    assert.equal(absent.propertyId, 10);
    assert.equal(absent.status, 'pending');
    let ready201 = views.ready.find(item => item.observationId === 201);
    const temperature = ready201.readings.find(cell => cell.propertyId === 9);
    const humidity = ready201.readings.find(cell => cell.propertyId === 10);
    assert.equal(temperature.value, 22.1);
    assert.equal(temperature.unit.symbol, 'Cel');
    assert.equal(humidity.value, 61);
    assert.equal(humidity.sensor, 'sensor-1');

    // 站点换传感器并重排属性列，还带来一条更正与新观测。
    client.routes['/MultiDatastreams(8)'] = { body: multiStream('station-a:4') };
    client.routes['/definitions/station-a:4'] = { body: { order: [10, 9], units: ['%', 'Cel'], sensor: 'sensor-2' } };
    client.routes['/MultiDatastreams(8)/Observations?$resultFormat=dataArray'] = {
      body: dataArray([[201, '2026-10-12T01:00:00Z', [22.4, 61]], [202, '2026-10-12T01:01:00Z', [null, 62]], [203, '2026-10-12T01:02:00Z', [21.7, 63]], [204, '2026-10-12T01:03:00Z', [21.9, 64]]]),
    };
    await new IntakeJob({ catalog, client, state }).run();
    views = buildViews(state);
    assert.equal(views.pending.length, 0);
    assert.equal(views.ready.length, 4);
    // 203 补齐缺列后转可用，且保留一次更正历史。
    const completed203 = views.ready.find(item => item.observationId === 203);
    assert.equal(completed203.corrections, 1);
    assert.deepEqual(completed203.readings.map(cell => cell.value), [21.7, 63]);
    ready201 = views.ready.find(item => item.observationId === 201);
    // 新行按新快照（位置 0 现在是湿度 10）取值，旧的已收版本仍绑定旧快照。
    assert.equal(ready201.definition, 'station-a:4');
    const newHumidity = ready201.readings.find(cell => cell.propertyId === 10);
    const newTemperature = ready201.readings.find(cell => cell.propertyId === 9);
    assert.equal(newHumidity.value, 22.4);
    assert.equal(newTemperature.value, 61);
    assert.equal(newTemperature.sensor, 'sensor-2');
    assert.equal(ready201.corrections, 1);
    const history201 = state.observations[201].versions;
    assert.deepEqual(history201.map(version => version.boundDefinition), ['station-a:3', 'station-a:4']);
    // 旧版本的解释没有被新定义覆盖。
    const oldEvaluation = buildViews({ ...state, observations: { 201: { observationId: 201, versions: [history201[0]] } } });
    const oldCell = oldEvaluation.ready[0].readings.find(cell => cell.propertyId === 9);
    assert.equal(oldCell.value, 22.1);
    assert.equal(oldCell.definition, 'station-a:3');
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test('重复运行不产生重复版本，且已就绪状态在依据再次缺失时不降级', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'intake-idem-'));
  try {
    const catalog = await new Catalog(directory).initialize();
    const client = new FakeClient({
      '/Datastreams(7)/Observations': { body: DS_PAGE(1) },
      '/definitions/station-a:3': { body: { order: [9], units: ['Cel'], sensor: 'sensor-1' } },
    });
    const state = createState({ kind: 'Datastream', streamId: 7 });
    const job = new IntakeJob({ catalog, client, state });
    await job.run();
    await job.run();
    const views = buildViews(state);
    assert.equal(views.ready.length, 1);
    assert.equal(state.observations[101].versions.length, 1);
    assert.equal(state.runs.length, 2);
    // 定义文档再次 404 不影响已解释的观测（解释依据已随版本留存）。
    delete client.routes['/definitions/station-a:3'];
    await job.run();
    assert.equal(buildViews(state).ready.length, 1);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test('元数据晚到后重跑不产生幽灵更正，绑定补齐即可解释', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'intake-phantom-'));
  try {
    const catalog = await new Catalog(directory).initialize();
    const stream = {
      '@iot.id': 8,
      multiObservationDataTypes: ['OM_Measurement', 'OM_Measurement'],
      unitOfMeasurements: [{ symbol: 'Cel' }, { symbol: '%' }],
      ObservedProperties: [{ '@iot.id': 9, name: '温度' }, { '@iot.id': 10, name: '湿度' }],
      properties: { definition: 'station-a:3' },
    };
    const page = dataArray([[201, 't', [22.1, 61]]]);
    const client = new FakeClient({
      '/MultiDatastreams(8)': { throw: '连接被切断' },
      '/MultiDatastreams(8)/Observations?$resultFormat=dataArray': { body: page },
      '/definitions/station-a:3': { body: { order: [9, 10], units: ['Cel', '%'], sensor: 'sensor-1' } },
    });
    const state = createState({ kind: 'MultiDatastream', streamId: 8 });
    await new IntakeJob({ catalog, client, state }).run();
    assert.equal(state.observations[201].versions[0].boundDefinition, null);

    client.routes[routeKey('/MultiDatastreams(8)')] = { body: stream };
    await new IntakeJob({ catalog, client, state }).run();
    assert.equal(state.observations[201].versions.length, 1);
    const views = buildViews(state);
    assert.equal(views.ready.length, 1);
    assert.equal(views.ready[0].corrections, 0);
    assert.equal(views.ready[0].definition, 'station-a:3');
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test('没有观测的流显式登记为空，与暂时缺失区分', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'intake-empty-'));
  try {
    const catalog = await new Catalog(directory).initialize();
    const client = new FakeClient({ '/Datastreams(12)/Observations': { body: { value: [] } } });
    const state = createState({ kind: 'Datastream', streamId: 12 });
    await new IntakeJob({ catalog, client, state }).run();
    const views = buildViews(state);
    assert.equal(views.empty, true);
    assert.equal(views.counts.observations, 0);
    assert.deepEqual(views.ready, []);
    assert.deepEqual(views.pending, []);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test('领取失败保留状态与已收原文，下次运行继续领取', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'intake-fail-'));
  try {
    const catalog = await new Catalog(directory).initialize();
    const client = new FakeClient({
      '/MultiDatastreams(8)': { throw: '连接被切断' },
      '/MultiDatastreams(8)/Observations?$resultFormat=dataArray': {
        body: dataArray([[201, 't', [22.1, 61]]]),
      },
    });
    const state = createState({ kind: 'MultiDatastream', streamId: 8 });
    // 关联实体（流元数据）领取失败不致命：原文留存、观测进入待补。
    await new IntakeJob({ catalog, client, state }).run();
    let views = buildViews(state);
    assert.equal(views.ready.length, 0);
    assert.equal(views.pending.length, 1);
    assert.match(views.pending[0].reasons[0], /元数据尚未领取/);
    const digest = state.observations[201].versions[0].sourceDigest;
    assert.ok((await catalog.object(digest)).length > 0);
    assert.ok(state.fetches.some(item => !item.ok && item.path === '/MultiDatastreams(8)'));

    // 观测首页本身失败时，运行报错但状态仍落盘，可在恢复后继续。
    client.routes['/MultiDatastreams(8)/Observations?$resultFormat=dataArray'] = { throw: '来源断开' };
    await assert.rejects(new IntakeJob({ catalog, client, state }).run(), /观测资料领取失败/);
    const reopened = await new Catalog(directory).readIntake('multi-8');
    assert.equal(reopened.observations[201].versions.length, 1);
    assert.equal(summarize(reopened).runs.at(-1).outcome, 'failed');

    // 元数据与定义恢复后继续领取，列身份得到补齐。
    client.routes['/MultiDatastreams(8)'] = { body: multiStream('station-a:3') };
    client.routes['/definitions/station-a:3'] = { body: { order: [9, 10], units: ['Cel', '%'], sensor: 'sensor-1' } };
    client.routes['/MultiDatastreams(8)/Observations?$resultFormat=dataArray'] = {
      body: dataArray([[201, 't', [22.1, 61]]]),
    };
    await new IntakeJob({ catalog, client, state: reopened }).run();
    views = buildViews(reopened);
    assert.equal(views.ready.length, 1);
    assert.deepEqual(views.ready[0].readings.map(cell => cell.propertyId), [9, 10]);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
