import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Catalog } from '../src/catalog.mjs';
import { Collector, streamFromPath, parseDataArray, snapshotFromMetadata, basisConflict } from '../src/collect.mjs';
import { SourceClient } from '../src/transport.mjs';
import { startSource } from '../source/service.mjs';
import { startApplication } from '../src/app.mjs';

async function makeCollector(base) {
  const directory = await mkdtemp(join(tmpdir(), 'collect-test-'));
  const catalog = await new Catalog(directory).initialize();
  const collector = new Collector({ client: new SourceClient(base), catalog, root: join(directory, 'collections') });
  return { directory, catalog, collector };
}

test('观测流路径识别区分普通流与多维流，拒绝其他路径', () => {
  const plain = streamFromPath('/Datastreams(7)/Observations');
  assert.equal(plain.type, 'datastream');
  assert.equal(plain.streamId, '7');
  assert.equal(plain.id, 'datastreams-7-observations');
  const multi = streamFromPath('/MultiDatastreams(8)/Observations?$resultFormat=dataArray');
  assert.equal(multi.type, 'multidatastream');
  assert.equal(multi.streamId, '8');
  assert.throws(() => streamFromPath('/observations/101'), /观测流/);
  assert.throws(() => streamFromPath('/Datastreams(7)'), /观测流/);
});

test('dataArray 解析按 components 取字段，列映射不依赖位置假设', () => {
  const parsed = parseDataArray({ value: [{ 'MultiDatastream@iot.navigationLink': '/MultiDatastreams(8)',
    components: ['id', 'phenomenonTime', 'result'],
    dataArray: [[201, '2026-10-12T01:00:00Z', [22.1, 61]]] }] });
  assert.equal(parsed.navigationLink, '/MultiDatastreams(8)');
  assert.deepEqual(parsed.rows, [{ id: 201, phenomenonTime: '2026-10-12T01:00:00Z', result: [22.1, 61] }]);
  const snapshot = snapshotFromMetadata({ properties: { definition: 'station-a:4' },
    ObservedProperties: [{ '@iot.id': 10, name: '湿度' }, { '@iot.id': 9, name: '温度' }],
    unitOfMeasurements: [{ symbol: '%' }, { symbol: 'Cel' }] });
  assert.deepEqual(snapshot.properties.map(item => item.id), [10, 9]);
  assert.equal(basisConflict(snapshot, { order: [10, 9], units: ['%', 'Cel'], sensor: 'sensor-2' }), null);
  assert.match(basisConflict(snapshot, { order: [9, 10], units: ['%', 'Cel'] }), /不一致/);
  assert.match(basisConflict(snapshot, { order: [10, 9], units: ['Cel', '%'] }), /不一致/);
  assert.throws(() => parseDataArray({ value: [] }), /dataArray/);
});

test('普通流领取：解释版本缺失时保留已收原文，数值不进入可用观测集', async () => {
  const source = await startSource({ scenario: 'transition' });
  const { directory, catalog, collector } = await makeCollector(source.base);
  try {
    const result = await collector.collect('/Datastreams(7)/Observations');
    assert.equal(result.created, true);
    const { view } = result;
    assert.equal(view.summary.observations, 2);
    assert.equal(view.summary.usable, 0);
    assert.equal(view.summary.pending, 1);
    // 观测 101 引用 station-a:3，定义取不到：数值被扣留并说明缺什么
    const pending = view.pending[0];
    assert.equal(pending.observation, 101);
    assert.equal(pending.state, 'pending');
    assert.ok(!('value' in pending));
    assert.deepEqual(pending.missing, ['definition:station-a:3']);
    // 观测 102 是明确空值，与暂时缺失区分
    const explicitNull = view.readings.find(reading => reading.observation === 102);
    assert.equal(explicitNull.state, 'explicitNull');
    assert.equal(explicitNull.value, null);
    // 已收原文留在原件目录，可凭摘要回读
    const receipts = await catalog.list();
    assert.ok(receipts.length >= 2);
    const raw = await catalog.object(pending.source);
    assert.ok(JSON.parse(raw.toString('utf8')).value.length > 0);
  } finally {
    await source.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test('继续领取：更正值进入可用集并注明依据，旧版本不套新定义', async () => {
  const source = await startSource({ scenario: 'transition' });
  const { directory, collector } = await makeCollector(source.base);
  const client = new SourceClient(source.base);
  try {
    await collector.collect('/Datastreams(7)/Observations');
    await client.control({ action: 'advance' });
    const result = await collector.collect('/Datastreams(7)/Observations');
    assert.equal(result.created, false);
    const { view } = result;
    // 观测 101 的更正值可用，并注明采用的属性、单位及传感器资料版本
    const reading = view.readings.find(item => item.observation === 101);
    assert.equal(reading.state, 'value');
    assert.equal(reading.value, 24.1);
    assert.equal(reading.definition, 'station-a:4');
    assert.equal(reading.revision, 2);
    assert.equal(reading.sensor, 'sensor-2');
    assert.deepEqual(reading.layout, { order: [10, 9], units: ['%', 'Cel'] });
    // 旧版本仍按 station-a:3 记录，没有被新定义覆盖
    const observation = view.observations.find(item => item.id === 101);
    assert.equal(observation.versions.length, 2);
    assert.equal(observation.versions[0].definition, 'station-a:3');
    assert.equal(observation.versions[0].basis, 'pending');
    assert.equal(observation.versions[1].definition, 'station-a:4');
    assert.equal(observation.versions[1].basis, 'ready');
    assert.equal(view.summary.corrections >= 1, true);
    assert.equal(view.summary.pending, 0);
    // 空值观测 102 也随新版本进入可用集
    const explicitNull = view.readings.find(item => item.observation === 102);
    assert.equal(explicitNull.state, 'explicitNull');
    assert.equal(explicitNull.definition, 'station-a:4');
  } finally {
    await source.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test('多维流领取：列序不代替属性身份，改版后旧版本仍按旧依据解释', async () => {
  const source = await startSource({ scenario: 'transition' });
  const { directory, collector } = await makeCollector(source.base);
  const client = new SourceClient(source.base);
  try {
    const first = await collector.collect('/MultiDatastreams(8)/Observations');
    // station-a:3 未公开：数值暂时缺失，空值仍是明确空值
    assert.equal(first.view.summary.usable, 0);
    assert.equal(first.view.summary.pending, 3);
    assert.equal(first.view.summary.explicitNull, 1);
    await client.control({ action: 'advance' });
    const second = await collector.collect('/MultiDatastreams(8)/Observations');
    const { view } = second;
    // 元数据顺序 [10, 9]：位置 0 是湿度，位置 1 是温度，按属性身份而不是列序解释
    const cells = view.readings.filter(item => item.observation === 201);
    const humidity = cells.find(item => item.property.id === 10);
    const temperature = cells.find(item => item.property.id === 9);
    assert.equal(humidity.value, 22.1);
    assert.equal(humidity.unit, '%');
    assert.equal(humidity.sensor, 'sensor-2');
    assert.equal(temperature.value, 61);
    assert.equal(temperature.unit, 'Cel');
    // 观测 202 的湿度列是明确空值
    const nullCell = view.readings.find(item => item.observation === 202 && item.property.id === 10);
    assert.equal(nullCell.state, 'explicitNull');
    assert.equal(nullCell.value, null);
    // 旧版本快照仍是 [9, 10]，新定义没有套到旧结果上
    const observation = view.observations.find(item => item.id === 201);
    assert.deepEqual(observation.versions[0].order, [9, 10]);
    assert.deepEqual(observation.versions[1].order, [10, 9]);
    assert.equal(observation.versions[0].basis, 'pending');
    assert.equal(observation.versions[1].basis, 'ready');
  } finally {
    await source.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test('解释版本后出现时，继续领取补齐既有观测依据且不产生新版本', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'collect-test-'));
  const catalog = await new Catalog(directory).initialize();
  try {
    const observation = { '@iot.id': 101, phenomenonTime: '2026-10-12T01:00:00Z', result: 23.4,
      Datastream: { '@iot.id': 7 }, parameters: { definition: 'station-a:3', revision: 1 } };
    let definitionAvailable = false;
    const client = { async request(path) {
      if (path.startsWith('/Datastreams(7)/Observations')) {
        return { status: 200, headers: { 'content-type': 'application/json' },
          bytes: Buffer.from(JSON.stringify({ value: [observation] })), url: path };
      }
      if (path === '/definitions/station-a:3') {
        if (!definitionAvailable) return { status: 404, headers: {}, bytes: Buffer.from('不存在'), url: path };
        return { status: 200, headers: { 'content-type': 'application/json' },
          bytes: Buffer.from(JSON.stringify({ order: [9], units: ['Cel'], sensor: 'sensor-1' })), url: path };
      }
      return { status: 404, headers: {}, bytes: Buffer.from(''), url: path };
    } };
    const collector = new Collector({ client, catalog, root: join(directory, 'collections') });
    const first = await collector.collect('/Datastreams(7)/Observations');
    assert.equal(first.view.summary.usable, 0);
    assert.equal(first.view.summary.pending, 1);
    definitionAvailable = true;
    const second = await collector.collect('/Datastreams(7)/Observations');
    assert.equal(second.view.summary.usable, 1);
    assert.equal(second.view.summary.pending, 0);
    const reading = second.view.readings[0];
    assert.equal(reading.value, 23.4);
    assert.equal(reading.sensor, 'sensor-1');
    const observationEntry = second.view.observations.find(item => item.id === 101);
    assert.equal(observationEntry.versions.length, 1);
    assert.equal(observationEntry.versions[0].basis, 'ready');
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test('空流与空值、暂时缺失区分：没有观测不等于缺资料', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'collect-test-'));
  const catalog = await new Catalog(directory).initialize();
  try {
    const client = { async request(path) {
      if (path.startsWith('/Datastreams(7)/Observations')) {
        return { status: 200, headers: { 'content-type': 'application/json' },
          bytes: Buffer.from(JSON.stringify({ value: [] })), url: path };
      }
      return { status: 404, headers: {}, bytes: Buffer.from(''), url: path };
    } };
    const collector = new Collector({ client, catalog, root: join(directory, 'collections') });
    const result = await collector.collect('/Datastreams(7)/Observations');
    assert.equal(result.view.empty, true);
    assert.equal(result.view.summary.observations, 0);
    assert.equal(result.view.summary.usable, 0);
    assert.equal(result.view.summary.explicitNull, 0);
    assert.equal(result.view.summary.pending, 0);
    assert.deepEqual(result.view.readings, []);
    assert.deepEqual(result.view.pending, []);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test('应用提供按观测流领取、列表与回读接口', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'collect-app-'));
  const source = await startSource({ scenario: 'transition' });
  const app = await startApplication({ source: source.base, root: directory });
  const client = new SourceClient(source.base);
  const post = path => fetch(`${app.base}/collections`, { method: 'POST',
    headers: { 'content-type': 'application/json' }, body: JSON.stringify({ path }) });
  try {
    const created = await post('/Datastreams(7)/Observations');
    assert.equal(created.status, 201);
    assert.equal((await created.json()).id, 'datastreams-7-observations');
    await client.control({ action: 'advance' });
    const continued = await post('/Datastreams(7)/Observations');
    assert.equal(continued.status, 200);
    const list = await fetch(`${app.base}/collections`).then(response => response.json());
    assert.deepEqual(list.map(item => item.id), ['datastreams-7-observations']);
    const view = await fetch(`${app.base}/collections/datastreams-7-observations`).then(response => response.json());
    assert.equal(view.summary.usable, 1);
    assert.equal(view.summary.corrections >= 1, true);
    const missing = await fetch(`${app.base}/collections/nope`);
    assert.equal(missing.status, 404);
    const badPath = await post('/observations/101');
    assert.equal(badPath.status, 422);
  } finally {
    await app.close();
    await source.close();
    await rm(directory, { recursive: true, force: true });
  }
});
