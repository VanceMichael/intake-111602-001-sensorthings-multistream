import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { startSource } from '../source/service.mjs';
import { startApplication } from '../src/app.mjs';
import { Catalog } from '../src/catalog.mjs';

async function postJson(base, path, body) {
  const response = await fetch(`${base}${path}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body || {}),
  });
  return { status: response.status, body: await response.json() };
}

const getJson = async (base, path) => {
  const response = await fetch(`${base}${path}`);
  return { status: response.status, body: await response.json() };
};

test('端到端：普通流按页领取，定义补齐后可用并追踪更正，空流与空值可区分', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'intake-e2e-ds-'));
  const source = await startSource({ scenario: 'intake' });
  const app = await startApplication({ source: source.base, root: directory });
  try {
    const created = await postJson(app.base, '/intake', { kind: 'Datastream', streamId: 7 });
    assert.equal(created.status, 201);
    assert.equal(created.body.job.counts.observations, 2);
    assert.equal(created.body.job.counts.ready, 0);
    assert.equal(created.body.views.pending.length, 2);
    // 缺解释依据的数值不进可用集。
    assert.deepEqual(created.body.views.ready, []);

    // 空流是明确的“没有观测”，与 null 取值不同。
    const empty = await postJson(app.base, '/intake', { kind: 'Datastream', streamId: 12 });
    assert.equal(empty.status, 201);
    assert.equal(empty.body.views.empty, true);
    assert.equal(empty.body.job.counts.observations, 0);

    // 重复创建被拒绝，必须继续领取同一任务。
    const duplicate = await postJson(app.base, '/intake', { kind: 'Datastream', streamId: 7 });
    assert.equal(duplicate.status, 409);

    // 推进发布日期：更正值 + 新定义 + 旧定义文档上线。
    await fetch(`${source.base}/control`, {
      method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ action: 'advance' }),
    });
    const continued = await postJson(app.base, '/intake/datastream-7/run');
    assert.equal(continued.status, 200);
    assert.equal(continued.body.views.pending.length, 0);
    assert.equal(continued.body.views.ready.length, 2);
    const corrected = continued.body.views.ready.find(item => item.observationId === 101);
    assert.equal(corrected.reading.result, 24.1);
    assert.equal(corrected.reading.definition, 'station-a:4');
    assert.equal(corrected.reading.sensor, 'sensor-2');
    assert.equal(corrected.corrections, 1);
    assert.deepEqual(corrected.history.map(item => item.definition), ['station-a:3', 'station-a:4']);
    const explicitNull = continued.body.views.ready.find(item => item.observationId === 102);
    assert.equal(explicitNull.reading.valueKind, 'null');
    assert.equal(continued.body.views.counts.explicitNulls, 1);

    // 重开目录后任务与原件仍在，可继续追踪。
    const reopened = await startApplication({ source: source.base, root: directory });
    try {
      const listed = await getJson(reopened.base, '/intake');
      assert.equal(listed.body.jobs.length, 2);
      const detail = await getJson(reopened.base, '/intake/datastream-7/observations?status=ready');
      assert.equal(detail.body.ready.length, 2);
      const digest = corrected.history[0].sourceDigest;
      const original = await getJson(reopened.base, `/objects/${digest}`).then(r => r.status);
      assert.equal(original, 200);
    } finally {
      await reopened.close();
    }
  } finally {
    await app.close();
    await source.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test('端到端：MultiDatastream 按属性身份解释，重排后旧结果不被新定义反套', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'intake-e2e-multi-'));
  const source = await startSource({ scenario: 'intake' });
  const app = await startApplication({ source: source.base, root: directory });
  try {
    const created = await postJson(app.base, '/intake', { kind: 'MultiDatastream', streamId: 8 });
    assert.equal(created.status, 201);
    // 第一版定义文档尚未发布，全部待补；但属性身份已按首份快照解析。
    assert.equal(created.body.views.pending.length, 3);
    const row201 = created.body.views.pending.find(item => item.observationId === 201);
    assert.deepEqual(row201.cells.map(cell => cell.propertyId), [9, 10]);
    assert.equal(row201.cells[0].value, 22.1);
    // 203 缺第二列：缺席列显式标记为暂时缺失，而不是当作空值。
    const row203 = created.body.views.pending.find(item => item.observationId === 203);
    const absent = row203.cells.find(cell => cell.valueKind === 'absent');
    assert.equal(absent.propertyId, 10);
    assert.equal(absent.status, 'pending');
    assert.match(row203.reasons.join(' '), /暂时缺失的列/);
    // 202 第一列是明确 null。
    const row202 = created.body.views.pending.find(item => item.observationId === 202);
    assert.equal(row202.raw.cells[0].value, null);

    await fetch(`${source.base}/control`, {
      method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ action: 'advance' }),
    });
    const continued = await postJson(app.base, '/intake/multi-8/run');
    assert.equal(continued.status, 200);
    assert.equal(continued.body.views.pending.length, 0);
    const latest201 = continued.body.views.ready.find(item => item.observationId === 201);
    // 新快照位置 0 是湿度(10)，新行的 22.4 必须落在湿度上。
    assert.equal(latest201.readings.find(cell => cell.propertyId === 10).value, 22.4);
    assert.equal(latest201.readings.find(cell => cell.propertyId === 9).value, 61);
    assert.equal(latest201.readings[0].sensor, 'sensor-2');
    assert.equal(latest201.corrections, 1);

    // 旧版本仍按 station-a:3 解释：22.1 是温度(9)，没有被重排后的新列序反套。
    const state = await new Catalog(directory).readIntake('multi-8');
    const oldVersion = state.observations[201].versions[0];
    assert.equal(oldVersion.boundDefinition, 'station-a:3');
    assert.equal(oldVersion.cells[0].value, 22.1);
    const oldSnapshot = state.contexts['station-a:3'].snapshot;
    assert.deepEqual(oldSnapshot.order, [9, 10]);
    assert.deepEqual(state.contexts['station-a:4'].snapshot.order, [10, 9]);
  } finally {
    await app.close();
    await source.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test('端到端：首次领取断连不丢已收状态，来源恢复后继续补齐', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'intake-e2e-fail-'));
  const source = await startSource({ scenario: 'intake' });
  const app = await startApplication({ source: source.base, root: directory });
  try {
    const failed = await postJson(app.base, '/intake', { kind: 'Datastream', streamId: 13 });
    assert.equal(failed.status, 422);
    assert.match(failed.body.error, /观测资料领取失败|terminated|socket|网络|断开/);
    const summary = failed.body.job;
    assert.equal(summary.runs[0].outcome, 'failed');

    await fetch(`${source.base}/control`, {
      method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ action: 'advance' }),
    });
    const recovered = await postJson(app.base, '/intake/datastream-13/run');
    assert.equal(recovered.status, 200);
    assert.equal(recovered.body.views.ready.length, 1);
    assert.equal(recovered.body.views.ready[0].reading.result, 7.5);
    assert.equal(recovered.body.views.ready[0].reading.sensor, 'sensor-2');
    assert.equal(recovered.body.job.runs.length, 2);
  } finally {
    await app.close();
    await source.close();
    await rm(directory, { recursive: true, force: true });
  }
});
