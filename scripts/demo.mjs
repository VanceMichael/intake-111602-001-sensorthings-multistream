import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { startSource } from '../source/service.mjs';
import { startApplication } from '../src/app.mjs';

const post = async (base, path, body = {}) => {
  const response = await fetch(`${base}${path}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
  return { status: response.status, body: await response.json() };
};

const directory = await mkdtemp(join(tmpdir(), 'intake-'));
const source = await startSource({ scenario: 'intake' });
const app = await startApplication({ source: source.base, root: directory });
try {
  const preview = await fetch(`${app.base}/preview`).then(response => response.json());
  process.stdout.write(`当前资料预览\n${JSON.stringify(preview, null, 2)}\n`);
  const saved = await fetch(`${app.base}/receipts`, { method: 'POST', body: '{}' }).then(response => response.json());
  const body = await fetch(`${app.base}/objects/${saved.digest}`).then(response => response.arrayBuffer());
  if (body.byteLength !== saved.bytes) throw new Error('原始资料长度不符');
  process.stdout.write(`原件已接收：${saved.digest}，${saved.bytes} 字节\n\n`);

  process.stdout.write('— 首次按观测流领取（解释文档尚未发布，数值进入待补清单）—\n');
  const first = await post(app.base, '/intake', { kind: 'Datastream', streamId: 7 });
  process.stdout.write(`普通流：${first.status}，待补 ${first.body.views.pending.length} 条，可用 ${first.body.views.ready.length} 条\n`);
  const multi = await post(app.base, '/intake', { kind: 'MultiDatastream', streamId: 8 });
  process.stdout.write(`多结果流：${multi.status}，待补 ${multi.body.views.pending.length} 条；列身份示例 ${JSON.stringify(multi.body.views.pending[0].cells.map(cell => cell.propertyId))}\n\n`);

  process.stdout.write('— 推进发布日期：传感器换型、属性列重排、更正值与定义文档上线 —\n');
  await post(source.base, '/control', { action: 'advance' });
  const rerun = await post(app.base, '/intake/datastream-7/run');
  const corrected = rerun.body.views.ready.find(item => item.observationId === 101);
  process.stdout.write(`普通流继续领取：可用 ${rerun.body.views.ready.length} 条；观测 101 最新结果 ${corrected.reading.result}（${corrected.reading.definition}，传感器 ${corrected.reading.sensor}），更正 ${corrected.corrections} 次\n`);
  const multiRerun = await post(app.base, '/intake/multi-8/run');
  const row201 = multiRerun.body.views.ready.find(item => item.observationId === 201);
  process.stdout.write(`多结果流继续领取：可用 ${multiRerun.body.views.ready.length} 条；观测 201 湿度=${row201.readings.find(cell => cell.propertyId === 10).value}，温度=${row201.readings.find(cell => cell.propertyId === 9).value}（按属性身份而非列序）\n`);
  process.stdout.write(`任务清单：${(await fetch(`${app.base}/intake`).then(r => r.json())).jobs.map(job => `${job.slug}:${job.counts.ready}/${job.counts.observations}`).join('，')}\n`);
} finally {
  await app.close();
  await source.close();
  await rm(directory, { recursive: true, force: true });
}
