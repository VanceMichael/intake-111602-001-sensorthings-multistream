import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { startSource } from '../source/service.mjs';
import { startApplication } from '../src/app.mjs';
import { SourceClient } from '../src/transport.mjs';

const directory = await mkdtemp(join(tmpdir(), 'intake-'));
const source = await startSource({ scenario: 'transition' });
const app = await startApplication({ source: source.base, root: directory });
const control = new SourceClient(source.base);
const post = (path, body) => fetch(`${app.base}${path}`, { method: 'POST',
  headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) }).then(response => response.json());
const cellText = reading => reading.state === 'explicitNull'
  ? `${reading.property?.name ?? '读数'}=空`
  : `${reading.property?.name ?? '读数'}=${reading.value}${reading.unit || ''}`;
try {
  const preview = await fetch(`${app.base}/preview`).then(response => response.json());
  process.stdout.write(`当前资料预览\n${JSON.stringify(preview, null, 2)}\n`);
  const saved = await post('/receipts', {});
  const body = await fetch(`${app.base}/objects/${saved.digest}`).then(response => response.arrayBuffer());
  if (body.byteLength !== saved.bytes) throw new Error('原始资料长度不符');
  process.stdout.write(`原件已接收：${saved.digest}，${saved.bytes} 字节\n`);

  // 按观测流领取：解释版本 station-a:3 尚未公开，数值暂时缺失，原文留存
  const first = await post('/collections', { path: '/Datastreams(7)/Observations' });
  const firstMulti = await post('/collections', { path: '/MultiDatastreams(8)/Observations' });
  process.stdout.write('首次领取（station-a:3 未公开）：\n');
  process.stdout.write(`  普通流：可用 ${first.view.summary.usable}，明确空值 ${first.view.summary.explicitNull}，暂时缺失 ${first.view.summary.pending}\n`);
  process.stdout.write(`  多维流：可用 ${firstMulti.view.summary.usable}，明确空值 ${firstMulti.view.summary.explicitNull}，暂时缺失 ${firstMulti.view.summary.pending}\n`);

  // 站点推进发布日期：观测 101 带上更正值，station-a:4 公开
  await control.control({ action: 'advance' });
  const second = await post('/collections', { path: '/Datastreams(7)/Observations' });
  const secondMulti = await post('/collections', { path: '/MultiDatastreams(8)/Observations' });
  const reading = second.view.readings.find(item => item.observation === 101);
  process.stdout.write('继续领取（station-a:4 公开，观测 101 更正）：\n');
  process.stdout.write(`  普通流：观测 101 = ${reading.value}（${reading.definition}，${reading.sensor}），追踪更正 ${second.view.summary.corrections} 条\n`);
  process.stdout.write(`  多维流：${secondMulti.view.readings.map(cellText).join('，')}\n`);
} finally {
  await app.close();
  await source.close();
  await rm(directory, { recursive: true, force: true });
}
