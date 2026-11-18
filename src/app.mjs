import { createServer } from 'node:http';
import { fileURLToPath } from 'node:url';
import { Catalog } from './catalog.mjs';
import { decode } from './decode.mjs';
import { SourceClient } from './transport.mjs';
import { profile } from './profile.mjs';
import { buildViews, createState, defaultObservationsPath, IntakeJob, summarize } from './intake.mjs';

export async function previewOne(base, path = profile.previewPath) {
  const result = await new SourceClient(base).request(path);
  if (result.status !== 200) throw new Error(`来源响应：${result.status}`);
  return { ...result, preview: decode(result.bytes, result.headers) };
}

const SLUG_PATTERN = /^(datastream|multi)-(\d+)$/;

async function readJsonBody(request) {
  const chunks = [];
  for await (const chunk of request) chunks.push(chunk);
  const raw = Buffer.concat(chunks).toString('utf8');
  if (!raw) return {};
  return JSON.parse(raw);
}

export function createIntakeHandler({ catalog, source }) {
  return async function intakeRoute(request, url) {
    const segments = url.pathname.split('/').filter(Boolean); // ['intake', ...]
    const client = new SourceClient(source);

    if (segments.length === 1 && request.method === 'GET') {
      const jobs = await catalog.listIntake();
      return { status: 200, body: { jobs: jobs.map(summarize) } };
    }

    if (segments.length === 1 && request.method === 'POST') {
      const input = await readJsonBody(request);
      const kind = input.kind;
      const streamId = Number(input.streamId);
      if (!['Datastream', 'MultiDatastream'].includes(kind) || !Number.isInteger(streamId)) {
        return { status: 422, body: { error: '需要有效的 kind（Datastream/MultiDatastream）与 streamId' } };
      }
      const state = createState({
        kind,
        streamId,
        observationsPath: input.observationsPath || defaultObservationsPath(kind, streamId),
      });
      const existing = await catalog.readIntake(state.slug);
      if (existing) return { status: 409, body: { error: `观测流 ${state.slug} 已有采集任务，请对其继续领取`, slug: state.slug } };
      const job = new IntakeJob({ catalog, client, state });
      let body;
      try {
        await job.run();
        body = { job: summarize(state), views: buildViews(state) };
      } catch (error) {
        body = { job: summarize(state), error: error.message };
      }
      return { status: body.error ? 422 : 201, body };
    }

    if (!segments[1]?.match(SLUG_PATTERN)) return { status: 404, body: { error: '没有此采集任务' } };
    const slug = segments[1];
    const state = await catalog.readIntake(slug);
    if (!state) return { status: 404, body: { error: '采集任务不存在' } };
    const sub = segments[2];

    if (!sub && request.method === 'GET') {
      return { status: 200, body: { job: summarize(state), views: buildViews(state) } };
    }
    if (sub === 'run' && request.method === 'POST') {
      const job = new IntakeJob({ catalog, client, state });
      let body;
      try {
        await job.run();
        body = { job: summarize(state), views: buildViews(state) };
      } catch (error) {
        body = { job: summarize(state), error: error.message };
      }
      return { status: body.error ? 422 : 200, body };
    }
    if (sub === 'observations' && request.method === 'GET') {
      const views = buildViews(state);
      const include = url.searchParams.get('status');
      if (include === 'ready') return { status: 200, body: { ...views, pending: undefined } };
      if (include === 'pending') return { status: 200, body: { ...views, ready: undefined } };
      return { status: 200, body: views };
    }
    return { status: 404, body: { error: '没有此采集操作' } };
  };
}

export async function startApplication({ source, root, port = 0 }) {
  const catalog = await new Catalog(root).initialize();
  const handleIntake = createIntakeHandler({ catalog, source });
  const server = createServer(async (request, response) => {
    response.setHeader('content-type', 'application/json; charset=utf-8');
    try {
      const url = new URL(request.url, 'http://localhost');
      if (url.pathname === '/health') return response.end(JSON.stringify({ ready: true, product: profile.name }));
      if (request.method === 'GET' && url.pathname === '/preview') {
        const result = await previewOne(source, url.searchParams.get('path') || profile.previewPath);
        return response.end(JSON.stringify({ source: result.url, preview: result.preview }));
      }
      if (request.method === 'POST' && url.pathname === '/receipts') {
        const chunks = [];
        for await (const chunk of request) chunks.push(chunk);
        const input = JSON.parse(Buffer.concat(chunks));
        const result = await previewOne(source, input.path || profile.previewPath);
        const receipt = await catalog.accept({ source: result.url, bytes: result.bytes,
          mediaType: result.headers['content-type'], preview: result.preview });
        response.statusCode = 201;
        return response.end(JSON.stringify(receipt));
      }
      if (request.method === 'GET' && url.pathname === '/receipts') return response.end(JSON.stringify(await catalog.list()));
      if (url.pathname === '/intake' || url.pathname.startsWith('/intake/')) {
        const answer = await handleIntake(request, url);
        response.statusCode = answer.status;
        return response.end(JSON.stringify(answer.body));
      }
      if (request.method === 'GET' && url.pathname.startsWith('/objects/')) {
        response.setHeader('content-type', 'application/octet-stream');
        return response.end(await catalog.object(url.pathname.split('/').at(-1)));
      }
      response.statusCode = 404;
      response.end(JSON.stringify({ error: '没有此操作' }));
    } catch (error) {
      response.statusCode = 422;
      response.end(JSON.stringify({ error: error.message }));
    }
  });
  await new Promise(resolve => server.listen(port, '127.0.0.1', resolve));
  return { base: `http://127.0.0.1:${server.address().port}`, catalog,
    close: () => new Promise(resolve => { server.closeAllConnections(); server.close(resolve); }) };
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const app = await startApplication({ source: process.env.SOURCE_URL || 'http://127.0.0.1:4310',
    root: process.env.DATA_DIR || './var', port: Number(process.env.PORT || 4320) });
  process.stdout.write(`资料查看服务已启动：${app.base}\n`);
  for (const signal of ['SIGINT', 'SIGTERM']) process.once(signal, async () => { await app.close(); process.exit(0); });
}
