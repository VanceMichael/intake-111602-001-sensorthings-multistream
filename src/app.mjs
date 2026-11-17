import { createServer } from 'node:http';
import { fileURLToPath } from 'node:url';
import { join } from 'node:path';
import { Catalog } from './catalog.mjs';
import { decode } from './decode.mjs';
import { SourceClient } from './transport.mjs';
import { profile } from './profile.mjs';
import { Collector } from './collect.mjs';

export async function previewOne(base, path = profile.previewPath) {
  const result = await new SourceClient(base).request(path);
  if (result.status !== 200) throw new Error(`来源响应：${result.status}`);
  return { ...result, preview: decode(result.bytes, result.headers) };
}

export async function startApplication({ source, root, port = 0 }) {
  const catalog = await new Catalog(root).initialize();
  const collector = new Collector({ client: new SourceClient(source), catalog, root: join(root, 'collections') });
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
      if (request.method === 'POST' && url.pathname === '/collections') {
        const chunks = [];
        for await (const chunk of request) chunks.push(chunk);
        const text = Buffer.concat(chunks).toString('utf8').trim();
        const input = text ? JSON.parse(text) : {};
        if (!input.path) {
          response.statusCode = 400;
          return response.end(JSON.stringify({ error: '缺少观测流路径 path' }));
        }
        const result = await collector.collect(input.path);
        response.statusCode = result.created ? 201 : 200;
        return response.end(JSON.stringify(result));
      }
      if (request.method === 'GET' && url.pathname === '/collections') {
        return response.end(JSON.stringify(await collector.list()));
      }
      if (request.method === 'GET' && url.pathname.startsWith('/collections/')) {
        const view = await collector.view(decodeURIComponent(url.pathname.slice('/collections/'.length)));
        if (!view) {
          response.statusCode = 404;
          return response.end(JSON.stringify({ error: '没有此观测流领取记录' }));
        }
        return response.end(JSON.stringify(view));
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
  return { base: `http://127.0.0.1:${server.address().port}`, catalog, collector,
    close: () => new Promise(resolve => { server.closeAllConnections(); server.close(resolve); }) };
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const app = await startApplication({ source: process.env.SOURCE_URL || 'http://127.0.0.1:4310',
    root: process.env.DATA_DIR || './var', port: Number(process.env.PORT || 4320) });
  process.stdout.write(`资料查看服务已启动：${app.base}\n`);
  for (const signal of ['SIGINT', 'SIGTERM']) process.once(signal, async () => { await app.close(); process.exit(0); });
}
