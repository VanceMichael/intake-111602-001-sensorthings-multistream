import test from 'node:test';
import assert from 'node:assert/strict';
import { startSource } from '../source/service.mjs';
import { SourceClient } from '../src/transport.mjs';
import { decode } from '../src/decode.mjs';

test('观测结果与解释目录分别发布，改版会交换属性位置', async () => {
  const source = await startSource({ scenario: 'transition' });
  try {
    const client = new SourceClient(source.base);
    const first = JSON.parse((await client.request('/MultiDatastreams(8)')).bytes);
    assert.deepEqual(first.ObservedProperties.map(item => item['@iot.id']), [9, 10]);
    await client.control({ action: 'advance' });
    const changed = JSON.parse((await client.request('/MultiDatastreams(8)')).bytes);
    assert.deepEqual(changed.ObservedProperties.map(item => item['@iot.id']), [10, 9]);
    assert.notEqual(changed.properties.definition, first.properties.definition);
    const page = JSON.parse((await client.request('/Datastreams(7)/Observations?$skip=1')).bytes);
    assert.equal(page.value[0].result, null);
    assert.equal(page['@iot.nextLink'], undefined);
  } finally { await source.close(); }
});
