export function dispatch({ url, phase }) {
  if (url.pathname === '/MultiDatastreams(8)') {
    const order = phase ? [10, 9] : [9, 10];
    return { json: { '@iot.id': 8, properties: { definition: `station-a:${phase + 3}` },
      unitOfMeasurements: order.map(id => ({ symbol: id === 9 ? 'Cel' : '%' })),
      multiObservationDataTypes: ['OM_Measurement', 'OM_Measurement'],
      ObservedProperties: order.map(id => ({ '@iot.id': id, name: id === 9 ? '温度' : '湿度' })) } };
  }
  if (url.pathname === '/Datastreams(7)/Observations') {
    const skip = Number(url.searchParams.get('$skip') || 0);
    if (![0, 1].includes(skip)) return { status: 400, json: { error: '不支持此页位置' } };
    return { json: { value: [{ '@iot.id': 101 + skip, phenomenonTime: '2026-10-12T01:00:00Z',
      result: skip ? null : phase ? 24.1 : 23.4, Datastream: { '@iot.id': 7 },
      parameters: { definition: `station-a:${phase + 3}`, revision: phase + 1 } }],
      ...(skip ? {} : { '@iot.nextLink': '/Datastreams(7)/Observations?$skip=1' }) } };
  }
  return null;
}
