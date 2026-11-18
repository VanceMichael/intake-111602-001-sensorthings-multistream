// SensorThings 观测资料的纯解析逻辑：只认字段身份（components / @iot.id /
// parameters.definition），不以数组位置代替观测属性身份。

export function parseJson(bytes) {
  const value = JSON.parse(bytes.toString('utf8'));
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('资料不是对象');
  return value;
}

function requireId(value, label) {
  if (!value || typeof value !== 'object' || !('@iot.id' in value)) throw new Error(`${label}缺少 @iot.id`);
  return value['@iot.id'];
}

/**
 * 解析普通 Datastream 的 Observations 分页文档。
 * 每条观测必须自带 @iot.id、result 以及解释版本引用 parameters.definition。
 */
export function parseDatastreamPage(bytes) {
  const doc = parseJson(bytes);
  if (!Array.isArray(doc.value)) throw new Error('普通流分页缺少 value 数组');
  const observations = doc.value.map(item => {
    if (!item || typeof item !== 'object') throw new Error('普通流观测不是对象');
    const observationId = requireId(item, '观测');
    if (!('result' in item)) throw new Error(`观测 ${observationId} 缺少 result`);
    const stream = item.Datastream;
    const streamId = stream ? requireId(stream, 'Datastream') : null;
    const definition = item.parameters?.definition ?? null;
    return {
      observationId,
      phenomenonTime: item.phenomenonTime ?? null,
      result: item.result,
      definition,
      streamId,
      embedded: stream && typeof stream === 'object'
        ? {
            unit: stream.unitOfMeasurement && typeof stream.unitOfMeasurement === 'object'
              ? stream.unitOfMeasurement
              : null,
            observedProperty: stream.ObservedProperty && typeof stream.ObservedProperty === 'object'
              ? stream.ObservedProperty
              : null,
          }
        : { unit: null, observedProperty: null },
    };
  });
  return { observations, nextLink: typeof doc['@iot.nextLink'] === 'string' ? doc['@iot.nextLink'] : null };
}

/**
 * 解析 MultiDatastream 的 dataArray 文档。
 * components 决定每一列的字段名，绝不假定列的固定位置。
 */
export function parseDataArrayPage(bytes) {
  const doc = parseJson(bytes);
  if (!Array.isArray(doc.value)) throw new Error('dataArray 文档缺少 value 数组');
  const blocks = doc.value.map((block, blockIndex) => {
    if (!block || !Array.isArray(block.components) || !Array.isArray(block.dataArray)) {
      throw new Error(`dataArray 第 ${blockIndex} 段结构不完整`);
    }
    const columns = new Map();
    block.components.forEach((name, index) => {
      if (columns.has(name)) throw new Error(`dataArray 第 ${blockIndex} 段出现重复列 ${name}`);
      columns.set(name, index);
    });
    if (!columns.has('id') || !columns.has('result')) {
      throw new Error(`dataArray 第 ${blockIndex} 段缺少 id 或 result 列`);
    }
    const idColumn = columns.get('id');
    const resultColumn = columns.get('result');
    const timeColumn = columns.get('phenomenonTime');
    const rows = block.dataArray.map((row, rowIndex) => {
      if (!Array.isArray(row)) throw new Error(`dataArray 第 ${blockIndex} 段第 ${rowIndex} 行不是数组`);
      const observationId = row[idColumn];
      if (observationId === undefined || observationId === null) {
        throw new Error(`dataArray 第 ${blockIndex} 段第 ${rowIndex} 行缺少观测身份`);
      }
      const result = row[resultColumn];
      if (!Array.isArray(result)) throw new Error(`观测 ${observationId} 的 result 不是结果数组`);
      return {
        observationId,
        phenomenonTime: timeColumn === undefined ? null : (row[timeColumn] ?? null),
        // 以列索引登记结果，身份解析交给 intake 阶段的流元数据快照。
        indexedResults: result.map((value, index) => ({ index, value })),
        navigationLink: block['MultiDatastream@iot.navigationLink'] ?? null,
      };
    });
    return { rows };
  });
  return { blocks, nextLink: typeof doc['@iot.nextLink'] === 'string' ? doc['@iot.nextLink'] : null };
}

/**
 * 解析 MultiDatastream 元数据。ObservedProperties / unitOfMeasurements 的
 * 有序集合只对该 properties.definition 版本有效；换版重排后是另一份快照。
 */
export function parseMultiStream(bytes) {
  const doc = parseJson(bytes);
  const streamId = requireId(doc, 'MultiDatastream');
  const definition = doc.properties?.definition ?? null;
  const observedProperties = Array.isArray(doc.ObservedProperties) ? doc.ObservedProperties : [];
  const units = Array.isArray(doc.unitOfMeasurements) ? doc.unitOfMeasurements : [];
  const dataTypes = Array.isArray(doc.multiObservationDataTypes) ? doc.multiObservationDataTypes : [];
  const columns = observedProperties.map((property, index) => {
    if (!property || typeof property !== 'object' || !('@iot.id' in property)) {
      throw new Error(`MultiDatastream ${streamId} 第 ${index} 个 ObservedProperty 缺少身份`);
    }
    return {
      propertyId: property['@iot.id'],
      name: property.name ?? null,
      unit: units[index] && typeof units[index] === 'object' ? units[index] : null,
      dataType: dataTypes[index] ?? null,
    };
  });
  return { streamId, definition, columns, order: columns.map(column => column.propertyId) };
}

/** 解析站点公开的解释版本文档（属性次序、单位、传感器）。 */
export function parseDefinition(bytes) {
  const doc = parseJson(bytes);
  return {
    order: Array.isArray(doc.order) ? [...doc.order] : null,
    units: Array.isArray(doc.units) ? [...doc.units] : null,
    sensor: doc.sensor ?? null,
  };
}
