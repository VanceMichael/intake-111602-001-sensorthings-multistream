function document(bytes) {
  const value = JSON.parse(bytes.toString('utf8'));
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('资料不是对象');
  return value;
}

export function decode(bytes) {
  const value = document(bytes);
  if (!('@iot.id' in value) || !('result' in value)) throw new Error('缺少观测身份或结果');
  const stream = value.Datastream || value.MultiDatastream;
  if (!stream || !('@iot.id' in stream)) throw new Error('没有观测流依据');
  return { observation: value['@iot.id'], phenomenonTime: value.phenomenonTime,
    result: value.result, stream: stream['@iot.id'], definition: value.parameters?.definition || null };
}
