import { fromArrow, fromCSV, fromJSON } from 'arquero';

function error(message) {
  throw Error(message);
}

export default function(type, url, options) {
  switch (type) {
    case 'csv':
      return loadCSV(url, options);
    case 'json':
      return loadJSON(url, options);
    case 'arrow':
      return loadArrow(url, options);
    default:
      error(`Unrecognized file type: "${type}"`);
  }
}

async function loadCSV(url, options) {
  const data = await fetch(url).then(res => res.text());
  return fromCSV(data, options);
}

async function loadJSON(url, options) {
  const data = await fetch(url).then(res => res.text());
  return fromJSON(data, options);
}

async function loadArrow(url, options) {
  if (typeof Arrow !== 'undefined') {
    const buf = await fetch(url).then(res => res.arrayBuffer());
    // eslint-disable-next-line no-undef
    const table = Arrow.Table.from([new Uint8Array(buf)]);
    return fromArrow(table, options);
  } else {
    error('Apache Arrow has not been imported.');
  }
}