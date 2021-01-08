import { Table } from 'apache-arrow';
import { fromArrow, fromCSV, fromJSON } from 'arquero';

function error(message) {
  throw Error(message);
}

export default function(format, url, options) {
  switch (format) {
    case 'csv':
      return loadCSV(url, options);
    case 'json':
      return loadJSON(url, options);
    case 'arrow':
      return loadArrow(url, options);
    default:
      error(`Unsupported file format: ${JSON.stringify(format)}`);
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
  const table = await Table.from(fetch(url));
  return fromArrow(table, options);
}