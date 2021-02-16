import { loadArrow, loadCSV, loadJSON } from 'arquero';

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