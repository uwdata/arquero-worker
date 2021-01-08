import { Table } from 'apache-arrow';
import { from, fromArrow, fromJSON, queryFrom, seed as setSeed, table } from 'arquero';
import { toArrow } from 'arquero-arrow';
import Database from './database';
import load from './load';

export async function handleMessage(request, response) {
  const { method, params } = request;
  if (handlers.has(method)) {
    try {
      const handler = handlers.get(method);
      const message = await handler(params);
      const trans = ArrayBuffer.isView(message.data)
        ? [message.data.buffer]
        : null;
      response.send(message, trans);
    } catch (err) {
      response.error(err);
    }
  } else {
    response.error(`Unknown method: ${method}`);
  }
}

const db = new Database();

const handlers = new Map()
  .set('add',   onAdd)
  .set('drop',  onDrop)
  .set('fetch', onFetch)
  .set('list',  onList)
  .set('load',  onLoad)
  .set('query', onQuery)
  .set('seed',  onSeed);

function decodeTable(data) {
  const type = typeof data;
  return ArrayBuffer.isView(data) ? fromArrow(Table.from(data))
    : Array.isArray(data) ? from(data)
    : type === 'string' ? fromJSON(data)
    : table(data);
}

// options: limit, offset, columns, format
function encodeTable(table, format, options = {}) {
  switch (format) {
    case 'arrow':
      return toArrow(table).serialize(); // TODO handle options
    case 'json':
    default:
      return table.toJSON(options);
  }
}

function insert(name, dt, append = false) {
  if (append) {
    db.append(name, dt);
  } else {
    db.add(name, dt);
  }
  return { type: 'table', table: name };
}

function transfer(dt, format, options) {
  return { type: 'data', data: encodeTable(dt, format, options) };
}

function onSeed({ seed }) {
  setSeed(seed);
  return { type: 'seed', seed };
}

function onList() {
  return { type: 'list', list: db.list() };
}

// drop table from catalog
// name: string
function onDrop({ name }) {
  return { type: 'table', name, drop: db.drop(name) };
}

// add table to catalog
// name: string
// append: boolean
// data: any
function onAdd({ name, append, data }) {
  const dt = decodeTable(data);
  return insert(name, dt, append);
}

// load table into catalog
// name: string
// append: boolean
// format: enum(csv, json, arrow)
// options: object
//   arrow: { columns, unpack }
//   csv: { delimiter, header, autoType, parse? }
//   json: { autoType, parse? }
async function onLoad({ name, append, url, format, options }) {
  const dt = await load(format, url, options);
  return insert(name, dt, append);
}

// query table in catalog
// name: string
// query: serialized Query
// as: string
function onQuery({ query, as, format, options }) {
  const dt = db.query(query.name, queryFrom(query));
  return as ? insert(as, dt) : transfer(dt, format, options);
}

// fetch table data
// name: string
// columns: select-compatible
function onFetch({ name, format, options }) {
  const dt = db.get(name);
  return transfer(dt, format, options);
}