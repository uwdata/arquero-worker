import { from, seed as setSeed, table } from 'arquero';
import Query from '../query/query';
import Database from './database';
import load from './load';

export async function handleMessage(request, response) {
  const { method, params } = request;
  if (handlers.has(method)) {
    try {
      const handler = handlers.get(method);
      response.send(await handler(params));
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

function insert(name, dt, append = false) {
  if (append) {
    db.append(name, dt);
  } else {
    db.add(name, dt);
  }
  return { type: 'table', table: name };
}

function transfer(dt, options) {
  return { type: 'data', data: dt.toJSON(options) };
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
// type: enum(columns, rows)
// append: boolean
// data: any
function onAdd({ name, type, append, data }) {
  const dt = type === 'rows' ? from(data) : table(data);
  return insert(name, dt, append);
}

// load table into catalog
// name: string
// type: enum(csv, json, arrow)
// append: boolean
// options: object
//   arrow: { columns, unpack }
//   csv: { delimiter, header, autoType, parse? }
//   json: { autoType, parse? }
async function onLoad({ name, append, url, type, options }) {
  const dt = await load(type, url, options);
  return insert(name, dt, append);
}

// query table in catalog
// name: string
// query: serialized Query
// as: string
function onQuery({ name, query, as, options }) {
  const dt = db.query(name, Query.from(query));
  return as ? insert(as, dt) : transfer(dt, options);
}

// fetch table data
// name: string
// columns: select-compatible
// rows: array or boolean
// type: enum(columns, rows)
function onFetch({ name, options }) {
  const dt = db.get(name);
  return transfer(dt, options);
}