import { Table } from 'apache-arrow';
import { fromArrow, fromJSON } from 'arquero';
import WorkerQuery from './worker-query';
import workerThread from './worker-thread';

export default class WorkerClient {
  constructor(workerSource, options = {}) {
    this._worker = workerThread(workerSource);
    this._format = options.format || 'json';
  }

  post(request, transfer) {
    return this._worker.post(request, transfer);
  }

  terminate() {
    this._worker.terminate();
  }

  async list() {
    const resp = await this.post({
      method: 'list'
    });
    return resp.list;
  }

  async seed(seed) {
    const resp = await this.post({
      method: 'seed',
      params: { seed }
    });
    return resp.seed;
  }

  async drop(name) {
    if (name && name.tableName && !name.length) {
      name = name.tableName();
    }
    const resp = await this.post({
      method: 'drop',
      params: { name }
    });
    return resp.drop;
  }

  async table(name, data) {
    if (data) {
      const transfer = ArrayBuffer.isView(data) ? [data] : undefined;
      const resp = await this.post({
        method: 'add',
        params: { name, data }
      }, transfer);
      name = resp.table;
    }
    return WorkerQuery.for(name, this);
  }

  async load(name, url, format = 'csv', options = {}, append = false) {
    const resp = await this.post({
      method: 'load',
      params: { name, url, format, options, append }
    });
    return WorkerQuery.for(resp.table, this);
  }

  async query(query, options, as) {
    const format = this._format;
    const resp = await this.post({
      method: 'query',
      params: { query, as, format, options }
    });
    return as
      ? WorkerQuery.for(resp.table, this)
      : decodeTable(resp.data);
  }

  async fetch(name, options) {
    const format = this._format;
    const resp = await this.post({
      method: 'fetch',
      params: { name, format, options }
    });
    return decodeTable(resp.data);
  }
}

function decodeTable(data) {
  return ArrayBuffer.isView(data) ? fromArrow(Table.from(data), { unpack: true })
    : typeof data === 'string' ? fromJSON(data)
    : error('Unrecognized table data format');
}

function error(msg) {
  throw Error(msg);
}