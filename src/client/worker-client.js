import WorkerQuery from './worker-query';
import workerThread from './worker-thread';
import { fromJSON } from 'arquero';

export default class WorkerClient {
  constructor(workerSource) {
    this._worker = workerThread(workerSource);
  }

  post(request) {
    return this._worker.post(request);
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
      const resp = await this.post({
        method: 'add',
        params: { name, data }
      });
      name = resp.table;
    }
    return WorkerQuery.for(name, this);
  }

  async load(name, url, type = 'csv', options = {}, append = false) {
    const resp = await this.post({
      method: 'load',
      params: { name, url, type, options, append }
    });
    return WorkerQuery.for(resp.table, this);
  }

  async query(query, options, as) {
    const resp = await this.post({
      method: 'query',
      params: { query, as, options }
    });
    return as
      ? WorkerQuery.for(resp.table, this)
      : fromJSON(resp.data);
  }

  async fetch(name, options) {
    const resp = await this.post({
      method: 'fetch',
      params: { name, options }
    });
    return fromJSON(resp.data);
  }
}