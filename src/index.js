import QueryWorker from './client/query-worker';

export * from 'arquero';
export function worker(source) {
  return new QueryWorker(source);
}