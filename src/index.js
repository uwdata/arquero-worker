import WorkerClient from './client/worker-client';

export * from 'arquero';
export * from 'arquero-arrow';
export function worker(source, options) {
  return new WorkerClient(source, options);
}