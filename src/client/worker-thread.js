import { ERROR, RESULT } from '../worker/constants';

const DEFAULT_SOURCE = () => __dirname + '/arquero-node-worker.js';

function onMessage(data, requests) {
  const { status, request } = data;
  const id = request.id;
  if (requests.has(id)) {
    const { resolve, reject } = requests.get(id);
    switch (status) {
      case RESULT:
        resolve(data.result);
        break;
      case ERROR:
        reject(data.error);
        break;
      default:
        // do nothing
    }
    requests.delete(id);
  }
}

export default function(source) {
  let REQUEST_ID = 0;
  let worker;

  const requests = new Map();

  if (typeof Worker === 'undefined') {
    // no global worker, assume we are in node.js
    const { Worker } = require('worker_threads');
    worker = new Worker(source || DEFAULT_SOURCE());
    worker.on('message', data => onMessage(data, requests));
  } else {
    // use web worker API
    worker = new Worker(source);
    worker.onmessage = event => onMessage(event.data, requests);
  }

  worker.post = (data, transfer) => {
    const id = ++REQUEST_ID;
    worker.postMessage({ id, ...data }, transfer);
    return new Promise((resolve, reject) => {
      // TODO: handle timeout?
      requests.set(id, { resolve, reject });
    });
  };

  return worker;
}