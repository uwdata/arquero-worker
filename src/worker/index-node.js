import { ERROR, RESULT } from './constants';
import { handleMessage } from './handler';

const { parentPort } = require('worker_threads');

parentPort.on('message', request => {
  handleMessage(request, response(request));
});

const response = req => {
  const request = {
    id: req.id,
    method: req.method
  };

  return {
    error(err) {
      parentPort.postMessage({
        status: ERROR,
        request,
        error: err && err.message || String(err)
      });
    },
    send(result) {
      parentPort.postMessage({
        status: RESULT,
        request,
        result
      });
    }
  };
};