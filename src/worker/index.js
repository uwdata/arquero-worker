import { ERROR, RESULT } from './constants';
import { handleMessage } from './handler';

onmessage = async function(event) {
  const request = event.data;
  handleMessage(request, response(request));
};

const response = req => {
  const request = {
    id: req.id,
    method: req.method
  };

  return {
    error(err) {
      postMessage({
        status: ERROR,
        request,
        error: err && err.message || String(err)
      });
    },
    send(result, transfer) {
      postMessage({
        status: RESULT,
        request,
        result
      }, transfer);
    }
  };
};