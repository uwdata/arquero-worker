# arquero-worker <a href="https://github.com/uwdata/arquero-worker"><img align="right" src="https://github.com/uwdata/arquero/blob/master/docs/assets/logo.svg?raw=true" height="38"></img></a>

A proof-of-concept implementation of worker thread support for [Arquero](https://github.com/uwdata/arquero) queries. Forks a worker thread as either a [Web Worker](https://developer.mozilla.org/en-US/docs/Web/API/Web_Workers_API) or a [node.js Worker thread](https://nodejs.org/api/worker_threads.html) and provides an API for authoring queries, submitting queries to the worker for processing, and fetching the query results.

See [worker-example.html](https://github.com/uwdata/arquero-query/blob/main/examples/worker-example.html) for a usage example.

## Build Instructions

To build and develop arquero-query locally:

- Clone [https://github.com/uwdata/arquero-worker](https://github.com/uwdata/arquero-worker).
- Run `yarn` to install dependencies for all packages. If you don't have yarn installed, see [https://yarnpkg.com/en/docs/install](https://yarnpkg.com/en/docs/install).
- Run `yarn test` to run test cases, and `yarn build` to build output files.
