# arquero-query <a href="https://github.com/uwdata/arquero-query"><img align="right" src="https://github.com/uwdata/arquero/blob/master/docs/assets/logo.svg?raw=true" height="38"></img></a>

Serialized query and worker support for [Arquero](https://github.com/uwdata/arquero).

This package includes support for:

- Representing Arquero verbs as serialized, JSON-compatible objects.
- Building serialized queries using the Arquero Table API.
- Off-loading query processing to worker threads.

See [worker-example.html](https://github.com/uwdata/arquero-query/blob/main/examples/worker-example.html) for a usage example.

## Build Instructions

To build and develop arquero-query locally:

- Clone [https://github.com/uwdata/arquero-query](https://github.com/uwdata/arquero-query).
- Run `yarn` to install dependencies for all packages. If you don't have yarn installed, see [https://yarnpkg.com/en/docs/install](https://yarnpkg.com/en/docs/install).
- Run `yarn test` to run test cases, and `yarn build` to build output files.
