# arquero-worker <a href="https://github.com/uwdata/arquero-worker"><img align="right" src="https://github.com/uwdata/arquero/blob/master/docs/assets/logo.svg?raw=true" height="38"></img></a>

A proof-of-concept implementation of worker thread support for [Arquero](https://github.com/uwdata/arquero) queries. Forks a worker thread using either a [Web Worker](https://developer.mozilla.org/en-US/docs/Web/API/Web_Workers_API) or a [node.js Worker thread](https://nodejs.org/api/worker_threads.html) and provides an API for authoring queries, submitting queries to the worker for processing, and fetching the query results.

## Example

```js
// create query worker, providing web worker script
const qw = aq.worker('./arquero-worker.min.js');

// load dataset into worker thread
// return value is a query builder with a table verb API
const beers = await qw.load('beers', 'data/beers.csv');

// build a query for beers with the word 'hop' in their name
// fetch the first 20 rows: query is processed on worker thread
const hops = await beers
  .filter(d => op.match(/hop/i, d.name))
  .select('name', 'abv', 'ibu')
  .orderby('name')
  .fetch({ limit: 20 });

// print the fetched rows to the console
hops.print();
```

For more, see the [example page](https://uwdata.github.io/arquero-worker/example/) and its [source code](https://github.com/uwdata/arquero-query/blob/main/docs/example/index.html).

## Build Instructions

To build and develop arquero-query locally:

- Clone [https://github.com/uwdata/arquero-worker](https://github.com/uwdata/arquero-worker).
- Run `yarn` to install dependencies for all packages. If you don't have yarn installed, see [https://yarnpkg.com/en/docs/install](https://yarnpkg.com/en/docs/install).
- Run `yarn test` to run test cases, and `yarn build` to build output files.
