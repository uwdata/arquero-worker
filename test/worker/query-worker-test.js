import tape from 'tape';
import tableEqual from '../table-equal';
import QueryWorker from '../../src/client/query-worker';

const SOURCE = './dist/arquero-node-worker.js';

tape('Worker thread processes queries', async t => {
  const worker = new QueryWorker(SOURCE);

  const q = await worker.table('test', {
    x: [1, 2, 3],
    y: [4, 5, 6]
  });

  tableEqual(
    t,
    await q.fetch(),
    { x: [1, 2, 3], y: [4, 5, 6] },
    'worker table fetch'
  );

  tableEqual(
    t,
    await q
      .rollup({
        sx: 'd => op.sum(d.x)',
        sy: 'd => op.sum(d.y)'
      })
      .fetch(),
    { sx: [6], sy: [15] },
    'worker table query'
  );

  t.deepEqual(
    await worker.list(),
    ['test'],
    'worker table list'
  );

  // shut down worker so we can exit properly
  worker.terminate();

  t.end();
});