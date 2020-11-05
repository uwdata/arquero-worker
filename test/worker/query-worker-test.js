import tape from 'tape';
import tableEqual from '../table-equal';
import QueryWorker from '../../src/worker-client/query-worker';

const SOURCE = './dist/arquero-node-worker.js';

tape('Worker thread processes queries', async t => {
  const qw = new QueryWorker(SOURCE);

  const qb = await qw.table('test', {
    x: [1, 2, 3],
    y: [4, 5, 6]
  });

  tableEqual(
    t,
    await qb.fetch(),
    { x: [1, 2, 3], y: [4, 5, 6] },
    'worker table fetch'
  );

  tableEqual(
    t,
    await qb.rollup({
      sx: 'd => op.sum(d.x)',
      sy: 'd => op.sum(d.y)'
    }).fetch(),
    { sx: [6], sy: [15] },
    'worker table query'
  );

  t.deepEqual(await qw.list(), ['test'], 'worker table list');

  qw.terminate();

  t.end();
});