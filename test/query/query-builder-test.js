import tape from 'tape';
import { desc, not, op } from 'arquero';
import { field, func } from './util';
import QueryBuilder from '../../src/query/query-builder';

tape('QueryBuilder builds single-table queries', t => {
  const q = new QueryBuilder()
    .derive({ bar: d => d.foo + 1 })
    .rollup({ count: op.count(), sum: op.sum('bar') })
    .orderby('foo', desc('bar'), d => d.baz, desc(d => d.bop))
    .groupby('foo', { baz: d => d.baz, bop: d => d.bop })
    .query();

  t.deepEqual(q.toObject(), {
    verbs: [
      {
        verb: 'derive',
        values: { bar: func('d => d.foo + 1') }
      },
      {
        verb: 'rollup',
        values: {
          count: func('d => op.count()'),
          sum: func('d => op.sum(d["bar"])')
        }
      },
      {
        verb: 'orderby',
        keys: [
          field('foo'),
          field('bar', { desc: true }),
          func('d => d.baz'),
          func('d => d.bop', { desc: true })
        ]
      },
      {
        verb: 'groupby',
        keys: [
          'foo',
          {
            baz: func('d => d.baz'),
            bop: func('d => d.bop')
          }
        ]
      }
    ]
  }, 'serialized query from builder');

  t.end();
});

tape('QueryBuilder supports multi-table verbs', t => {
  const q = new QueryBuilder()
    .concat('concat_table')
    .join('join_table')
    .query();

  t.deepEqual(q.toObject(), {
    verbs: [
      {
        verb: 'concat',
        tables: ['concat_table']
      },
      {
        verb: 'join',
        table: 'join_table',
        on: undefined,
        values: undefined,
        options: undefined
      }
    ]
  }, 'serialized query from builder');

  t.end();
});

tape('QueryBuilder supports multi-table queries', t => {
  const qc = new QueryBuilder('concat_table')
    .select(not('foo'));

  const qj = new QueryBuilder('join_table')
    .select(not('bar'));

  const q = new QueryBuilder()
    .concat(qc)
    .join(qj)
    .query();

  t.deepEqual(q.toObject(), {
    verbs: [
      {
        verb: 'concat',
        tables: [ qc.toObject() ]
      },
      {
        verb: 'join',
        table: qj.toObject(),
        on: undefined,
        values: undefined,
        options: undefined
      }
    ]
  }, 'serialized query from builder');

  t.end();
});