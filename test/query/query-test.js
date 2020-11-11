import tape from 'tape';
import { all, desc, not, op, range, rolling, seed, table } from 'arquero';
import groupbyEqual from '../groupby-equal';
import tableEqual from '../table-equal';
import { field, func } from './util';
import Query from '../../src/query/query';
import {
  Antijoin, Concat, Count, Cross, Dedupe, Derive, Except, Filter, Fold,
  Groupby, Intersect, Join, Orderby, Pivot, Rollup, Sample, Select,
  Semijoin, Spread, Union, Unroll
} from '../../src/query/verb';
import QueryBuilder from '../../src/query/query-builder';

tape('Query serializes to objects', t => {
  const q = new Query([
    new Derive({ bar: d => d.foo + 1 }),
    new Rollup({
      count: op.count(),
      sum: op.sum('bar')
    }),
    new Orderby(['foo', desc('bar'), d => d.baz, desc(d => d.bop)]),
    new Groupby(['foo', { baz: d => d.baz, bop: d => d.bop }])
  ]);

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
  }, 'serialized query');
  t.end();
});

tape('Query evaluates unmodified inputs', t => {
  const q = new Query([
    new Derive({ bar: (d, $) => d.foo + $.offset }),
    new Rollup({ count: op.count(), sum: op.sum('bar') })
  ], { offset: 1});

  const dt = table({ foo: [0, 1, 2, 3] });
  const dr = q.evaluate(dt);

  tableEqual(t, dr, { count: [4], sum: [10] }, 'query data');
  t.end();
});

tape('Query evaluates serialized inputs', t => {
  const dt = table({
    foo: [0, 1, 2, 3],
    bar: [1, 1, 0, 0]
   });

  tableEqual(
    t,
    Query.from(
      new Query([
        new Derive({ baz: (d, $) => d.foo + $.offset }),
        new Orderby(['bar', 0]),
        new Select([not('bar')])
      ], { offset: 1 }).toObject()
    ).evaluate(dt),
    { foo: [ 2, 3, 0, 1 ], baz: [ 3, 4, 1, 2 ] },
    'serialized query data'
  );

  tableEqual(
    t,
    Query.from(
      new Query([
        new Derive({ bar: (d, $) => d.foo + $.offset }),
        new Rollup({ count: op.count(), sum: op.sum('bar') })
      ], { offset: 1 }).toObject()
    ).evaluate(dt),
    { count: [4], sum: [10] },
    'serialized query data'
  );

  t.end();
});

tape('Query evaluates count verbs', t => {
  const dt = table({
    foo: [0, 1, 2, 3],
    bar: [1, 1, 0, 0]
   });

  tableEqual(
    t,
    Query.from(
      new Query([new Count()]).toObject()
    ).evaluate(dt),
    { count: [4] },
    'count query result'
  );

  tableEqual(
    t,
    Query.from(
      new Query([new Count({ as: 'cnt' })]).toObject()
    ).evaluate(dt),
    { cnt: [4] },
    'count query result, with options'
  );

  t.end();
});

tape('Query evaluates dedupe verbs', t => {
  const dt = table({
    foo: [0, 1, 2, 3],
    bar: [1, 1, 0, 0]
   });

  tableEqual(
    t,
    Query.from(
      new Query([new Dedupe([])]).toObject()
    ).evaluate(dt),
    { foo: [0, 1, 2, 3], bar: [1, 1, 0, 0] },
    'dedupe query result'
  );

  tableEqual(
    t,
    Query.from(
      new Query([new Dedupe(['bar'])]).toObject()
    ).evaluate(dt),
    { foo: [0, 2], bar: [1, 0] },
    'dedupe query result, key'
  );

  tableEqual(
    t,
    Query.from(
      new Query([new Dedupe([not('foo')])]).toObject()
    ).evaluate(dt),
    { foo: [0, 2], bar: [1, 0] },
    'dedupe query result, key selection'
  );

  t.end();
});

tape('Query evaluates derive verbs', t => {
  const dt = table({
    foo: [0, 1, 2, 3],
    bar: [1, 1, 0, 0]
   });

  const verb = new Derive({
    baz: d => d.foo + 1 - op.mean(d.foo),
    bop: 'd => 2 * (d.foo - op.mean(d.foo))',
    sum: rolling(d => op.sum(d.foo)),
    win: rolling(d => op.product(d.foo), [0, 1])
  });

  tableEqual(
    t,
    Query.from(
      new Query([verb]).toObject()
    ).evaluate(dt),
    {
      foo: [0, 1, 2, 3],
      bar: [1, 1, 0, 0],
      baz: [-0.5, 0.5, 1.5, 2.5],
      bop: [-3, -1, 1, 3],
      sum: [0, 1, 3, 6],
      win: [0, 2, 6, 3]
    },
    'derive query result'
  );

  t.end();
});

tape('Query evaluates filter verbs', t => {
  const dt = table({
    foo: [0, 1, 2, 3],
    bar: [1, 1, 0, 0]
   });

  const verb = new Filter(d => d.bar > 0);

  tableEqual(
    t,
    Query.from(
      new Query([verb]).toObject()
    ).evaluate(dt),
    {
      foo: [0, 1],
      bar: [1, 1]
    },
    'filter query result'
  );

  t.end();
});

tape('Query evaluates groupby verbs', t => {
  const dt = table({
    foo: [0, 1, 2, 3],
    bar: [1, 1, 0, 0]
   });

  groupbyEqual(
    t,
    Query.from(
      new Query([new Groupby(['bar'])]).toObject()
    ).evaluate(dt),
    dt.groupby('bar'),
    'groupby query result'
  );

  groupbyEqual(
    t,
    Query.from(
      new Query([new Groupby([{bar: d => d.bar}])]).toObject()
    ).evaluate(dt),
    dt.groupby('bar'),
    'groupby query result, table expression'
  );

  groupbyEqual(
    t,
    Query.from(
      new Query([new Groupby([not('foo')])]).toObject()
    ).evaluate(dt),
    dt.groupby('bar'),
    'groupby query result, selection'
  );

  t.end();
});

tape('Query evaluates orderby verbs', t => {
  const dt = table({
    foo: [0, 1, 2, 3],
    bar: [1, 1, 0, 0]
   });

  tableEqual(
    t,
    Query.from(
      new Query([new Orderby(['bar', 'foo'])]).toObject()
    ).evaluate(dt),
    {
      foo: [2, 3, 0, 1],
      bar: [0, 0, 1, 1]
    },
    'orderby query result'
  );

  tableEqual(
    t,
    Query.from(
      new Query([new Orderby([
        d => d.bar,
        d => d.foo
      ])]).toObject()
    ).evaluate(dt),
    {
      foo: [2, 3, 0, 1],
      bar: [0, 0, 1, 1]
    },
    'orderby query result'
  );

  tableEqual(
    t,
    Query.from(
      new Query([new Orderby([desc('bar'), desc('foo')])]).toObject()
    ).evaluate(dt),
    {
      foo: [1, 0, 3, 2],
      bar: [1, 1, 0, 0]
    },
    'orderby query result, desc'
  );

  t.end();
});

tape('Query evaluates rollup verbs', t => {
  const dt = table({
    foo: [0, 1, 2, 3],
    bar: [1, 1, 0, 0]
   });

  tableEqual(
    t,
    Query.from(
      new Query([new Rollup({
        count: op.count(),
        sum:   op.sum('foo'),
        sump1: d => 1 + op.sum(d.foo + d.bar),
        avgt2: 'd => 2 * op.mean(op.abs(d.foo))'
      })]).toObject()
    ).evaluate(dt),
    { count: [4], sum: [6], sump1: [9], avgt2: [3] },
    'rollup query result'
  );

  t.end();
});

tape('Query evaluates sample verbs', t => {
  seed(12345);

  const dt = table({
    foo: [0, 1, 2, 3],
    bar: [1, 1, 0, 0]
   });

  tableEqual(
    t,
    Query.from(
      new Query([new Sample(2)]).toObject()
    ).evaluate(dt),
    { foo: [ 3, 1 ], bar: [ 0, 1 ] },
    'sample query result'
  );

  tableEqual(
    t,
    Query.from(
      new Query([new Sample(2, { replace: true })]).toObject()
    ).evaluate(dt),
    { foo: [ 2, 2 ], bar: [ 0, 0 ] },
    'sample query result, replace'
  );

  tableEqual(
    t,
    Query.from(
      new Query([new Sample(2, { weight: 'foo' })]).toObject()
    ).evaluate(dt),
    { foo: [ 3, 2 ], bar: [ 0, 0 ] },
    'sample query result, weight column name'
  );

  tableEqual(
    t,
    Query.from(
      new Query([new Sample(2, { weight: d => d.foo })]).toObject()
    ).evaluate(dt),
    { foo: [ 3, 1 ], bar: [ 0, 1 ] },
    'sample query result, weight table expression'
  );

  seed(null);
  t.end();
});

tape('Query evaluates select verbs', t => {
  const dt = table({
    foo: [0, 1, 2, 3],
    bar: [1, 1, 0, 0]
   });

  tableEqual(
    t,
    Query.from(
      new Query([new Select(['bar'])]).toObject()
    ).evaluate(dt),
    { bar: [1, 1, 0, 0] },
    'select query result, column name'
  );

  tableEqual(
    t,
    Query.from(
      new Query([new Select([all()])]).toObject()
    ).evaluate(dt),
    { foo: [0, 1, 2, 3], bar: [1, 1, 0, 0] },
    'select query result, all'
  );

  tableEqual(
    t,
    Query.from(
      new Query([new Select([not('foo')])]).toObject()
    ).evaluate(dt),
    { bar: [1, 1, 0, 0] },
    'select query result, not'
  );

  tableEqual(
    t,
    Query.from(
      new Query([new Select([range(1, 1)])]).toObject()
    ).evaluate(dt),
    { bar: [1, 1, 0, 0] },
    'select query result, range'
  );

  t.end();
});

tape('Query evaluates fold verbs', t => {
  const dt = table({
    foo: [0, 1, 2, 3],
    bar: [1, 1, 0, 0]
   });

  const folded =  {
    key: [ 'foo', 'bar', 'foo', 'bar', 'foo', 'bar', 'foo', 'bar' ],
    value: [ 0, 1, 1, 1, 2, 0, 3, 0 ]
  };

  tableEqual(
    t,
    Query.from(
      new Query([new Fold(['foo', 'bar'])]).toObject()
    ).evaluate(dt),
    folded,
    'fold query result, column names'
  );

  tableEqual(
    t,
    Query.from(
      new Query([new Fold([all()])]).toObject()
    ).evaluate(dt),
    folded,
    'fold query result, all'
  );

  tableEqual(
    t,
    Query.from(
      new Query([new Fold([{ foo: d => d.foo }])]).toObject()
    ).evaluate(dt),
    {
      bar: [ 1, 1, 0, 0 ],
      key: [ 'foo', 'foo', 'foo', 'foo' ],
      value: [ 0, 1, 2, 3 ]
    },
    'fold query result, table expression'
  );

  t.end();
});

tape('Query evaluates pivot verbs', t => {
  const dt = table({
    foo: [0, 1, 2, 3],
    bar: [1, 1, 0, 0]
   });

  tableEqual(
    t,
    Query.from(
      new Query([new Pivot(['bar'], ['foo'])]).toObject()
    ).evaluate(dt),
    { '0': [2], '1': [0] },
    'pivot query result, column names'
  );

  tableEqual(
    t,
    Query.from(
      new Query([new Pivot(
        [{ bar: d => d.bar }],
        [{ foo: op.sum('foo') }]
      )]).toObject()
    ).evaluate(dt),
    { '0': [5], '1': [1] },
    'pivot query result, table expressions'
  );

  t.end();
});

tape('Query evaluates spread verbs', t => {
  const dt = table({
    list: [[1, 2, 3]]
   });

  tableEqual(
    t,
    Query.from(
      new Query([new Spread(['list'])]).toObject()
    ).evaluate(dt),
    {
      'list': [[1, 2, 3]],
      'list1': [1],
      'list2': [2],
      'list3': [3]
    },
    'spread query result, column names'
  );

  tableEqual(
    t,
    Query.from(
      new Query([new Spread([{ list: d => d.list }])]).toObject()
    ).evaluate(dt),
    {
      'list': [[1, 2, 3]],
      'list1': [1],
      'list2': [2],
      'list3': [3]
    },
    'spread query result, table expression'
  );

  tableEqual(
    t,
    Query.from(
      new Query([new Spread(['list'], { limit: 2 })]).toObject()
    ).evaluate(dt),
    {
      'list': [[1, 2, 3]],
      'list1': [1],
      'list2': [2]
    },
    'spread query result, limit'
  );

  t.end();
});

tape('Query evaluates unroll verbs', t => {
  const dt = table({
    list: [[1, 2, 3]]
   });

  tableEqual(
    t,
    Query.from(
      new Query([new Unroll(['list'])]).toObject()
    ).evaluate(dt),
    { 'list': [1, 2, 3] },
    'unroll query result, column names'
  );

  tableEqual(
    t,
    Query.from(
      new Query([new Unroll([{ list: d => d.list }])]).toObject()
    ).evaluate(dt),
    { 'list': [1, 2, 3] },
    'unroll query result, table expression'
  );

  tableEqual(
    t,
    Query.from(
      new Query([new Unroll(['list'], { limit: 2 })]).toObject()
    ).evaluate(dt),
    { 'list': [1, 2] },
    'unroll query result, limit'
  );

  t.end();
});

tape('Query evaluates cross verbs', t => {
  const lt = table({
    x: ['A', 'B'],
    y: [1, 2]
  });

  const rt = table({
    u: ['C'],
    v: [3]
  });

  const catalog = name => name === 'other' ? rt : null;

  tableEqual(
    t,
    Query.from(
      new Query([
        new Cross('other')
      ]).toObject()
    ).evaluate(lt, catalog),
    { x: ['A', 'B'], y: [1, 2], u: ['C', 'C'], v: [3, 3] },
    'cross query result'
  );

  tableEqual(
    t,
    Query.from(
      new Query([
        new Cross('other', ['y', 'v'])
      ]).toObject()
    ).evaluate(lt, catalog),
    { y: [1, 2], v: [3, 3] },
    'cross query result, column name values'
  );

  tableEqual(
    t,
    Query.from(
      new Query([
        new Cross('other', [
          { y: d => d.y },
          { v: d => d.v }
        ])
      ]).toObject()
    ).evaluate(lt, catalog),
    { y: [1, 2], v: [3, 3] },
    'cross query result, table expression values'
  );

  tableEqual(
    t,
    Query.from(
      new Query([
        new Cross('other', {
          y: a => a.y,
          v: (a, b) => b.v
        })
      ]).toObject()
    ).evaluate(lt, catalog),
    { y: [1, 2], v: [3, 3] },
    'cross query result, two-table expression values'
  );

  t.end();
});

tape('Query evaluates join verbs', t => {
  const lt = table({
    x: ['A', 'B', 'C'],
    y: [1, 2, 3]
  });

  const rt = table({
    u: ['A', 'B', 'D'],
    v: [4, 5, 6]
  });

  const catalog = name => name === 'other' ? rt : null;

  tableEqual(
    t,
    Query.from(
      new Query([
        new Join('other', ['x', 'u'])
      ]).toObject()
    ).evaluate(lt, catalog),
    { x: ['A', 'B'], y: [1, 2], u: ['A', 'B'], v: [4, 5] },
    'join query result, column name keys'
  );

  tableEqual(
    t,
    Query.from(
      new Query([
        new Join('other', (a, b) => op.equal(a.x, b.u))
      ]).toObject()
    ).evaluate(lt, catalog),
    { x: ['A', 'B'], y: [1, 2], u: ['A', 'B'], v: [4, 5] },
    'join query result, predicate expression'
  );

  tableEqual(
    t,
    Query.from(
      new Query([
        new Join('other', ['x', 'u'], [['x', 'y'], 'v'])
      ]).toObject()
    ).evaluate(lt, catalog),
    { x: ['A', 'B'], y: [1, 2], v: [4, 5] },
    'join query result, column name values'
  );

  tableEqual(
    t,
    Query.from(
      new Query([
        new Join('other', ['x', 'u'], [all(), not('u')])
      ]).toObject()
    ).evaluate(lt, catalog),
    { x: ['A', 'B'], y: [1, 2], v: [4, 5] },
    'join query result, selection values'
  );

  tableEqual(
    t,
    Query.from(
      new Query([
        new Join('other', ['x', 'u'], [
          { x: d => d.x, y: d => d.y },
          { v: d => d.v }
        ])
      ]).toObject()
    ).evaluate(lt, catalog),
    { x: ['A', 'B'], y: [1, 2], v: [4, 5] },
    'join query result, table expression values'
  );

  tableEqual(
    t,
    Query.from(
      new Query([
        new Join('other', ['x', 'u'], {
          x: a => a.x,
          y: a => a.y,
          v: (a, b) => b.v
        })
      ]).toObject()
    ).evaluate(lt, catalog),
    { x: ['A', 'B'], y: [1, 2], v: [4, 5] },
    'join query result, two-table expression values'
  );

  tableEqual(
    t,
    Query.from(
      new Query([
        new Join('other', ['x', 'u'], [['x', 'y'], ['u', 'v']],
          { left: true, right: true})
      ]).toObject()
    ).evaluate(lt, catalog),
    {
      x: [ 'A', 'B', 'C', undefined ],
      y: [ 1, 2, 3, undefined ],
      u: [ 'A', 'B', undefined, 'D' ],
      v: [ 4, 5, undefined, 6 ]
    },
    'join query result, full join'
  );

  t.end();
});

tape('Query evaluates semijoin verbs', t => {
  const lt = table({
    x: ['A', 'B', 'C'],
    y: [1, 2, 3]
  });

  const rt = table({
    u: ['A', 'B', 'D'],
    v: [4, 5, 6]
  });

  const catalog = name => name === 'other' ? rt : null;

  tableEqual(
    t,
    Query.from(
      new Query([
        new Semijoin('other', ['x', 'u'])
      ]).toObject()
    ).evaluate(lt, catalog),
    { x: ['A', 'B'], y: [1, 2] },
    'semijoin query result, column name keys'
  );

  tableEqual(
    t,
    Query.from(
      new Query([
        new Semijoin('other', (a, b) => op.equal(a.x, b.u))
      ]).toObject()
    ).evaluate(lt, catalog),
    { x: ['A', 'B'], y: [1, 2] },
    'semijoin query result, predicate expression'
  );

  t.end();
});

tape('Query evaluates antijoin verbs', t => {
  const lt = table({
    x: ['A', 'B', 'C'],
    y: [1, 2, 3]
  });

  const rt = table({
    u: ['A', 'B', 'D'],
    v: [4, 5, 6]
  });

  const catalog = name => name === 'other' ? rt : null;

  tableEqual(
    t,
    Query.from(
      new Query([
        new Antijoin('other', ['x', 'u'])
      ]).toObject()
    ).evaluate(lt, catalog),
    { x: ['C'], y: [3] },
    'antijoin query result, column name keys'
  );

  tableEqual(
    t,
    Query.from(
      new Query([
        new Antijoin('other', (a, b) => op.equal(a.x, b.u))
      ]).toObject()
    ).evaluate(lt, catalog),
    { x: ['C'], y: [3] },
    'antijoin query result, predicate expression'
  );

  t.end();
});

tape('Query evaluates concat verbs', t => {
  const lt = table({
    x: ['A', 'B'],
    y: [1, 2]
  });

  const rt = table({
    x: ['B', 'C'],
    y: [2, 3]
  });

  const catalog = name => name === 'other' ? rt : null;

  tableEqual(
    t,
    Query.from(
      new Query([ new Concat(['other']) ]).toObject()
    ).evaluate(lt, catalog),
    { x: ['A', 'B', 'B', 'C'], y: [1, 2, 2, 3] },
    'concat query result'
  );

  t.end();
});

tape('Query evaluates concat verbs with subqueries', t => {
  const lt = table({
    x: ['A', 'B'],
    y: [1, 2]
  });

  const rt = table({
    a: ['B', 'C'],
    b: [2, 3]
  });

  const catalog = name => name === 'other' ? rt : null;

  const sub = new QueryBuilder('other')
    .select({ a: 'x', b: 'y' });

  tableEqual(
    t,
    Query.from(
      new Query([ new Concat([sub]) ]).toObject()
    ).evaluate(lt, catalog),
    { x: ['A', 'B', 'B', 'C'], y: [1, 2, 2, 3] },
    'concat query result'
  );

  t.end();
});

tape('Query evaluates union verbs', t => {
  const lt = table({
    x: ['A', 'B'],
    y: [1, 2]
  });

  const rt = table({
    x: ['B', 'C'],
    y: [2, 3]
  });

  const catalog = name => name === 'other' ? rt : null;

  tableEqual(
    t,
    Query.from(
      new Query([ new Union(['other']) ]).toObject()
    ).evaluate(lt, catalog),
    { x: ['A', 'B', 'C'], y: [1, 2, 3] },
    'union query result'
  );

  t.end();
});

tape('Query evaluates except verbs', t => {
  const lt = table({
    x: ['A', 'B'],
    y: [1, 2]
  });

  const rt = table({
    x: ['B', 'C'],
    y: [2, 3]
  });

  const catalog = name => name === 'other' ? rt : null;

  tableEqual(
    t,
    Query.from(
      new Query([ new Except(['other']) ]).toObject()
    ).evaluate(lt, catalog),
    { x: ['A'], y: [1] },
    'except query result'
  );

  t.end();
});

tape('Query evaluates intersect verbs', t => {
  const lt = table({
    x: ['A', 'B'],
    y: [1, 2]
  });

  const rt = table({
    x: ['B', 'C'],
    y: [2, 3]
  });

  const catalog = name => name === 'other' ? rt : null;

  tableEqual(
    t,
    Query.from(
      new Query([ new Intersect(['other']) ]).toObject()
    ).evaluate(lt, catalog),
    { x: ['B'], y: [2] },
    'intersect query result'
  );

  t.end();
});