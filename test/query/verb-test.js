import tape from 'tape';
import { all, bin, desc, not, op, range, rolling } from 'arquero';
import { field, func } from './util';
import QueryBuilder from '../../src/query/query-builder';
import {
  Concat, Count, Dedupe, Derive, Filter, Groupby, Join, Orderby,
  Pivot, Rollup, Select, Ungroup, Unorder, Verb
} from '../../src/query/verb';

function test(t, verb, expect, msg) {
  const object = verb.toObject();
  t.deepEqual(object, expect, msg);
  const rt = Verb.from(object).toObject();
  t.deepEqual(rt, expect, msg + ' round-trip');
}

tape('Count verb serializes to objects', t => {
  test(t,
    new Count(),
    {
      verb: 'count',
      options: undefined
    },
    'serialized count, no options'
  );

  test(t,
    new Count({ as: 'cnt' }),
    {
      verb: 'count',
      options: { as: 'cnt' }
    },
    'serialized count, with options'
  );

  t.end();
});

tape('Dedupe verb serializes to objects', t => {
  test(t,
    new Dedupe(),
    {
      verb: 'dedupe',
      keys: []
    },
    'serialized dedupe, no keys'
  );

  test(t,
    new Dedupe(['id', d => d.foo]),
    {
      verb: 'dedupe',
      keys: [
        'id',
        func('d => d.foo')
      ]
    },
    'serialized dedupe, with keys'
  );

  t.end();
});

tape('Derive verb serializes to object', t => {
  const verb = new Derive({
    foo: 'd.bar * 5',
    bar: d => d.foo + 1,
    baz: rolling(d => op.mean(d.foo), [-3, 3])
  });

  test(t,
    verb,
    {
      verb: 'derive',
      values: {
        foo: 'd.bar * 5',
        bar: func('d => d.foo + 1'),
        baz: func(
          'd => op.mean(d.foo)',
          { window: { frame: [ -3, 3 ], peers: false } }
        )
      }
    },
    'serialized derive verb'
  );

  t.end();
});

tape('Filter verb serializes to object', t => {
  test(t,
    new Filter(d => d.foo > 2),
    {
      verb: 'filter',
      criteria: func('d => d.foo > 2')
    },
    'serialized filter verb'
  );

  t.end();
});

tape('Groupby verb serializes to object', t => {
  const verb = new Groupby([
    'foo',
    { baz: d => d.baz, bop: d => d.bop }
  ]);

  test(t,
    verb,
    {
      verb: 'groupby',
      keys: [
        'foo',
        {
          baz: func('d => d.baz'),
          bop: func('d => d.bop')
        }
      ]
    },
    'serialized groupby verb'
  );

  const binVerb = new Groupby([{ bin0: bin('foo') }]);

  test(t,
    binVerb,
    {
      verb: 'groupby',
      keys: [
        {
          bin0: 'd => op.bin(d["foo"], ...op.bins(d["foo"]), 0)'
        }
      ]
    },
    'serialized groupby verb, with binning'
  );

  t.end();
});

tape('Join verb serializes to object', t => {
  const verbSel = new Join(
    'tableRef',
    ['keyL', 'keyR'],
    [all(), not('keyR')],
    { suffix: ['_L', '_R'] }
  );

  test(t,
    verbSel,
    {
      verb: 'join',
      table: 'tableRef',
      on: [
        [field('keyL')],
        [field('keyR')]
      ],
      values: [
        [ { all: [] } ],
        [ { not: ['keyR'] } ]
      ],
      options: { suffix: ['_L', '_R'] }
    },
    'serialized join verb, column selections'
  );

  const verbCols = new Join(
    'tableRef',
    [
      [d => d.keyL],
      [d => d.keyR]
    ],
    [
      ['keyL', 'valL', { foo: d => 1 + d.valL }],
      ['valR', { bar: d => 2 * d.valR }]
    ],
    { suffix: ['_L', '_R'] }
  );

  test(t,
    verbCols,
    {
      verb: 'join',
      table: 'tableRef',
      on: [
        [ func('d => d.keyL') ],
        [ func('d => d.keyR') ]
      ],
      values: [
        ['keyL', 'valL', { foo: func('d => 1 + d.valL') } ],
        ['valR', { bar: func('d => 2 * d.valR') }]
      ],
      options: { suffix: ['_L', '_R'] }
    },
    'serialized join verb, column lists'
  );

  const verbExpr = new Join(
    'tableRef',
    (a, b) => op.equal(a.keyL, b.keyR),
    {
      key: a => a.keyL,
      foo: a => a.foo,
      bar: (a, b) => b.bar
    }
  );

  test(t,
    verbExpr,
    {
      verb: 'join',
      table: 'tableRef',
      on: func('(a, b) => op.equal(a.keyL, b.keyR)'),
      values: {
        key: func('a => a.keyL'),
        foo: func('a => a.foo'),
        bar: func('(a, b) => b.bar')
      },
      options: undefined
    },
    'serialized join verb, table expressions'
  );

  t.end();
});

tape('Orderby verb serializes to object', t => {
  const verb = new Orderby([
    1,
    'foo',
    desc('bar'),
    d => d.baz,
    desc(d => d.bop)
  ]);

  test(t,
    verb,
    {
      verb: 'orderby',
      keys: [
        1,
        field('foo'),
        field('bar', { desc: true }),
        func('d => d.baz'),
        func('d => d.bop', { desc: true })
      ]
    },
    'serialized orderby verb'
  );

  t.end();
});

tape('Pivot verb serializes to object', t => {
  const verb = new Pivot(
    ['key'],
    ['value', { sum: d => op.sum(d.foo), prod: op.product('bar') }],
    { sort: false }
  );

  test(t,
    verb,
    {
      verb: 'pivot',
      keys: ['key'],
      values: [
        'value',
        {
          sum: func('d => op.sum(d.foo)'),
          prod: func('d => op.product(d["bar"])')
        }
      ],
      options: { sort: false }
    },
    'serialized pivot verb'
  );

  t.end();
});

tape('Rollup verb serializes to object', t => {
  const verb = new Rollup({
    count: op.count(),
    sum: op.sum('bar'),
    mean: d => op.mean(d.foo)
  });

  test(t,
    verb,
    {
      verb: 'rollup',
      values: {
        count: func('d => op.count()'),
        sum: func('d => op.sum(d["bar"])'),
        mean: func('d => op.mean(d.foo)')
      }
    },
    'serialized rollup verb'
  );

  t.end();
});

tape('Select verb serializes to objects', t => {
  const verb = new Select([
    'foo',
    'bar',
    { bop: 'boo', baz: 'bao' },
    all(),
    range(0, 1),
    range('a', 'b'),
    not('foo', 'bar', range(0, 1), range('a', 'b'))
  ]);

  test(t,
    verb,
    {
      verb: 'select',
      columns: [
        'foo',
        'bar',
        { bop: 'boo', baz: 'bao' },
        { all: [] },
        { range: [0, 1] },
        { range: ['a', 'b'] },
        {
          not: [
            'foo',
            'bar',
            { range: [0, 1] },
            { range: ['a', 'b'] }
          ]
        }
      ]
    },
    'serialized select verb'
  );

  t.end();
});

tape('Ungroup verb serializes to object', t => {
  test(t,
    new Ungroup(),
    { verb: 'ungroup' },
    'serialized ungroup verb'
  );

  t.end();
});

tape('Unorder verb serializes to object', t => {
  test(t,
    new Unorder(),
    { verb: 'unorder' },
    'serialized unorder verb'
  );

  t.end();
});

tape('Concat verb serializes to object', t => {
  test(t,
    new Concat(['foo', 'bar']),
    {
      verb: 'concat',
      tables: ['foo', 'bar']
    },
    'serialized concat verb'
  );

  const ct1 = new QueryBuilder('foo').select(not('bar'));
  const ct2 = new QueryBuilder('bar').select(not('foo'));

  test(t,
    new Concat([ct1, ct2]),
    {
      verb: 'concat',
      tables: [ ct1.toObject(), ct2.toObject() ]
    },
    'serialized concat verb, with subqueries'
  );

  t.end();
});