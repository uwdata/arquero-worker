import tape from 'tape';
import { all, bin, desc, not, op, range, rolling } from 'arquero';
import {
  Count, Dedupe, Derive, Filter, Groupby, Join, Orderby,
  Pivot, Rollup, Select, Ungroup, Unorder
} from '../../src/query/verb';

const func = (expr, props) => ({
  expr, func: true, ...props
});

const field = (expr, props) => ({
  expr, field: true, ...props
});

tape('Count verb serializes to objects', t => {
  t.deepEqual(
    (new Count()).toObject(),
    {
      verb: 'count',
      options: undefined
    },
    'serialized count, no options'
  );

  t.deepEqual(
    (new Count({ as: 'cnt' })).toObject(),
    {
      verb: 'count',
      options: { as: 'cnt' }
    },
    'serialized count, with options'
  );

  t.end();
});

tape('Dedupe verb serializes to objects', t => {
  t.deepEqual(
    (new Dedupe()).toObject(),
    {
      verb: 'dedupe',
      keys: []
    },
    'serialized dedupe, no keys'
  );

  t.deepEqual(
    (new Dedupe(['id', d => d.foo])).toObject(),
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

  t.deepEqual(
    verb.toObject(),
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
  const verb = new Filter(d => d.foo > 2);

  t.deepEqual(
    verb.toObject(),
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

  t.deepEqual(
    verb.toObject(),
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

  t.deepEqual(
    binVerb.toObject(),
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

  t.deepEqual(
    verbSel.toObject(),
    {
      verb: 'join',
      table: 'tableRef',
      on: [
        [field('keyL')],
        [field('keyR', { index: 1 })]
      ],
      values: [
        { all: [] },
        { not: ['keyR'] }
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

  t.deepEqual(
    verbCols.toObject(),
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

  t.deepEqual(
    verbExpr.toObject(),
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

  t.deepEqual(
    verb.toObject(),
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

  t.deepEqual(
    verb.toObject(),
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

  t.deepEqual(
    verb.toObject(),
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
    all(),
    range(0, 1),
    range('a', 'b'),
    not('foo', 'bar', range(0, 1), range('a', 'b'))
  ]);

  t.deepEqual(
    verb.toObject(),
    {
      verb: 'select',
      columns: [
        'foo',
        'bar',
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
  const verb = new Ungroup();

  t.deepEqual(
    verb.toObject(),
    { verb: 'ungroup' },
    'serialized ungroup verb'
  );

  t.end();
});

tape('Unorder verb serializes to object', t => {
  const verb = new Unorder();

  t.deepEqual(
    verb.toObject(),
    { verb: 'unorder' },
    'serialized unorder verb'
  );

  t.end();
});