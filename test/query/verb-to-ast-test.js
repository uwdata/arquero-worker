import tape from 'tape';
import { all, bin, desc, not, op, range, rolling } from 'arquero';
import QueryBuilder from '../../src/query/query-builder';
import {
  Concat, Count, Dedupe, Derive, Filter, Groupby, Join, Orderby,
  Pivot, Reify, Rollup, Sample, Select, Ungroup, Unorder, Unroll
} from '../../src/query/verb';

function toAST(verb) {
  return JSON.parse(JSON.stringify(verb.toAST()));
}

tape('Count verb serializes to AST', t => {
  t.deepEqual(
    toAST(new Count()),
    { verb: 'count' },
    'ast count, no options'
  );

  t.deepEqual(
    toAST(new Count({ as: 'cnt' })),
    {
      verb: 'count',
      options: { as: 'cnt' }
    },
    'ast count, with options'
  );

  t.end();
});

tape('Dedupe verb serializes to AST', t => {
  t.deepEqual(
    toAST(new Dedupe()),
    {
      verb: 'dedupe',
      keys: []
    },
    'ast dedupe, no keys'
  );

  t.deepEqual(
    toAST(new Dedupe(['id', d => d.foo, d => Math.abs(d.bar)])),
    {
      verb: 'dedupe',
      keys: [
        { type: 'Column', name: 'id' },
        { type: 'Column', name: 'foo' },
        {
          type: 'CallExpression',
          callee: { type: 'Function', name: 'abs' },
          arguments: [ { type: 'Column', name: 'bar' } ]
        }
      ]
    },
    'ast dedupe, with keys'
  );
  t.end();
});

tape('Derive verb serializes to AST', t => {
  const verb = new Derive({
    col: d => d.foo,
    foo: 'd.bar * 5',
    bar: d => d.foo + 1,
    baz: rolling(d => op.mean(d.foo), [-3, 3])
  });

  t.deepEqual(
    toAST(verb),
    {
      verb: 'derive',
      values: [
        { type: 'Column', name: 'foo', as: 'col' },
        {
          type: 'BinaryExpression',
          left: { type: 'Column', name: 'bar' },
          operator: '*',
          right: { type: 'Literal', value: 5, raw: '5' },
          as: 'foo'
        },
        {
          type: 'BinaryExpression',
          left: { type: 'Column', name: 'foo' },
          operator: '+',
          right: { type: 'Literal', value: 1, raw: '1' },
          as: 'bar'
        },
        {
          type: 'Window',
          frame: [ -3, 3 ],
          peers: false,
          expr: {
            type: 'CallExpression',
            callee: { type: 'Function', name: 'mean' },
            arguments: [ { type: 'Column', name: 'foo' } ]
          },
          as: 'baz'
        }
      ]
    },
    'ast derive verb'
  );

  t.end();
});

tape('Filter verb serializes to AST', t => {
  const ast = {
    verb: 'filter',
    criteria: {
      type: 'BinaryExpression',
      left: { type: 'Column', name: 'foo' },
      operator: '>',
      right: { type: 'Literal', value: 2, raw: '2' }
    }
  };

  t.deepEqual(
    toAST(new Filter(d => d.foo > 2)),
    ast,
    'ast filter verb'
  );

  t.deepEqual(
    toAST(new Filter('d.foo > 2')),
    ast,
    'ast filter verb, expr string'
  );

  t.end();
});

tape('Groupby verb serializes to AST', t => {
  t.deepEqual(
    toAST(new Groupby([
      'foo',
      1,
      { baz: d => d.baz, bop: d => d.bop, bar: d => Math.abs(d.bar) }
    ])),
    {
      verb: 'groupby',
      keys: [
        { type: 'Column', name: 'foo' },
        { type: 'Column', index: 1 },
        { type: 'Column', name: 'baz', as: 'baz' },
        { type: 'Column', name: 'bop', as: 'bop' },
        {
          type: 'CallExpression',
          callee: { type: 'Function', name: 'abs' },
          arguments: [ { type: 'Column', name: 'bar' } ],
          as: 'bar'
        }
      ]
    },
    'ast groupby verb'
  );

  t.deepEqual(
    toAST(new Groupby([{ bin0: bin('foo') }])),
    {
      verb: 'groupby',
      keys: [
        {
          as: 'bin0',
          type: 'CallExpression',
          callee: { type: 'Function', name: 'bin' },
          arguments: [
            { type: 'Column', name: 'foo' },
            {
              type: 'SpreadElement',
              argument: {
                type: 'CallExpression',
                callee: { type: 'Function', name: 'bins' },
                arguments: [{ type: 'Column', name: 'foo' }]
              }
            },
            { type: 'Literal', value: 0, raw: '0' }
          ]
        }
      ]
    },
    'ast groupby verb, with binning'
  );

  t.end();
});

tape('Orderby verb serializes to AST', t => {
  const verb = new Orderby([
    1,
    'foo',
    desc('bar'),
    d => d.baz,
    desc(d => d.bop)
  ]);

  t.deepEqual(
    toAST(verb),
    {
      verb: 'orderby',
      keys:  [
        { type: 'Column', index: 1 },
        { type: 'Column', name: 'foo' },
        { type: 'Descending', expr: { type: 'Column', name: 'bar' } },
        { type: 'Column', name: 'baz' },
        { type: 'Descending', expr: { type: 'Column', name: 'bop' } }
      ]
    },
    'ast orderby verb'
  );

  t.end();
});

tape('Pivot verb serializes to AST', t => {
  const verb = new Pivot(
    ['key'],
    ['value', { sum: d => op.sum(d.foo), prod: op.product('bar') }],
    { sort: false }
  );

  t.deepEqual(
    toAST(verb),
    {
      verb: 'pivot',
      keys: [ { type: 'Column', name: 'key' } ],
      values: [
        { type: 'Column', name: 'value' },
        {
          as: 'sum',
          type: 'CallExpression',
          callee: { type: 'Function', name: 'sum' },
          arguments: [ { type: 'Column', name: 'foo' } ]
        },
        {
          as: 'prod',
          type: 'CallExpression',
          callee: { type: 'Function', name: 'product' },
          arguments: [ { type: 'Column', name: 'bar' } ]
        }
      ],
      options: { sort: false }
    },
    'ast pivot verb'
  );

  t.end();
});

tape('Reify verb serializes to AST', t => {
  const verb = new Reify();

  t.deepEqual(
    toAST(verb),
    { verb: 'reify' },
    'ast ungroup verb'
  );

  t.end();
});

tape('Rollup verb serializes to AST', t => {
  const verb = new Rollup({
    count: op.count(),
    sum: op.sum('bar'),
    mean: d => op.mean(d.foo)
  });

  t.deepEqual(
    toAST(verb),
    {
      verb: 'rollup',
      values: [
        {
          as: 'count',
          type: 'CallExpression',
          callee: { type: 'Function', name: 'count' },
          arguments: []
        },
        {
          as: 'sum',
          type: 'CallExpression',
          callee: { type: 'Function', name: 'sum' },
          arguments: [{ type: 'Column', name: 'bar' } ]
        },
        {
          as: 'mean',
          type: 'CallExpression',
          callee: { type: 'Function', name: 'mean' },
          arguments: [ { type: 'Column', name: 'foo' } ]
        }
      ]
    },
    'ast rollup verb'
  );

  t.end();
});

tape('Sample verb serializes to AST', t => {
  t.deepEqual(
    toAST(new Sample(2, { replace: true })),
    {
      verb: 'sample',
      size: 2,
      options: { replace: true }
    },
    'ast sample verb'
  );

  t.deepEqual(
    toAST(new Sample(() => op.count())),
    {
      verb: 'sample',
      size: {
        type: 'CallExpression',
        callee: { type: 'Function', name: 'count' },
        arguments: []
      }
    },
    'ast sample verb, size function'
  );

  t.deepEqual(
    toAST(new Sample('() => op.count()')),
    {
      verb: 'sample',
      size: {
        type: 'CallExpression',
        callee: { type: 'Function', name: 'count' },
        arguments: []
      }
    },
    'ast sample verb, size function as string'
  );

  t.deepEqual(
    toAST(new Sample(2, { weight: 'foo' })),
    {
      verb: 'sample',
      size: 2,
      options: { weight: { type: 'Column', name: 'foo' } }
    },
    'ast sample verb, weight column name'
  );

  t.deepEqual(
    toAST(new Sample(2, { weight: d => 2 * d.foo })),
    {
      verb: 'sample',
      size: 2,
      options: {
        weight: {
          type: 'BinaryExpression',
          left: { type: 'Literal', value: 2, raw: '2' },
          operator: '*',
          right: { type: 'Column', name: 'foo' }
        }
      }
    },
    'ast sample verb, weight table expression'
  );

  t.end();
});

tape('Select verb serializes to AST', t => {
  const verb = new Select([
    'foo',
    'bar',
    { bop: 'boo', baz: 'bao' },
    all(),
    range(0, 1),
    range('a', 'b'),
    not('foo', 'bar', range(0, 1), range('a', 'b'))
  ]);

  t.deepEqual(
    toAST(verb),
    {
      verb: 'select',
      columns: [
        { type: 'Column', name: 'foo' },
        { type: 'Column', name: 'bar' },
        { type: 'Column', name: 'bop', as: 'boo' },
        { type: 'Column', name: 'baz', as: 'bao' },
        { type: 'Selection', operator: 'all' },
        {
          type: 'Selection',
          operator: 'range',
          arguments: [
            { type: 'Column', index: 0 },
            { type: 'Column', index: 1 }
          ]
        },
        {
          type: 'Selection',
          operator: 'range',
          arguments: [
            { type: 'Column', name: 'a' },
            { type: 'Column', name: 'b' }
          ]
        },
        {
          type: 'Selection',
          operator: 'not',
          arguments: [
            { type: 'Column', name: 'foo' },
            { type: 'Column', name: 'bar' },
            {
              type: 'Selection',
              operator: 'range',
              arguments: [
                { type: 'Column', index: 0 },
                { type: 'Column', index: 1 }
              ]
            },
            {
              type: 'Selection',
              operator: 'range',
              arguments: [
                { type: 'Column', name: 'a' },
                { type: 'Column', name: 'b' }
              ]
            }
          ]
        }
      ]
    },
    'ast select verb'
  );

  t.end();
});

tape('Ungroup verb serializes to AST', t => {
  const verb = new Ungroup();

  t.deepEqual(
    toAST(verb),
    { verb: 'ungroup' },
    'ast ungroup verb'
  );

  t.end();
});

tape('Unorder verb serializes to AST', t => {
  const verb = new Unorder();

  t.deepEqual(
    toAST(verb),
    { verb: 'unorder' },
    'ast unorder verb'
  );

  t.end();
});

tape('Unroll verb serializes to AST', t => {
  t.deepEqual(
    toAST(new Unroll(['foo', 1])),
    {
      verb: 'unroll',
      values: [
        { type: 'Column', name: 'foo' },
        { type: 'Column', index: 1 }
      ]
    },
    'ast unroll verb'
  );

  t.deepEqual(
    toAST(new Unroll({
      foo: d => d.foo,
      bar: d => op.split(d.bar, ' ')
    })),
    {
      verb: 'unroll',
      values: [
        { type: 'Column', name: 'foo', as: 'foo' },
        {
          as: 'bar',
          type: 'CallExpression',
          callee: { type: 'Function', name: 'split' },
          arguments: [
            { type: 'Column', name: 'bar' },
            { type: 'Literal', value: ' ', raw: '\' \'' }
          ]
        }
      ]
    },
    'ast unroll verb, values object'
  );

  t.deepEqual(
    toAST(new Unroll(['foo'], { index: true })),
    {
      verb: 'unroll',
      values: [ { type: 'Column', name: 'foo' } ],
      options: { index: true }
    },
    'ast unroll verb, index boolean'
  );

  t.deepEqual(
    toAST(new Unroll(['foo'], { index: 'idxnum' })),
    {
      verb: 'unroll',
      values: [ { type: 'Column', name: 'foo' } ],
      options: { index: 'idxnum' }
    },
    'ast unroll verb, index string'
  );

  t.deepEqual(
    toAST(new Unroll(['foo'], { drop: [ 'bar' ] })),
    {
      verb: 'unroll',
      values: [ { type: 'Column', name: 'foo' } ],
      options: {
        drop: [ { type: 'Column', name: 'bar' } ]
      }
    },
    'ast unroll verb, drop column name'
  );

  t.deepEqual(
    toAST(new Unroll(['foo'], { drop: d => d.bar })),
    {
      verb: 'unroll',
      values: [ { type: 'Column', name: 'foo' } ],
      options: {
        drop: [ { type: 'Column', name: 'bar' } ]
      }
    },
    'ast unroll verb, drop table expression'
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
    toAST(verbSel),
    {
      verb: 'join',
      table: 'tableRef',
      on: [
        [ { type: 'Column', name: 'keyL' } ],
        [ { type: 'Column', name: 'keyR' } ]
      ],
      values: [
        [ { type: 'Selection', operator: 'all' } ],
        [ {
          type: 'Selection',
          operator: 'not',
          arguments: [ { type: 'Column', name: 'keyR' } ]
        } ]
      ],
      options: { suffix: ['_L', '_R'] }
    },
    'ast join verb, column selections'
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
    toAST(verbCols),
    {
      verb: 'join',
      table: 'tableRef',
      on: [
        [ { type: 'Column', name: 'keyL' } ],
        [ { type: 'Column', name: 'keyR' } ]
      ],
      values: [
        [
          { type: 'Column', name: 'keyL' },
          { type: 'Column', name: 'valL' },
          {
            as: 'foo',
            type: 'BinaryExpression',
            left: { type: 'Literal', 'value': 1, 'raw': '1' },
            operator: '+',
            right: { type: 'Column', name: 'valL' }
          }
        ],
        [
          { type: 'Column', name: 'valR' },
          {
            as: 'bar',
            type: 'BinaryExpression',
            left: { type: 'Literal', 'value': 2, 'raw': '2' },
            operator: '*',
            right: { type: 'Column', name: 'valR' }
          }
        ]
      ],
      options: { suffix: ['_L', '_R'] }
    },
    'ast join verb, column lists'
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
    toAST(verbExpr),
    {
      verb: 'join',
      table: 'tableRef',
      on: {
        type: 'CallExpression',
        callee: { type: 'Function', name: 'equal' },
        arguments: [
          { type: 'Column', table: 1, name: 'keyL' },
          { type: 'Column', table: 2, name: 'keyR' }
        ]
      },
      values: [
        { type: 'Column', table: 1, name: 'keyL', as: 'key' },
        { type: 'Column', table: 1, name: 'foo', as: 'foo' },
        { type: 'Column', table: 2, name: 'bar', as: 'bar' }
      ]
    },
    'ast join verb, table expressions'
  );

  t.end();
});

tape('Concat verb serializes to AST', t => {
  t.deepEqual(
    toAST(new Concat(['foo', 'bar'])),
    {
      verb: 'concat',
      tables: ['foo', 'bar']
    },
    'ast concat verb'
  );

  const ct1 = new QueryBuilder('foo').select(not('bar'));
  const ct2 = new QueryBuilder('bar').select(not('foo'));

  t.deepEqual(
    toAST(new Concat([ct1, ct2])),
    {
      verb: 'concat',
      tables: [ ct1.toAST(), ct2.toAST() ]
    },
    'ast concat verb, with subqueries'
  );

  t.end();
});