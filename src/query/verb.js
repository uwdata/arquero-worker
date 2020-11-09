import { all, desc, field, not, range, rolling } from 'arquero';
import error from '../util/error';
import isArray from '../util/is-array';
import isFunction from '../util/is-function';
import isNumber from '../util/is-number';
import isObject from '../util/is-object';
import isString from '../util/is-string';
import map from '../util/map-object';
import toArray from '../util/to-array';

function func(expr) {
  const f = d => d;
  f.toString = () => expr;
  return f;
}

function toObject(value) {
  return value && isFunction(value.toObject) ? value.toObject()
    : isFunction(value) ? { expr: String(value), func: true }
    : isArray(value) ? value.map(toObject)
    : isObject(value) ? map(value, _ => toObject(_))
    : value;
}

function fromObject(value) {
  return isArray(value) ? value.map(fromObject)
    : !isObject(value) ? value
    : isArray(value.all) ? all()
    : isArray(value.range) ? range(...value.range)
    : isArray(value.not) ? not(value.not.map(toObject))
    : fromExprObject(value);
}

function fromExprObject(value) {
  let output = value;
  let expr = value.expr;

  if (expr != null) {
    if (value.field === true) {
      output = expr = field(expr);
    } else if (value.func === true) {
      output = expr = func(expr);
    }

    if (isObject(value.window)) {
      const { frame, peers } = value.window;
      output = expr = rolling(expr, frame, peers);
    }

    if (value.desc === true) {
      output = desc(expr);
    }
  }

  return value === output
    ? map(value, _ => fromObject(_))
    : output;
}

function joinKeys(keys) {
  return isArray(keys) ? keys.map(parseJoinKeys)
    : keys;
}

function parseJoinKeys(keys, index) {
  const list = [];

  toArray(keys).forEach(param => {
    isNumber(param) ? list.push(param)
      : isString(param) ? list.push(field(param, null, index))
      : isObject(param) && param.expr ? list.push(param)
      : isFunction(param) ? list.push(param)
      : error(`Invalid key value: ${param+''}`);
  });

  return list;
}

function joinValues(values) {
  return isArray(values)
    ? values.map(parseJoinValues)
    : values;
}

function parseJoinValues(values, index) {
  return index < 2 ? toArray(values) : values;
}

function orderbyKeys(keys) {
  const list = [];

  keys.forEach(param => {
    const expr = param.expr != null ? param.expr : param;
    if (isObject(expr) && !isFunction(expr)) {
      for (const key in expr) {
        list.push(expr[key]);
      }
    } else {
      param = isNumber(expr) ? expr
        : isString(expr) ? field(param)
        : isFunction(expr) ? param
        : error(`Invalid orderby field: ${param+''}`);
      list.push(param);
    }
  });

  return list;
}

export class Verb {
  constructor(verb, params) {
    this.__verb = verb;
    this.__params = params || [];
  }

  // eslint-disable-next-line no-unused-vars
  evaluate(table, catalog) {
    throw 'Abstract class method.';
  }

  toObject() {
    const obj = { verb: this.__verb };
    this.__params.forEach(name => obj[name] = toObject(this[`_${name}`]));
    return obj;
  }

  static from(object) {
    const Type = Verbs[object.verb];
    return new Type(...(Type.params || [])
      .map(name => fromObject(object[name])));
  }
}

export class Reify extends Verb {
  constructor() {
    super('reify');
  }
  evaluate(table) {
    return table.reify();
  }
}

export class Count extends Verb {
  constructor(options) {
    super('count', Count.params);
    this._options = options;
  }
  evaluate(table) {
    return table.count(this._options);
  }
}
Count.params = ['options'];

export class Dedupe extends Verb {
  constructor(keys) {
    super('dedupe', Dedupe.params);
    this._keys = keys || [];
  }
  evaluate(table) {
    return table.dedupe(this._keys);
  }
}
Dedupe.params = ['keys'];

export class Derive extends Verb {
  constructor(values) {
    super('derive', Derive.params);
    this._values = values;
  }
  evaluate(table) {
    return table.derive(this._values);
  }
}
Derive.params = ['values'];

export class Filter extends Verb {
  constructor(criteria) {
    super('filter', Filter.params);
    this._criteria = criteria;
  }
  evaluate(table) {
    return table.filter(this._criteria);
  }
}
Filter.params = ['criteria'];

export class Groupby extends Verb {
  constructor(keys) {
    super('groupby', Groupby.params);
    this._keys = keys;
  }
  evaluate(table) {
    return table.groupby(this._keys);
  }
}
Groupby.params = ['keys'];

export class Orderby extends Verb {
  constructor(keys) {
    super('orderby', Orderby.params);
    this._keys = orderbyKeys(keys);
  }
  evaluate(table) {
    return table.orderby(this._keys);
  }
}
Orderby.params = ['keys'];

export class Rollup extends Verb {
  constructor(values) {
    super('rollup', Rollup.params);
    this._values = values;
  }
  evaluate(table) {
    return table.rollup(this._values);
  }
}
Rollup.params = ['values'];

export class Sample extends Verb {
  constructor(size, options) {
    super('sample', Sample.params);
    this._size = size;
    this._options = options;
  }
  evaluate(table) {
    return table.sample(this._size, this._options);
  }
}
Sample.params = ['size', 'options'];

export class Select extends Verb {
  constructor(columns) {
    super('select', Select.params);
    this._columns = columns;
  }
  evaluate(table) {
    return table.select(this._columns);
  }
}
Select.params = ['columns'];

export class Ungroup extends Verb {
  constructor() {
    super('ungroup');
  }
  evaluate(table) {
    return table.ungroup();
  }
}

export class Unorder extends Verb {
  constructor() {
    super('unorder');
  }
  evaluate(table) {
    return table.ungroup();
  }
}

export class Fold extends Verb {
  constructor(values, options) {
    super('fold', Fold.params);
    this._values = values;
    this._options = options;
  }
  evaluate(table) {
    return table.fold(this._values, this._options);
  }
}
Fold.params = ['values', 'options'];

export class Pivot extends Verb {
  constructor(keys, values, options) {
    super('pivot', Pivot.params);
    this._keys = keys;
    this._values = values;
    this._options = options;
  }
  evaluate(table) {
    return table.pivot(this._keys, this._values, this._options);
  }
}
Pivot.params = ['keys', 'values', 'options'];

export class Spread extends Verb {
  constructor(values, options) {
    super('spread', Spread.params);
    this._values = values;
    this._options = options;
  }
  evaluate(table) {
    return table.spread(this._values, this._options);
  }
}
Spread.params = ['values', 'options'];

export class Unroll extends Verb {
  constructor(values, options) {
    super('unroll', Unroll.params);
    this._values = values;
    this._options = options;
  }
  evaluate(table) {
    return table.unroll(this._values, this._options);
  }
}
Unroll.params = ['values', 'options'];

export class Lookup extends Verb {
  constructor(table, on, values) {
    super('lookup', Lookup.params);
    this._table = table;
    this._on = joinKeys(on);
    this._values = values;
  }
  evaluate(table, catalog) {
    return table.lookup(
      catalog(this._table),
      this._on,
      this._values
    );
  }
}
Lookup.params = ['table', 'on', 'values'];

export class Join extends Verb {
  constructor(table, on, values, options) {
    super('join', Join.params);
    this._table = table;
    this._on = joinKeys(on);
    this._values = joinValues(values);
    this._options = options;
  }
  evaluate(table, catalog) {
    return table.join(
      catalog(this._table),
      this._on,
      this._values,
      this._options
    );
  }
}
Join.params = ['table', 'on', 'values', 'options'];

export class Cross extends Verb {
  constructor(table, values, options) {
    super('cross', Cross.params);
    this._table = table;
    this._values = joinValues(values);
    this._options = options;
  }
  evaluate(table, catalog) {
    return table.cross(
      catalog(this._table),
      this._values,
      this._options
    );
  }
}
Cross.params = ['table', 'values', 'options'];

export class Semijoin extends Verb {
  constructor(table, on) {
    super('semijoin', Semijoin.params);
    this._table = table;
    this._on = joinKeys(on);
  }
  evaluate(table, catalog) {
    return table.semijoin(
      catalog(this._table),
      this._on
    );
  }
}
Semijoin.params = ['table', 'on'];

export class Antijoin extends Verb {
  constructor(table, on) {
    super('antijoin', Antijoin.params);
    this._table = table;
    this._on = joinKeys(on);
  }
  evaluate(table, catalog) {
    return table.antijoin(
      catalog(this._table),
      this._on
    );
  }
}
Antijoin.params = ['table', 'on'];

export class Concat extends Verb {
  constructor(tables) {
    super('concat', Concat.params);
    this._tables = tables;
  }
  evaluate(table, catalog) {
    const tables = this._tables.map(catalog);
    return table.concat(tables);
  }
}
Concat.params = ['tables'];

export class Union extends Verb {
  constructor(tables) {
    super('union', Union.params);
    this._tables = tables;
  }
  evaluate(table, catalog) {
    const tables = this._tables.map(catalog);
    return table.union(tables);
  }
}
Union.params = ['tables'];

export class Intersect extends Verb {
  constructor(tables) {
    super('intersect', Intersect.params);
    this._tables = tables;
  }
  evaluate(table, catalog) {
    const tables = this._tables.map(catalog);
    return table.intersect(tables);
  }
}
Intersect.params = ['tables'];

export class Except extends Verb {
  constructor(tables) {
    super('except', Except.params);
    this._tables = tables;
  }
  evaluate(table, catalog) {
    const tables = this._tables.map(catalog);
    return table.except(tables);
  }
}
Except.params = ['tables'];

export const Verbs = {
  count:     Count,
  dedupe:    Dedupe,
  derive:    Derive,
  filter:    Filter,
  groupby:   Groupby,
  orderby:   Orderby,
  rollup:    Rollup,
  sample:    Sample,
  select:    Select,
  ungroup:   Ungroup,
  unorder:   Unorder,
  fold:      Fold,
  pivot:     Pivot,
  spread:    Spread,
  unroll:    Unroll,
  lookup:    Lookup,
  join:      Join,
  cross:     Cross,
  semijoin:  Semijoin,
  antijoin:  Antijoin,
  concat:    Concat,
  union:     Union,
  intersect: Intersect,
  except:    Except
};