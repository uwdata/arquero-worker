import error from '../util/error';
import isArray from '../util/is-array';
import isNumber from '../util/is-number';
import isObject from '../util/is-object';
import isString from '../util/is-string';
import map from '../util/map-object';
import toArray from '../util/to-array';
import { isSelection, toObject } from './util';

import {
  Column,
  Expr,
  ExprList,
  ExprNumber,
  ExprObject,
  JoinKeys,
  JoinValues,
  Options,
  OrderbyKeys,
  TableRef,
  TableRefList
} from './constants';

import { internal } from 'arquero';
import isFunction from '../util/is-function';
const { parse } = internal;

const Methods = {
  [Expr]: astExpr,
  [ExprList]: astExprList,
  [ExprNumber]: astExprNumber,
  [ExprObject]: astExprObject,
  [JoinKeys]: astJoinKeys,
  [JoinValues]: astJoinValues,
  [OrderbyKeys]: astExprList
};

export default function(value, type, propTypes) {
  return type === TableRef ? astTableRef(value)
    : type === TableRefList ? value.map(astTableRef)
    : ast(toObject(value), type, propTypes);
}

function ast(value, type, propTypes) {
  return type === Options
    ? (value ? astOptions(value, propTypes) : value)
    : Methods[type](value);
}

function astOptions(value, types = {}) {
  const output = {};
  for (const key in value) {
    const prop = value[key];
    output[key] = types[key] ? ast(prop, types[key]) : prop;
  }
  return output;
}

function astParse(expr, opt) {
  return parse({ expr }, { ...opt, ast: true }).expr;
}

function astColumn(name) {
  return { type: Column, name };
}

function astColumnIndex(index) {
  return { type: Column, index };
}

function astExprObject(obj, opt) {
  if (isString(obj)) {
    return astParse(obj, opt);
  }

  if (obj.expr) {
    let ast;
    if (obj.field === true) {
      ast = astColumn(obj.expr);
    } else if (obj.func === true) {
      ast = astExprObject(obj.expr, opt);
    }
    if (ast) {
      if (obj.desc) {
        ast = {
          type: 'Descending',
          expr: ast
        };
      }
      if (obj.window) {
        ast = {
          type: 'Window',
          expr: ast,
          ...obj.window
        };
      }
      return ast;
    }
  }

  return {
    type: 'Expressions',
    values: map(obj, val => astExprObject(val, opt))
  };
}

function astSelection(sel) {
  const type = 'Selection';
  return sel.all ? { type, operator: 'all' }
    : sel.not ? { type, operator: 'not', arguments: astExprList(sel.not) }
    : sel.range ? { type, operator: 'range', arguments: astExprList(sel.range) }
    : error('Invalid input');
}

function astExpr(val) {
  return isSelection(val) ? astSelection(val)
    : isNumber(val) ? astColumnIndex(val)
    : isString(val) ? astColumn(val)
    : isObject(val) ? astExprObject(val)
    : error('Invalid input');
}

function astExprList(arr) {
  return toArray(arr).map(astExpr);
}

function astExprNumber(val) {
  return isNumber(val) ? val : astExprObject(val);
}

function astJoinKeys(val) {
  return isArray(val)
    ? val.map(astExprList)
    : astExprObject(val, { join: true });
}

function astJoinValues(val) {
  return isArray(val)
    ? val.map((v, i) => i < 2
        ? astExprList(v)
        : astExprObject(v, { join: true })
      )
    : astExprObject(val, { join: true });
}

function astTableRef(value) {
  return value && isFunction(value.toAST)
    ? value.toAST()
    : value;
}