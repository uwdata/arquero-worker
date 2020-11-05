import { Verb } from './verb';

export default class Query {
  constructor(verbs, params) {
    this._verbs = verbs;
    this._params = params || null;
  }

  get length() {
    return this._verbs.length;
  }

  evaluate(table, catalog) {
    for (const verb of this._verbs) {
      table = verb.evaluate(table.params(this._params), catalog);
    }
    return table;
  }

  static from({ verbs, params }) {
    return new Query(verbs.map(Verb.from), params);
  }

  toObject() {
    return {
      verbs: this._verbs.map(verb => verb.toObject()),
      ...(this._params ? { params: this._params } : null)
    };
  }
}