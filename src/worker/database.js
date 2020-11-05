export default class Database {
  constructor() {
    this._tables = new Map();
  }

  get(name) {
    return this._tables.get(name);
  }

  set(name, table) {
    this._tables.set(name, table);
  }

  list() {
    return [...this._tables.keys()];
  }

  add(name, table) {
    if (this._tables.has(name)) {
      throw `Table already exists: "${name}"`;
    }
    this.set(name, table);
  }

  drop(name) {
    return this._tables.delete(name);
  }

  append(name, table) {
    this.set(name, this.get(name).concat(table));
  }

  query(name, query) {
    return query.evaluate(
      this.get(name),
      name => this.get(name)
    );
  }
}