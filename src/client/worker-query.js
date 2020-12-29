import { internal } from 'arquero';
const { Query, Verbs } = internal;

/**
 * Query builder interface for web workers.
 */
export default class WorkerQuery extends Query {

  /**
   * Construct a new WorkerQuery instance.
   */
  constructor(source, verbs, params, worker) {
    super(verbs, params, source);
    this._worker = worker;
  }

  /**
   * Instantiate a new query builder for the given source table
   * and worker instance.
   * @param {string} source The name of the table this query applies to.
   * @param {WorkerQuery} worker The query worker for processing queries.
   */
  static for(source, worker) {
    return new WorkerQuery(source, null, null, worker);
  }

  /**
   * Submit a query to the backing query worker and return a
   * Promise for the result.
   * @param {Object} options Query options, including the row
   *  limit and selected columns for resulting tables.
   * @return {Promise} A Promise for the query results.
   */
  fetch(options) {
    return this._worker.query(this.toObject(), options);
  }
}

// Internal verb handlers
for (const name in Verbs) {
  const verb = Verbs[name];
  WorkerQuery.prototype['__' + name] = (q, ...args) => new WorkerQuery(
    q._table,
    q._verbs.concat(verb(...args)),
    q._params,
    q._worker
  );
}