import { internal } from 'arquero';
const { QueryBuilder } = internal;

/**
 * Query builder interface for web workers.
 */
export default class WorkerBuilder extends QueryBuilder {

  /**
   * Construct a new WorkerTable instance.
   */
  constructor(source, query, params, worker) {
    super(source, query, params);
    this._worker = worker;
  }

  /**
   * Instantiate a new query builder for the given source table
   * and worker instance.
   * @param {string} source The name of the table this query applies to.
   * @param {QueryWorker} worker The query worker for processing queries.
   */
  static for(source, worker) {
    return new WorkerBuilder(source, null, null, worker);
  }

  /**
   * Append a verb to the current query and return a new query builder.
   * @param {Verb} verb The verb to append to the query.
   * @return {WorkerBuilder} A new query builder with appended verb.
   */
  append(verb) {
    return new WorkerBuilder(
      this._table,
      this._verbs.concat(verb),
      this._params,
      this._worker
    );
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