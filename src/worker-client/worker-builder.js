import QueryBuilder from '../query/query-builder';

/**
 * Query builder interface for web workers.
 */
export default class WorkerBuilder extends QueryBuilder {

  /**
   * Construct a new WorkerTable instance.
   */
  constructor(query, params, source, worker) {
    super(query, params);
    this._source = source;
    this._worker = worker;
  }

  /**
   * Instantiate a new query builder for the given source table
   * and worker instance.
   * @param {string} source The name of the table this query applies to.
   * @param {QueryWorker} worker The query worker for processing queries.
   */
  static for(source, worker) {
    return new WorkerBuilder(null, null, source, worker);
  }

  /**
   * Return the name of the table this query applies to.
   * @return {string} The name of the backing table, or undefined.
   */
  name() {
    return !this._query.length ? this._source : undefined;
  }

  /**
   * Append a verb to the current query and return a new query builder.
   * @param {Verb} verb The verb to append to the query.
   * @return {WorkerBuilder} A new query builder with appended verb.
   */
  append(verb) {
    return new WorkerBuilder(
      this._query.concat(verb),
      this._params,
      this._source,
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
    return this._worker.query(
      this._source,
      this.query().toObject(),
      options
    );
  }
}