const { Pool } = require('undici');
const { Readable } = require('stream');
const qs = require('querystring');
const defaults = require('lodash.defaults');
const assert = require('assert');
const split2 = require('split2');
// const binarySplit = require('binary-split');
const util = require('util');
const { BufferListStream } = require('bl');
const debug = require('debug')('makeomatic:clickhouse');
const {
  JSONStream,
  RecordStream,
  CounterStream,
  StreamingParser,
} = require('./streams');
const parseError = require('./parse-error');
const { LIBRARY_SPECIFIC_OPTIONS } = require('./consts');

// eslint-disable-next-line max-len
const kFormatRegexp = /FORMAT\s+(BlockTabSeparated|CSV|CSVWithNames|JSON|JSONCompact|JSONEachRow|Native|Null|Pretty|PrettyCompact|PrettyCompactMonoBlock|PrettyNoEscapes|PrettyCompactNoEscapes|PrettySpaceNoEscapes|PrettySpace|RowBinary|TabSeparated|TabSeparatedRaw|TabSeparatedWithNames|TabSeparatedWithNamesAndTypes|TSKV|Values|Vertical|XML)/i;
const pipeline = util.promisify(require('stream').pipeline);

/**
 * @typedef { import('undici').Dispatcher.ResponseData } ResponseData
 * @typedef { ResponseData['body'] } ResponseBody
 */

/**
 * HTTP response handler
 * @template T
 * @param {NodeJS.WritableStream} stream
 * @param {Record<string, any>} reqData
 * @param {(err: Error | null, response: T) => void} [cb]
 * @param {ResponseData} response
 */
async function httpResponseHandler(stream, reqData, cb, response) {
  const contentType = response.headers['content-type'];
  const isError = response.statusCode !== 200;
  const isStreamingParser = /EachRow/.test(reqData.format);
  const isJSON = contentType && contentType.indexOf('application/json') === 0;
  const streamingParse = (isJSON || isStreamingParser)
    && !reqData.syncParser
    && isError === false;

  const counter = new CounterStream(stream);

  /**
   * @param {Error} e - unparsed error
   */
  function errorHandler(e, passThrough) {
    // error called
    const err = passThrough ? e : parseError(e);

    // user should define callback or add event listener for the error event
    if (!cb || (cb && stream.listenerCount('error'))) {
      stream.emit('error', err);
    }

    if (cb) cb(err);
  }

  let str;
  if (isStreamingParser) {
    const parser = new StreamingParser(reqData.format, stream);
    // await pipeline(response.body, counter, binarySplit(), parser);
    await pipeline(response.body, counter, split2(JSON.parse), parser);
  } else if (streamingParse) {
    const parser = new JSONStream(stream);
    // await pipeline(response.body, counter, binarySplit(), parser);
    await pipeline(response.body, counter, split2(), parser);
  } else {
    await pipeline(response.body, counter, new BufferListStream((err, buf) => {
      if (err) {
        errorHandler(err, true);
        return;
      }

      str = buf;
    }));
  }

  if (isError) {
    errorHandler(str);
    return;
  }

  let data;
  if (!isError && (streamingParse || isStreamingParser)) {
    if (cb) {
      cb(null, {
        ...stream.supplemental,
        meta: stream.meta,
        transferred: stream.transferred,
      });
    }
    return;
  }

  // Early return and stream end in case when content-type means empty body
  if (!isError && (
    !contentType
    || contentType.indexOf('text/plain') === 0
    || contentType.indexOf('text/html') === 0 // WTF: xenial - no content-type, precise - text/html
  )) {
    // probably this is a ping response or any other successful response with *empty* body
    if (cb) cb(null, str.toString('utf8'));
    return;
  }

  let supplemental = Object.create(null);

  // we already pushed all the data
  if (stream.meta && stream.meta.length) {
    try {
      supplemental = JSON.parse(`{${str.toString('utf8')}`);
    } catch (e) {
      // TODO
    }
    stream.supplemental = supplemental;

    // end stream
    stream.push(null);

    if (cb) {
      cb(null, {
        ...supplemental,
        meta: stream.meta,
        transferred: stream.transferred,
      });
    }

    return;
  }

  // one shot data parsing, should be much faster for smaller datasets
  try {
    data = JSON.parse(str.toString('utf8'));

    data.transferred = stream.transferred;

    if (data.meta) {
      stream.emit('metadata', data.meta);
    }

    if (data.data) {
      // no highWatermark support
      data.data.forEach((row) => {
        stream.push(row);
      });
    }
  } catch (e) {
    if (!reqData.format || !reqData.format.match(/^(JSON|JSONCompact)$/)) {
      data = str.toString('utf8');
    } else {
      errorHandler(e);
      return;
    }
  } finally {
    if (!stream.readableEnded) {
      stream.push(null);
      if (cb) cb(null, data);
    }
  }

  // In case of error, we're just throw away data
  // response.on('error', errorHandler);
  // response.on('aborted', () => {
  //   aborted = true;
  //   const err = new Error('aborted');
  //   err.code = 'ECONNRESET';

  //   // user should define callback or add event listener for the error event
  //   if (!cb || (cb && stream.listenerCount('error'))) {
  //     stream.emit('error', err);
  //   }

  //   if (cb) cb(err);
  // });
}

class ClickHouse {
  constructor(options) {
    this.options = typeof options === 'string'
      ? { host: options }
      : options;

    assert(this.options, 'You must provide at least host name to query ClickHouse');
    assert(this.options.host, 'You must provide at least host name to query ClickHouse');

    defaults(this.options, {
      protocol: 'http:',
      port: 8123,
      poolOptions: process.env.NODE_ENV === 'test' ? {
        connections: 75,
        bodyTimeout: null,
        headersTimeout: null,
        // keepAliveMaxTimeout: 100,
        // keepAliveTimeout: 100,
      } : {
        connections: 100,
      },
    });

    const { protocol, host, port } = this.options;
    const { user, password } = this.options;
    let origin = `${host}:${port}`;

    if (user || password) {
      origin = `${user || 'default'}:${password || ''}@${origin}`;
    }

    /**
     * @type {Pool}
     */
    this.client = new Pool(`${protocol}//${origin}`, this.options.poolOptions);
  }

  async close() {
    await this.client.close();
  }

  httpRequest(reqParams, reqData, cb) {
    if (reqParams.query) {
      reqParams.path = `${reqParams.pathname || reqParams.path}?${qs.stringify(reqParams.query)}`;
    }

    const stream = new RecordStream({ format: reqData.format });
    (async () => {
      try {
        debug('[%j] request', reqParams);
        const body = new Readable({ read() {} });
        const req = this.client.request({
          ...reqParams,
          body,
        });

        if (reqData.query) {
          body.push(reqData.query);
        }

        if (reqData.finalized) {
          body.push(null);
        }

        stream.req = body;
        const response = await req;
        await httpResponseHandler(stream, reqData, cb, response);
        debug('[%j] response handled', reqParams);
      } catch (e) {
        debug('[%j] response error', reqParams, e);
        if (!cb || (cb && stream.listenerCount('error'))) {
          stream.emit('error', e);
        }

        if (cb) cb(e);
      }
    })();

    return stream;
  }

  getReqParams() {
    const urlObject = Object.create(null);

    // avoid to set defaults - node http module is not happy
    for (const name of Object.keys(this.options)) {
      if (!LIBRARY_SPECIFIC_OPTIONS.has(name)) {
        urlObject[name] = this.options[name];
      }
    }

    urlObject.method = 'POST';
    urlObject.path = urlObject.path || '/';

    return urlObject;
  }

  query(_chQuery, _options, _cb) {
    const chQuery = _chQuery.trim();

    let cb = _cb;
    let options = _options;

    if (cb === undefined && options && typeof options === 'function') {
      cb = options;
      options = undefined;
    }

    if (!options) {
      options = {
        queryOptions: Object.create(null),
      };
    }

    options.omitFormat = options.omitFormat || this.options.omitFormat || false;
    options.dataObjects = options.dataObjects || this.options.dataObjects || false;
    options.format = options.format || this.options.format || null;
    options.readonly = options.readonly || this.options.readonly || this.options.useQueryString || false;

    // we're adding `queryOptions` passed for constructor if any
    const queryObject = { ...this.options.queryOptions, ...options.queryOptions };
    const formatMatch = chQuery.match(kFormatRegexp);

    if (!options.omitFormat && formatMatch) {
      [, options.format] = formatMatch;
      options.omitFormat = true;
    }

    const reqData = {
      syncParser: options.syncParser || this.options.syncParser || false,
      finalized: true, // allows to write records into connection stream
    };

    const reqParams = this.getReqParams();

    let formatEnding = '';

    // format should be added for data queries
    if (chQuery.match(/^(?:SELECT|WITH|SHOW|DESC|DESCRIBE|EXISTS\s+TABLE)/i)) {
      if (!options.format) options.format = options.dataObjects ? 'JSON' : 'JSONCompact';
    } else if (chQuery.match(/^INSERT/i)) {
      // There is some variants according to the documentation:
      // 1. Values already available in the query: INSERT INTO t VALUES (1),(2),(3)
      // 2. Values must me provided with POST data: INSERT INTO t VALUES
      // 3. Same as previous but without VALUES keyword: INSERT INTO t FORMAT Values
      // 4. Insert from SELECT: INSERT INTO t SELECT…

      // we need to handle 2 and 3 and http stream must stay open in that cases
      if (chQuery.match(/\s+VALUES\b/i)) {
        if (chQuery.match(/\s+VALUES\s*$/i)) reqData.finalized = false;

        options.format = 'Values';
        options.omitFormat = true;
      } else if (chQuery.match(/INSERT\s+INTO\s+\S+\s+(?:\([^)]+\)\s+)?SELECT/mi)) {
        reqData.finalized = true;
        options.omitFormat = true;
      } else {
        reqData.finalized = false;

        // Newline is recommended https://clickhouse.yandex/docs/en/query_language/insert_into/#insert
        formatEnding = '\n';
        if (!chQuery.match(/FORMAT/i)) {
          // simplest format to use, only need to escape \t, \\ and \n
          options.format = options.format || 'TabSeparated';
        } else {
          options.omitFormat = true;
        }
      }
    } else {
      options.omitFormat = true;
    }

    reqData.format = options.format;

    // use query string to submit ClickHouse query — useful to mock CH server
    if (options.readonly) {
      queryObject.query = chQuery + ((options.omitFormat) ? '' : ` FORMAT ${options.format}${formatEnding}`);
      reqParams.method = 'GET';
    } else {
      // Trimmed query still may require `formatEnding` when FORMAT clause specified in query
      reqData.query = chQuery + (options.omitFormat ? '' : ` FORMAT ${options.format}`) + formatEnding;
      reqParams.method = 'POST';
    }

    reqParams.query = queryObject;

    return this.httpRequest(reqParams, reqData, cb);
  }

  querying(chQuery, options) {
    return new Promise((resolve, reject) => {
      // Force override `syncParser` option when using promise api
      const queryOptions = { ...options, syncParser: true };
      this.query(chQuery, queryOptions, (err, data) => {
        if (err) {
          reject(err);
          return;
        }

        resolve(data);
      });
    });
  }

  ping(cb) {
    const reqParams = this.getReqParams();
    reqParams.method = 'GET';

    const stream = this.httpRequest(reqParams, { finalized: true }, cb);
    return stream;
  }

  pinging() {
    return new Promise((resolve, reject) => {
      const reqParams = this.getReqParams();

      reqParams.method = 'GET';

      this.httpRequest(reqParams, { finalized: true }, (err, data) => {
        if (err) {
          reject(err);
          return;
        }

        resolve(data);
      });
    });
  }
}

module.exports = ClickHouse;
