const http = require('http');
const https = require('https');
const qs = require('querystring');
const assert = require('assert');
const { RecordStream } = require('./streams');
const { JSONStream } = require('./streams');
const parseError = require('./parse-error');
const { LIBRARY_SPECIFIC_OPTIONS } = require('./consts');

/**
 * HTTP response handler
 * @template T
 * @param {NodeJS.WritableStream} stream
 * @param {Record<string, any>} reqParams
 * @param {Record<string, any>} reqData
 * @param {(err: Error | null, response: T) => void} [cb]
 * @param {NodeJS.ReadableStream} response
 */
function httpResponseHandler(stream, reqParams, reqData, cb, response) {
  let str;
  let error;
  let aborted = false;

  if (response.statusCode === 200) {
    str = Buffer.allocUnsafe(0);
  } else {
    error = Buffer.allocUnsafe(0);
  }

  /**
   * @param {Error} e - unparsed error
   */
  function errorHandler(e) {
    if (aborted) return;

    // error called
    const err = parseError(e);

    // user should define callback or add event listener for the error event
    if (!cb || (cb && stream.listenerCount('error'))) {
      stream.emit('error', err);
    }

    if (cb) cb(err);
  }

  // TODO: use streaming interface
  // from https://github.com/jimhigson/oboe.js
  // or https://www.npmjs.com/package/stream-json or
  // or https://github.com/creationix/jsonparse

  // or implement it youself
  let jsonParser;
  if (/EachRow/.test(reqData.format)) {
    let linesProcessed = 0;

    const isCompact = /Compact/.test(reqData.format);
    const withProgress = /WithProgress/.test(reqData.format);
    const withNamesAndTypes = isCompact && /WithNamesAndTypes/.test(reqData.format);

    jsonParser = (line) => {
      const data = JSON.parse(line);

      if (withNamesAndTypes && linesProcessed < 2) {
        if (linesProcessed === 0) {
          jsonParser.columns = data;
        } else if (linesProcessed === 1) {
          jsonParser.columns = jsonParser.columns.map((name, idx) => ({
            name,
            type: data[idx],
          }));
          stream.emit('metadata', jsonParser.columns);
        }
      } else if (withProgress && data.progress) {
        stream.emit('progress', data.progress);
      } else if (withProgress) {
        jsonParser.rows.push(data.row);
      } else {
        jsonParser.rows.push(data);
      }

      linesProcessed += 1;
    };

    jsonParser.rows = [];
    jsonParser.columns = [];
  } else {
    jsonParser = JSONStream(stream);
  }

  let symbolsTransferred = 0;

  const streamingParse = response.headers['content-type']
    && response.headers['content-type'].indexOf('application/json') === 0
    && !reqData.syncParser
    && str;

  // In case of error, we're just throw away data
  response.on('error', errorHandler);
  response.on('aborted', () => {
    aborted = true;
    const err = new Error('aborted');
    err.code = 'ECONNRESET';

    // user should define callback or add event listener for the error event
    if (!cb || (cb && stream.listenerCount('error'))) {
      stream.emit('error', err);
    }

    if (cb) cb(err);
  });

  // another chunk of data has been received, so append it to `str`
  response.on('data', (chunk) => {
    symbolsTransferred += chunk.length;
    const newLinePos = chunk.lastIndexOf('\n');

    // JSON response
    if (streamingParse && newLinePos !== -1) {
      // store in buffer anything after
      const remains = chunk.slice(newLinePos + 1);

      Buffer.concat([str, chunk.slice(0, newLinePos)])
        .toString('utf8')
        .split('\n')
        .forEach(jsonParser);

      jsonParser.rows.forEach((row) => {
        // write to readable stream
        stream.push(row);
      });

      jsonParser.rows = [];
      str = remains;

      // plaintext response
    } else if (str) {
      str = Buffer.concat([str, chunk]);
    } else {
      error = Buffer.concat([error, chunk]);
    }
  });

  // the whole response has been received, so we just print it out here
  response.on('end', () => {
    // debug (response.headers);

    if (error) {
      errorHandler(error);
      return;
    }

    let data;

    const contentType = response.headers['content-type'];

    // Early return and stream end in case when content-type means empty body
    if (response.statusCode === 200 && (
      !contentType
      || contentType.indexOf('text/plain') === 0
      || contentType.indexOf('text/html') === 0 // WTF: xenial - no content-type, precise - text/html
    )) {
      // probably this is a ping response or any other successful response with *empty* body
      stream.push(null);
      if (cb) cb(null, str.toString('utf8'));
      return;
    }

    let supplemental = {};

    // we already pushed all the data
    if (jsonParser.columns.length) {
      try {
        supplemental = JSON.parse(jsonParser.supplementalString + str.toString('utf8'));
      } catch (e) {
        // TODO
      }
      stream.supplemental = supplemental;

      // end stream
      stream.push(null);

      if (cb) {
        cb(null, {
          ...supplemental,
          meta: jsonParser.columns,
          transferred: symbolsTransferred,
        });
      }

      return;
    }

    // one shot data parsing, should be much faster for smaller datasets
    try {
      data = JSON.parse(str.toString('utf8'));

      data.transferred = symbolsTransferred;

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
  });
}

function httpRequest(reqParams, reqData, cb) {
  if (reqParams.query) {
    reqParams.path = `${reqParams.pathname || reqParams.path}?${qs.stringify(reqParams.query)}`;
  }

  const stream = new RecordStream({ format: reqData.format });
  const requestInstance = reqParams.protocol === 'https:' ? https : http;
  const req = requestInstance.request(reqParams, httpResponseHandler.bind(
    this, stream, reqParams, reqData, cb
  ));

  req.on('error', (e) => {
    // user should define callback or add event listener for the error event
    if (!cb || (cb && stream.listenerCount('error'))) {
      stream.emit('error', e);
    }

    if (cb) cb(e);
  });

  req.on('timeout', (e) => {
    req.destroy(e);
  });

  stream.req = req;

  if (reqData.query) req.write(reqData.query);

  if (reqData.finalized) {
    req.end();
  }

  return stream;
}

function ClickHouse(options) {
  assert(options, 'You must provide at least host name to query ClickHouse');

  this.options = typeof options === 'string'
    ? { host: options }
    : options;
}

ClickHouse.prototype.getReqParams = function getReqParams() {
  const urlObject = {};

  // avoid to set defaults - node http module is not happy
  for (const name of Object.keys(this.options)) {
    if (!LIBRARY_SPECIFIC_OPTIONS.has(name)) {
      urlObject[name] = this.options[name];
    }
  }

  if ('user' in this.options || 'password' in this.options) {
    urlObject.auth = `${this.options.user || 'default'}:${this.options.password || ''}`;
  }

  urlObject.method = 'POST';

  urlObject.path = urlObject.path || '/';

  urlObject.port = urlObject.port || 8123;

  return urlObject;
};

ClickHouse.prototype.query = function query(_chQuery, _options, _cb) {
  const chQuery = _chQuery.trim();

  let cb = _cb;
  let options = _options;

  if (cb === undefined && options && typeof options === 'function') {
    cb = options;
    options = undefined;
  }

  if (!options) {
    options = {
      queryOptions: {},
    };
  }

  options.omitFormat = options.omitFormat || this.options.omitFormat || false;
  options.dataObjects = options.dataObjects || this.options.dataObjects || false;
  options.format = options.format || this.options.format || null;
  options.readonly = options.readonly || this.options.readonly || this.options.useQueryString || false;

  // we're adding `queryOptions` passed for constructor if any
  const queryObject = { ...this.options.queryOptions, ...options.queryOptions };

  // eslint-disable-next-line max-len
  const formatRegexp = /FORMAT\s+(BlockTabSeparated|CSV|CSVWithNames|JSON|JSONCompact|JSONEachRow|Native|Null|Pretty|PrettyCompact|PrettyCompactMonoBlock|PrettyNoEscapes|PrettyCompactNoEscapes|PrettySpaceNoEscapes|PrettySpace|RowBinary|TabSeparated|TabSeparatedRaw|TabSeparatedWithNames|TabSeparatedWithNamesAndTypes|TSKV|Values|Vertical|XML)/i;
  const formatMatch = chQuery.match(formatRegexp);

  if (!options.omitFormat && formatMatch) {
    // eslint-disable-next-line prefer-destructuring
    options.format = formatMatch[1];
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

      // Newline is recomended https://clickhouse.yandex/docs/en/query_language/insert_into/#insert
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

  const stream = httpRequest(reqParams, reqData, cb);

  return stream;
};

ClickHouse.prototype.querying = function querying(chQuery, options) {
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
};

ClickHouse.prototype.ping = function ping(cb) {
  const reqParams = this.getReqParams();

  reqParams.method = 'GET';

  const stream = httpRequest(reqParams, { finalized: true }, cb);

  return stream;
};

ClickHouse.prototype.pinging = function pinging() {
  return new Promise((resolve, reject) => {
    const reqParams = this.getReqParams();

    reqParams.method = 'GET';

    httpRequest(reqParams, { finalized: true }, (err, data) => {
      if (err) {
        reject(err);
        return;
      }

      resolve(data);
    });
  });
};

module.exports = ClickHouse;
