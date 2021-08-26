/* eslint-disable max-classes-per-file */
const util = require('util');
const { Duplex, Transform, Writable } = require('stream');
const { encodeRow } = require('./process-db-value');

/**
 * @typedef {(l: string) => void} LineProcessor
 * @property {{ name: string, type: string }[]} columns
 * @property {any[]} rows
 * @property {string} supplementalString
 */

class StreamingParser extends Writable {
  constructor(format, stream) {
    super({ objectMode: true });

    this.isCompact = /Compact/.test(format);
    this.withProgress = /WithProgress/.test(format);
    this.withNamesAndTypes = this.isCompact && /WithNamesAndTypes/.test(format);
    this.linesProcessed = 0;
    this.columns = [];
    this.output = stream;
  }

  _write(chunk, encoding, callback) {
    const data = encoding === 'binary' ? JSON.parse(chunk) : chunk;

    if (this.withNamesAndTypes && this.linesProcessed < 2) {
      if (this.linesProcessed === 0) {
        this.columns = data;
      } else if (this.linesProcessed === 1) {
        this.columns = this.columns.map((name, idx) => ({
          name,
          type: data[idx],
        }));

        this.output.emit('metadata', this.columns);
        this.output.meta = this.columns;
      }
    } else if (this.withProgress && data.progress) {
      this.output.emit('progress', data.progress);
    } else if (this.withProgress) {
      this.output.push(data.row);
    } else {
      this.output.push(data);
    }

    this.linesProcessed += 1;
    callback(null);
  }

  _final(callback) {
    this.output.push(null);
    callback();
  }
}

class JSONStream extends Writable {
  constructor(stream) {
    super({ objectMode: true });
    this.objBuffer = undefined;
    this.state = null;
    this.columns = [];
    this.supplementalString = '{';
    this.output = stream;
  }

  _write(chunk, encoding, callback) {
    const l = encoding === 'buffer'
      ? chunk.toString('utf8').trim()
      : chunk.trim();

    if (!l.length) {
      return callback();
    }

    if (this.state === null) {
      // first string should contains `{`
      if (l === '{') {
        this.state = 'topKeys';
      }
    } else if (this.state === 'topKeys') {
      if (l === '"meta":') {
        this.state = 'meta';
      } else if (l === '"data":') {
        this.state = 'data';
      } else if (l === '"meta": [') {
        this.state = 'meta-array';
      } else if (l === '"data": [') {
        this.state = 'data-array';
      } else {
        this.supplementalString += l;
      }
    } else if (this.state === 'meta') {
      if (l === '[') {
        this.state = 'meta-array';
      }
    } else if (this.state === 'data') {
      if (l === '[') {
        this.state = 'data-array';
      }
    } else if (this.state === 'meta-array') {
      if (l.match(/^},?$/)) {
        this.columns.push(JSON.parse(`${this.objBuffer}}`));
        this.objBuffer = undefined;
      } else if (l === '{') {
        this.objBuffer = l;
      } else if (l.match(/^],?$/)) {
        this.output.emit('metadata', this.columns);
        this.output.meta = this.columns;
        this.state = 'topKeys';
      } else {
        this.objBuffer += l;
      }
    } else if (this.state === 'data-array') {
      if (l.match(/^[\]}],?$/) && this.objBuffer) {
        this.output.push(JSON.parse(this.objBuffer + l[0]));
        this.objBuffer = undefined;
      } else if (l === '{' || l === '[') {
        this.objBuffer = l;
      } else if (l.match(/^],?$/)) {
        this.state = 'topKeys';
      } else if (this.objBuffer === undefined) {
        this.output.push(JSON.parse(l[l.length - 1] !== ',' ? l : l.substr(0, l.length - 1)));
      } else {
        this.objBuffer += l;
      }
    }

    return callback();
  }

  _final(callback) {
    try {
      this.supplementalString = JSON.parse(this.supplementalString);
    } catch (e) {
      /* noop */
    }

    this.output.emit('supplementalString', this.supplementalString);
    this.output.supplemental = this.supplementalString;
    this.output.push(null);
    callback();
  }
}

class CounterStream extends Transform {
  constructor(stream) {
    super();
    this.size = 0;
    this.output = stream;
  }

  _transform(chunk, encoding, callback) {
    this.size += chunk.length;
    this.push(chunk, encoding);
    callback();
  }

  _flush(callback) {
    this.emit('size', this.size);
    this.output.transferred = this.size;
    callback();
  }
}

/**
 * Duplex stream to work with database
 * @class RecordStream
 * @extends {Duplex}
 * @param {object} [options] options
 * @param {object} [options.format] how to format filelds/rows internally
 */
function RecordStream(options = {}) {
  options.objectMode = true;
  Duplex.call(this, options);

  this.format = options.format;

  Object.defineProperty(this, 'req', {
    get() { return this._req; },
    set(req) { this._req = req; this._canWrite = true; },
  });
}

util.inherits(RecordStream, Duplex);

RecordStream.prototype._read = function read() {
  // nothing to do there, when data comes, push will be called
};

// http://ey3ball.github.io/posts/2014/07/17/node-streams-back-pressure/
// https://nodejs.org/en/docs/guides/backpressuring-in-streams/
// https://nodejs.org/docs/latest/api/stream.html#stream_implementing_a_writable_stream

// TODO: implement _writev

RecordStream.prototype._write = function _write(_chunk, enc, cb) {
  let chunk = _chunk;

  if (!Buffer.isBuffer(chunk) && typeof chunk !== 'string') {
    chunk = encodeRow(chunk, this.format);
  }

  // there is no way to determine line ending efficiently for Buffer
  if (typeof chunk === 'string') {
    if (chunk.charAt(chunk.length - 1) !== '\n') {
      chunk = `${chunk}\n`;
    }

    chunk = Buffer.from(chunk, enc);
  }

  if (!(chunk instanceof Buffer)) {
    this.emit('error', new Error('Incompatible format'));
    return;
  }

  this.req.push(chunk);
  cb(null);
};

RecordStream.prototype._destroy = function _destroy(err, cb) {
  process.nextTick(() => {
    RecordStream.super_.prototype._destroy.call(this, err, (destroyErr) => {
      this.req.destroy();
      cb(destroyErr || err);
    });
  });
};

RecordStream.prototype.end = function end(chunk, enc, cb) {
  RecordStream.super_.prototype.end.call(this, chunk, enc, () => {
    this.req.push(null);
    if (cb) cb();
  });
};

module.exports = {
  JSONStream,
  RecordStream,
  CounterStream,
  StreamingParser,
};
