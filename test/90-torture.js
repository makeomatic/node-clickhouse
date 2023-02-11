/* eslint-disable no-console */
// const memwatch = require('@airbnb/node-memwatch');
const { once } = require('events');
const Limit = require('p-limit');
// const util = require('util');
const ClickHouse = require('../src/clickhouse');

// replace with it, if you want to run this test suite
const test = process.env.TORTURE ? it : it.skip;
const timeout = 120000;
const limit = Limit(100);

function checkUsageAndGC(used, baseline) {
  function inMB(v) {
    return `${(v / (2 ** 20)).toFixed(1)}MB`;
  }
  const usage = 'rss heapTotal heapUsed external'.split(' ').map((k) => {
    const delta = used[k] - baseline[k];
    return `${k} ${inMB(used[k])} / +${inMB(delta)}`;
  });

  console.log(usage.join(' '));
  if (global.gc) global.gc();
}

describe('torturing', function testSuite() {
  this.timeout(timeout);

  const host = process.env.CLICKHOUSE_HOST || '127.0.0.1';
  const port = process.env.CLICKHOUSE_PORT || 8123;
  const user = process.env.CLICKHOUSE_USER || 'new_user';
  const password = process.env.CLIKHOUSE_PASSWORD || 'new_password';

  const clickhouseOptions = {
    host,
    port,
    user,
    password,
  };

  let ch;
  // let hd;
  let baselineMemoryUsage;

  before(() => {
    if (global.gc) global.gc();
    // hd = new memwatch.HeapDiff();
    baselineMemoryUsage = process.memoryUsage();
    ch = new ClickHouse({ ...clickhouseOptions, readonly: true });
  });

  test('selects 1 million using async parser', async () => {
    let symbolsTransferred = 0;

    const input = Array.from({ length: 10 }).map(() => {
      return limit(async () => {
        const stream = ch.query('SELECT number FROM system.numbers LIMIT 1000000', { format: 'JSONEachRow' });
        stream.on('data', () => {});
        await once(stream, 'end');
        symbolsTransferred += stream.transferred;
      });
    });

    await Promise.all(input);
    console.log('symbols transferred:', symbolsTransferred);
  });

  // enable this test separately
  test('selects 1 million using sync parser', async () => {
    let symbolsTransferred = 0;

    const input = Array.from({ length: 10 }).map(() => {
      return limit(async () => {
        const stream = ch.query('SELECT number FROM system.numbers LIMIT 1000000', { syncParser: true });
        stream.on('data', () => {});
        await once(stream, 'end');
        symbolsTransferred += stream.transferred;
      });
    });

    await Promise.all(input);
    console.log('symbols transferred:', symbolsTransferred);
  });

  test('selects system.columns using async parser #1', async () => {
    let symbolsTransferred = 0;

    const input = Array.from({ length: 10000 }).map(() => {
      return limit(async () => {
        const stream = ch.query('SELECT * FROM system.columns', { format: 'JSONCompactEachRowWithNamesAndTypes' });
        stream.on('data', () => {});
        await Promise.all([
          once(stream, 'metadata'),
          once(stream, 'end'),
        ]);
        symbolsTransferred += stream.transferred;
      });
    });

    await Promise.all(input);
    console.log('symbols transferred:', symbolsTransferred);
  });

  test('selects system.columns using sync parser #1', async () => {
    let symbolsTransferred = 0;
    const input = Array.from({ length: 10000 }).map(() => {
      return limit(async () => {
        const stream = ch.query('SELECT * FROM system.columns', { syncParser: true });
        stream.on('data', () => {});
        await once(stream, 'end');
        symbolsTransferred += stream.transferred;
      });
    });

    await Promise.all(input);
    console.log('symbols transferred:', symbolsTransferred);
  });

  test('selects system.columns using async parser #2', async () => {
    let symbolsTransferred = 0;
    const input = Array.from({ length: 5000 }).map(() => {
      return limit(async () => {
        const stream = ch.query('SELECT * FROM system.columns', { format: 'JSONEachRowWithProgress' });
        stream.on('data', () => {});
        await once(stream, 'end');
        symbolsTransferred += stream.transferred;
      });
    });

    await Promise.all(input);
    console.log('symbols transferred:', symbolsTransferred);
  });

  test('selects system.columns using sync parser #2', async () => {
    let symbolsTransferred = 0;
    const input = Array.from({ length: 10000 }).map(() => {
      return limit(async () => {
        const stream = ch.query('SELECT * FROM system.columns', { syncParser: true });
        stream.on('data', () => {});
        await once(stream, 'end');
        symbolsTransferred += stream.transferred;
      });
    });

    await Promise.all(input);
    console.log('symbols transferred:', symbolsTransferred);
  });

  afterEach(async () => {
    checkUsageAndGC(process.memoryUsage(), baselineMemoryUsage);
    // console.log(util.inspect(hd.end(), false, 1, true));
    // hd = new memwatch.HeapDiff();
  });

  after(async () => {
    await ch.close();
  });
});
