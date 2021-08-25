/* eslint-disable no-console */
const memwatch = require('@airbnb/node-memwatch');
const { once } = require('events');
const Limit = require('p-limit');
const ClickHouse = require('../src/clickhouse');

// replace with it, if you want to run this test suite
const test = process.env.TORTURE ? it : it.skip;
const timeout = 120000;
const limit = Limit(50);

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
  if (global.gc) global.gc();

  const host = process.env.CLICKHOUSE_HOST || '127.0.0.1';
  const port = process.env.CLICKHOUSE_PORT || 8123;
  const baselineMemoryUsage = process.memoryUsage();

  before(() => {
    memwatch.on('leak', (info) => {
      console.log('leak', info);
    });
  });

  test('selects 1 million using async parser', async () => {
    const ch = new ClickHouse({ host, port, readonly: true });
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
  it('selects 1 million using sync parser', async () => {
    const ch = new ClickHouse({ host, port, readonly: true });
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

  test.only('selects system.columns using async parser #1', async () => {
    const ch = new ClickHouse({ host, port, readonly: true });
    let symbolsTransferred = 0;

    const input = Array.from({ length: 10000 }).map(() => {
      return limit(async () => {
        const stream = ch.query('SELECT * FROM system.columns', { format: 'JSONCompactEachRowWithNamesAndTypes' });
        stream.on('data', () => {});
        stream.on('metadata', (meta) => console.log(meta));
        await once(stream, 'end');
        symbolsTransferred += stream.transferred;
      });
    });

    await Promise.all(input);
    console.log('symbols transferred:', symbolsTransferred);
  });

  test('selects system.columns using sync parser #1', async () => {
    const ch = new ClickHouse({ host, port, readonly: true });

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
    const ch = new ClickHouse({ host, port, readonly: true });

    let symbolsTransferred = 0;
    const input = Array.from({ length: 10000 }).map(() => {
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
    const ch = new ClickHouse({ host, port, readonly: true });

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

  afterEach(() => {
    checkUsageAndGC(process.memoryUsage(), baselineMemoryUsage);
    // console.log ('after', lastSuite,  process.memoryUsage());
  });
});
